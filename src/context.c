
#include "functions.h"

CMUTIL_LogDefine("cmdbm.context")

typedef struct CMDBM_ContextDBLib {
	int initcnt;
	void (*libclear)();
} CMDBM_ContextDBLib;

typedef struct CMDBM_Context_Internal {
	CMDBM_ContextEx     base;
    CMUTIL_Map          *databases;
    CMUTIL_Bool         istimerinternal;
    CMUTIL_Timer        *timer;
    char                *progcs;        // program character set
	CMUTIL_Map			*poolconfs;
	CMUTIL_Bool			logqueryid;
	CMUTIL_Bool			logquery;
	CMUTIL_Bool			logresult;
	CMUTIL_Map			*libctx;
} CMDBM_Context_Internal;

CMDBM_STATIC void CMDBM_ContextDatabaseDestroyer(void *data)
{
    CMDBM_Database *db = (CMDBM_Database*)data;
	if (db)
        CMUTIL_CALL(db, Destroy);
}

CMDBM_STATIC void CMDBM_ContextPoolConfDestroyer(void *data)
{
	CMDBM_PoolConfig *pconf = (CMDBM_PoolConfig*)data;
	if (pconf) {
		if (pconf->testsql) CMFree(pconf->testsql);
		CMFree(pconf);
	}
}

CMDBM_STATIC void CMDBM_ContextDBLibDestroyer(void *data)
{
	CMDBM_ContextDBLib *plib = (CMDBM_ContextDBLib*)data;
	if (plib) {
		plib->libclear();
		CMFree(plib);
	}
}

CMDBM_STATIC void CMDBM_ContextConfigClean(CMUTIL_Json *json)
{
	int i;
	switch (CMUTIL_CALL(json, GetType)) {
	case CMUTIL_JsonTypeObject: {
		CMUTIL_JsonObject *jobj = (CMUTIL_JsonObject*)json;
		CMUTIL_StringArray *keys = CMUTIL_CALL(jobj, GetKeys);
		for (i=0; i<CMUTIL_CALL(keys, GetSize); i++) {
			CMUTIL_String *str = CMUTIL_CALL(keys, GetAt, i);
			const char *key = CMUTIL_CALL(str, GetCString);
			CMUTIL_Json *item = CMUTIL_CALL(jobj, Remove, key);
			// change internal buffer to lowercase
			CMUTIL_CALL(str, SelfToLower);
			// change item recursively.
			CMDBM_ContextConfigClean(item);
			// 'key' is changed to lowercase already by calling SelfToLower.
			// so just put with it.
			CMUTIL_CALL(jobj, Put, key, item);
		}
		CMUTIL_CALL(keys, Destroy);
		break;
	}
	case CMUTIL_JsonTypeArray: {
		CMUTIL_JsonArray *jarr = (CMUTIL_JsonArray*)json;
		for (i=0; i<CMUTIL_CALL(jarr, GetSize); i++)
			// change item recursively.
			CMDBM_ContextConfigClean(CMUTIL_CALL(jarr, Get, i));
		break;
	}
	default: return;
	}
}

CMDBM_STATIC CMUTIL_Bool CMDBM_ContextParsePoolConfig(
		CMDBM_Context *context,
		CMUTIL_JsonObject *pcfg)
{
	CMUTIL_Bool res = CMUTIL_False;
	CMDBM_Context_Internal *ictx = (CMDBM_Context_Internal*)context;
	CMUTIL_JsonValue *id =
			(CMUTIL_JsonValue*)CMUTIL_CALL(pcfg, Get, "id");
	CMUTIL_JsonValue *testsql =
			(CMUTIL_JsonValue*)CMUTIL_CALL(pcfg, Get, "testsql");
	if (id) {
		const char *sid = CMUTIL_CALL(id, GetCString);
		CMDBM_PoolConfig *poolconf = CMAlloc(sizeof(CMDBM_PoolConfig));

		memset(poolconf, 0x0, sizeof(CMDBM_PoolConfig));
		if (CMUTIL_CALL(pcfg, Get, "initcount"))
			poolconf->initcnt = (int)CMUTIL_CALL(pcfg, GetLong, "initcount");
		else
			poolconf->initcnt = 5;
		if (CMUTIL_CALL(pcfg, Get, "maxcount"))
			poolconf->maxcnt = (int)CMUTIL_CALL(pcfg, GetLong, "maxcount");
		else
			poolconf->maxcnt = 20;
		if (CMUTIL_CALL(pcfg, Get, "pinginterval"))
			poolconf->pingterm =
					(int)CMUTIL_CALL(pcfg, GetLong, "pinginterval");
		else
			poolconf->pingterm = 30;
		if (testsql)
			poolconf->testsql = CMStrdup(CMUTIL_CALL(testsql, GetCString));
		else
			poolconf->testsql = CMStrdup("select 1");

		CMUTIL_CALL(ictx->poolconfs, Put, sid, poolconf);
		res = CMUTIL_True;
	} else {
		CMLogError("PoolConfig does not have 'id' attribute.");
	}
	return res;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_ContextParseMappers(
		CMDBM_Database *db, CMUTIL_JsonArray *mappers)
{
	int i;
	CMUTIL_Bool res = CMUTIL_True;
	for (i=0; res && i<CMUTIL_CALL(mappers, GetSize); i++) {
		CMUTIL_JsonObject *mcfg =
				(CMUTIL_JsonObject*)CMUTIL_CALL(mappers, Get, i);
		const char *stype = CMUTIL_CALL(mcfg, GetCString, "type");
		if (strcasecmp(stype, "mapperSet") == 0) {
			// add mapper set
			const char *bpath = CMUTIL_CALL(mcfg, GetCString, "basepath");
			const char *fpattern = CMUTIL_CALL(mcfg, GetCString, "filepattern");
			CMUTIL_Bool recur = CMUTIL_CALL(mcfg, GetBoolean, "recursive");
			res = CMUTIL_CALL(db, AddMapperSet, bpath, fpattern, recur);
		} else if (strcasecmp(stype, "mapper") == 0) {
			// add mapper
			const char *fpath = CMUTIL_CALL(mcfg, GetCString, "filepath");
			res = CMUTIL_CALL(db, AddMapper, fpath);
		} else {
			CMLogErrorS("unknown mapper type: %s", stype);
			res = CMUTIL_False;
		}
	}
	return res;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_ContextParseDatabase(
		CMDBM_Context *context,
		const char *sdbtype,
		CMUTIL_JsonObject *dcfg)
{
	CMUTIL_Bool res = CMUTIL_False;
	CMDBM_Context_Internal *ictx = (CMDBM_Context_Internal*)context;
	CMUTIL_JsonObject *pcfg =
			(CMUTIL_JsonObject*)CMUTIL_CALL(dcfg, Get, "pool");
	CMUTIL_JsonValue *pref =
			(CMUTIL_JsonValue*)CMUTIL_CALL(pcfg, Get, "confref");
	CMUTIL_JsonValue *testsql =
			(CMUTIL_JsonValue*)CMUTIL_CALL(pcfg, Get, "testsql");
	CMUTIL_JsonValue *id =
			(CMUTIL_JsonValue*)CMUTIL_CALL(dcfg, Get, "id");
	CMUTIL_JsonValue *charset =
			(CMUTIL_JsonValue*)CMUTIL_CALL(dcfg, Get, "charset");
	const char *sid = NULL, *scharset = NULL;
	CMDBM_Database *db = NULL;
	CMDBM_PoolConfig *pconf = NULL;
	CMDBM_DBType dbtype;
	CMUTIL_JsonObject *param = NULL;
	CMUTIL_StringArray *keys = NULL;
	int i;

	if (id == NULL) {
		CMLogErrorS("There is no 'id' attribute in '%s' type datasource.");
		goto ENDPOINT;
	}
	sid = CMUTIL_CALL(id, GetCString);

	if (charset == NULL)
		scharset = "utf-8";
	else
		scharset = CMUTIL_CALL(charset, GetCString);

	pconf = CMAlloc(sizeof(CMDBM_PoolConfig));

	if (pref) {
		const char *refkey = CMUTIL_CALL(pref, GetCString);
		CMDBM_PoolConfig *ref = (CMDBM_PoolConfig*)CMUTIL_CALL(
					ictx->poolconfs, Get, refkey);
		memcpy(pconf, ref, sizeof(CMDBM_PoolConfig));
		if (testsql)
			pconf->testsql = CMStrdup(CMUTIL_CALL(testsql, GetCString));
		else
			pconf->testsql = CMStrdup(pconf->testsql);
	} else {
		memset(pconf, 0x0, sizeof(CMDBM_PoolConfig));
		pconf->initcnt = 5;
		pconf->maxcnt = 20;
		pconf->pingterm = 30;
		if (testsql)
			pconf->testsql = CMStrdup(CMUTIL_CALL(testsql, GetCString));
		else
			pconf->testsql = CMStrdup("select 1");
	}

	if (CMUTIL_CALL(pcfg, Get, "initcount"))
		pconf->initcnt = (int)CMUTIL_CALL(pcfg, GetLong, "initcount");

	if (CMUTIL_CALL(pcfg, Get, "maxcount"))
		pconf->maxcnt = (int)CMUTIL_CALL(pcfg, GetLong, "maxcount");

	if (CMUTIL_CALL(pcfg, Get, "pinginterval"))
		pconf->pingterm =(int)CMUTIL_CALL(pcfg, GetLong, "pinginterval");

	if (CMUTIL_CALL(dcfg, Get, "params")) {
		CMUTIL_Json *json = CMUTIL_CALL(dcfg, Get, "params");
		param = (CMUTIL_JsonObject*)CMUTIL_CALL(json, Clone);
	} else {
		param = CMUTIL_JsonObjectCreate();
	}
	keys = CMUTIL_CALL(dcfg, GetKeys);
	for (i=0; i<CMUTIL_CALL(keys, GetSize); i++) {
		const char *key = CMUTIL_CALL(keys, GetCString, i);
		CMUTIL_Json *item = CMUTIL_CALL(dcfg, Get, key);
		CMUTIL_JsonType type = CMUTIL_CALL(item, GetType);
		if (type == CMUTIL_JsonTypeValue) {
			CMUTIL_Json *nitem = CMUTIL_CALL(item, Clone);
			CMUTIL_CALL(param, Put, key, nitem);
		}
	}
	CMUTIL_CALL(keys, Destroy);

	dbtype = CMDBM_DatabaseType(sdbtype);
	db = CMDBM_DatabaseCreate(sid, dbtype, scharset, pconf, param);
	if (!db) {
		CMLogErrorS("cannot create database(%s)", sid);
		goto ENDPOINT;
	}

	// parse mappers
	if (CMUTIL_CALL(dcfg, Get, "mappers")) {
		CMUTIL_Json *mappers = CMUTIL_CALL(dcfg, Get, "mappers");
		if (CMUTIL_CALL(mappers, GetType) == CMUTIL_JsonTypeArray) {
			if (!CMDBM_ContextParseMappers(db, (CMUTIL_JsonArray*)mappers)) {
				goto ENDPOINT;
			}
		} else {
			CMLogError("invalid mapper configuration. for database %s", sid);
			goto ENDPOINT;
		}
	} else {
		CMLogWarn("database(%s) has no mappers. "
				  "any request on this database will be failed.", sid);
	}

	if (!CMUTIL_CALL(context, AddDatabase, db)) {
		CMLogErrorS("database(%s) cannot be added to context.", sid);
		goto ENDPOINT;
	}

	res = CMUTIL_True;
ENDPOINT:
	if (pconf) CMDBM_ContextPoolConfDestroyer(pconf);
	if (param)
		CMUTIL_JsonDestroy(param);
	if (!res && db)
		CMUTIL_CALL(db, Destroy);
	return res;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_ContextParseConfig(
		CMDBM_Context *context,
		CMUTIL_Json *config)
{
	int i;
	CMUTIL_Bool res = CMUTIL_False;
	CMUTIL_Json *item = NULL;
	CMUTIL_JsonArray *jarr = NULL;
	CMUTIL_JsonObject *jconf = NULL;
	CMUTIL_String *type = NULL, *ltype = NULL;

	if (CMUTIL_CALL(config, GetType) != CMUTIL_JsonTypeObject) {
		CMLogErrorS("invalid configuration structure.");
		goto ENDPOINT;
	}

	jconf = (CMUTIL_JsonObject*)config;

	CMDBM_ContextConfigClean(config);

	// load pool config
	item = CMUTIL_CALL(jconf, Get, "poolconfigurations");
	if (item) {
		if (CMUTIL_CALL(item, GetType) != CMUTIL_JsonTypeArray) {
			CMLogErrorS("invalid configuration structure.");
			goto ENDPOINT;
		}
		jarr = (CMUTIL_JsonArray*)item;
		for (i=0; i<CMUTIL_CALL(jarr, GetSize); i++) {
			CMUTIL_JsonObject *pcfg = NULL;

			item = CMUTIL_CALL(jarr, Get, i);
			if (CMUTIL_CALL(item, GetType) != CMUTIL_JsonTypeObject) {
				CMLogErrorS("invalid configuration structure.");
				goto ENDPOINT;
			}

			pcfg = (CMUTIL_JsonObject*)item;;
			if (!CMDBM_ContextParsePoolConfig(context, pcfg)) {
				CMLogErrorS("pool configuration parse failed.");
				goto ENDPOINT;
			}
		}
	}

	// load database config
	item = CMUTIL_CALL((CMUTIL_JsonObject*)config, Get, "databases");
	if (!item) {
		CMLogErrorS("database configuration not found.");
		goto ENDPOINT;
	}
	if (CMUTIL_CALL(item, GetType) != CMUTIL_JsonTypeArray) {
		CMLogErrorS("invalid configuration structure.");
		goto ENDPOINT;
	}
	jarr = (CMUTIL_JsonArray*)item;
	for (i=0; i<CMUTIL_CALL(jarr, GetSize); i++) {
		CMUTIL_JsonObject *dcfg = NULL;

		item = CMUTIL_CALL(jarr, Get, i);
		if (CMUTIL_CALL(item, GetType) != CMUTIL_JsonTypeObject) {
			CMLogErrorS("invalid configuration structure.");
			goto ENDPOINT;
		}

		dcfg = (CMUTIL_JsonObject*)item;;
		type = CMUTIL_CALL(dcfg, GetString, "type");
		if (type == NULL) {
			CMLogErrorS("database item does not have 'type' property.");
			goto ENDPOINT;
		}
		ltype = CMUTIL_CALL(type, ToLower);
		if (!CMDBM_ContextParseDatabase(
					context, CMUTIL_CALL(ltype, GetCString), dcfg)) {
			CMLogError("database configuration parse failed.");
			goto ENDPOINT;
		}
	}

	// TODO: load logging config
	res = CMUTIL_True;
ENDPOINT:
	if (ltype) CMUTIL_CALL(ltype, Destroy);

	return res;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_ContextInitialize(
		CMDBM_Context_Internal *ictx,
		const char *confjson,
        const char *progcharset,
        CMUTIL_Timer *timer)
{
	CMUTIL_Bool res = CMUTIL_False;
	CMUTIL_Json *conf = NULL;

	if (confjson) {
		CMUTIL_File *cfile = CMUTIL_FileCreate(confjson);
		CMUTIL_String *content = CMUTIL_CALL(cfile, GetContents);
		if (content) conf = CMUTIL_JsonParse(content);
		if (cfile) CMUTIL_CALL(cfile, Destroy);
		if (content) CMUTIL_CALL(content, Destroy);
	}

	if (!progcharset) progcharset = "UTF-8";
    ictx->progcs = CMStrdup(progcharset);
    if (timer) {
        ictx->timer = timer;
    } else {
		ictx->timer = CMUTIL_TimerCreateEx(1000, 2);
        ictx->istimerinternal = CMUTIL_True;
    }

	if (conf) {
		// parse config xml
		if (!CMDBM_ContextParseConfig((CMDBM_Context*)ictx, conf)) {
			CMLogError("configuration loading failed.");
			goto ENDPOINT;
		}
	}

	res = CMUTIL_True;
ENDPOINT:
	if (conf) CMUTIL_JsonDestroy(conf);
	return res;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_ContextAddDatabase(
		CMDBM_Context *ctx, CMDBM_Database *db)
{
	CMUTIL_Bool res = CMUTIL_False;
	CMDBM_Context_Internal *ictx = (CMDBM_Context_Internal*)ctx;
	CMDBM_DatabaseEx *edb = (CMDBM_DatabaseEx*)db;
	const char *dbid = NULL;

	if (!edb) {
		CMLogError("invalid parameter.");
		goto ENDPOINT;
	}
	dbid = CMUTIL_CALL(edb, GetId);

	if (!CMUTIL_CALL(edb, Initialize, ictx->timer, ictx->progcs)) {
		CMLogError("database(%s) initialization failed.", dbid);
		goto ENDPOINT;
	}
	CMUTIL_CALL(ictx->databases, Put, dbid, db);
	res = CMUTIL_True;
ENDPOINT:
	return res;
}

CMDBM_STATIC CMDBM_Session *CMDBM_ContextGetSession(CMDBM_Context *ctx)
{
	CMDBM_Session *res = NULL;
	CMDBM_ContextEx *ectx = (CMDBM_ContextEx*)ctx;
	res = CMDBM_SessionCreate(ectx);
	if (res == NULL)
		CMLogError("cannot create session.");
	return res;
}

CMDBM_STATIC void CMDBM_ContextDestroy(CMDBM_Context *ctx)
{
	CMDBM_Context_Internal *ictx = (CMDBM_Context_Internal*)ctx;
	if (ictx) {
		if (ictx->istimerinternal) CMUTIL_CALL(ictx->timer, Destroy);
		if (ictx->databases) CMUTIL_CALL(ictx->databases, Destroy);
		if (ictx->poolconfs) CMUTIL_CALL(ictx->poolconfs, Destroy);
		if (ictx->libctx) CMUTIL_CALL(ictx->libctx, Destroy);
		if (ictx->progcs) CMFree(ictx->progcs);
		CMFree(ictx);
	}
}

CMDBM_STATIC CMDBM_DatabaseEx *CMDBM_ContextGetDatabase(
		CMDBM_ContextEx *ctx, const char *dbid)
{
	CMDBM_Context_Internal *ictx = (CMDBM_Context_Internal*)ctx;
	CMDBM_DatabaseEx *res = NULL;

	res = (CMDBM_DatabaseEx*)CMUTIL_CALL(ictx->databases, Get, dbid);
	if (res == NULL)
		CMLogError("source id(%s) not found in this CMDBM context.", dbid);
	return res;
}

static CMDBM_ContextEx g_cmdbm_context = {
	{
		CMDBM_ContextAddDatabase,
		CMDBM_ContextGetSession,
		CMDBM_ContextDestroy
	},
	CMDBM_ContextGetDatabase
};

CMDBM_Context *CMDBM_ContextCreate(
		const char			*confjson,		/* optional */
		const char			*progcharset,	/* optional: utf-8 default. */
		CMUTIL_Timer        *timer)			/* optional */
{
	CMDBM_Context_Internal *res = CMAlloc(sizeof(CMDBM_Context_Internal));

	memset(res, 0x0, sizeof(CMDBM_Context_Internal));
	memcpy(res, &g_cmdbm_context, sizeof(CMDBM_ContextEx));

	res->databases = CMUTIL_MapCreateEx(32, CMDBM_ContextDatabaseDestroyer);
	res->poolconfs = CMUTIL_MapCreateEx(16, CMDBM_ContextPoolConfDestroyer);

	res->libctx = CMUTIL_MapCreateEx(32, CMDBM_ContextDBLibDestroyer);

	if (!CMDBM_ContextInitialize(res, confjson, progcharset, timer)) {
		CMLogError("context initializing failed.");
		CMUTIL_CALL((CMDBM_Context*)res, Destroy);
		res = NULL;
	}

	return (CMDBM_Context*)res;
}

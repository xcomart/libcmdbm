
#include "functions.h"

CMUTIL_LogDefine("cmdbm.database")

typedef struct CMDBM_MapperFile {
	CMUTIL_XmlNode			*node;
	time_t					lastupdt;
	CMUTIL_Bool				isinset;
	char					*fpath;
	CMUTIL_StringArray		*mapperids;
	CMUTIL_Map				*queries;
} CMDBM_MapperFile;

typedef struct CMDBM_MapperFileSet {
	CMUTIL_Array			*mfileset;
	char					*dpath;
	char					*fpattern;
	CMUTIL_Map				*queries;
	CMUTIL_Bool				recursive;
} CMDBM_MapperFileSet;

typedef struct CMDBM_Database_Internal {
	CMDBM_DatabaseEx		base;
	char					*sourceid;
	char					*dbcs;
	char					*pgcs;
	void					*initres;
	CMUTIL_Map				*queries;
	CMDBM_ModuleInterface	*modif;
	CMUTIL_RWLock			*rwlock;
	CMUTIL_Map				*mfiles;
	CMUTIL_Map				*mfsets;
	CMDBM_PoolConfig		*poolconf;
	CMUTIL_TimerTask		*monitor;
	CMUTIL_Pool				*connpool;
	int						minterval;
	CMUTIL_JsonObject		*params;
	CMUTIL_String			*testqry;
} CMDBM_Database_Internal;

CMDBM_STATIC CMDBM_PoolConfig *CMDBM_PoolConfigClone(CMDBM_PoolConfig *pconf)
{
	CMDBM_PoolConfig *res = CMAlloc(sizeof(CMDBM_PoolConfig));
	memcpy(res, pconf, sizeof(CMDBM_PoolConfig));
	res->testsql = CMStrdup(pconf->testsql);
	return res;
}

CMDBM_STATIC void CMDBM_PoolConfigDestroy(CMDBM_PoolConfig *pconf)
{
	if (pconf) {
		if (pconf->testsql) CMFree(pconf->testsql);
		CMFree(pconf);
	}
}

CMDBM_STATIC void CMDBM_MapperFileDestroy(void *data)
{
	CMDBM_MapperFile *mfile = (CMDBM_MapperFile*)data;
	if (mfile) {
		if (mfile->mapperids)
			CMUTIL_CALL(mfile->mapperids, Destroy);
		if (mfile->node)
			CMUTIL_CALL(mfile->node, Destroy);
		if (mfile->queries)
			CMUTIL_CALL(mfile->queries, Destroy);
		if (mfile->fpath)
			CMFree(mfile->fpath);
		CMFree(mfile);
	}
}

CMDBM_STATIC CMDBM_MapperFile *CMDBM_MapperFileCreate(
		const char *fpath, CMUTIL_XmlNode *node,
		CMUTIL_Bool isinset, CMUTIL_Map *queries)
{
	CMDBM_MapperFile *res = NULL;
	CMUTIL_File *file = CMUTIL_FileCreate(fpath);
	if (CMUTIL_CALL(file, IsExists)) {
		res = CMAlloc(sizeof(CMDBM_MapperFile));
		memset(res, 0x0, sizeof(CMDBM_MapperFile));
		res->node = node;
		res->isinset = isinset;
		res->lastupdt = CMUTIL_CALL(file, ModifiedTime);
		res->mapperids = CMUTIL_CALL(queries, GetKeys);
		res->queries = queries;
		res->fpath = CMStrdup(fpath);
	}
	CMUTIL_CALL(file, Destroy);
	return res;
}

CMDBM_STATIC void CMDBM_DatabaseRemoveFile(
		CMDBM_Database_Internal *idb, const char *fpath)
{
	int i;
	CMDBM_MapperFile *mfile = NULL;
	mfile = (CMDBM_MapperFile*)CMUTIL_CALL(idb->mfiles, Remove, fpath);
	if (mfile) {
		for (i=0; i<CMUTIL_CALL(mfile->mapperids, GetSize); i++) {
			CMUTIL_String *sid = CMUTIL_CALL(mfile->mapperids, GetAt, i);
			const char *id = CMUTIL_CALL(sid, GetCString);
			CMUTIL_CALL(idb->queries, Remove, id);
		}
		CMDBM_MapperFileDestroy(mfile);
	}
}

CMDBM_STATIC CMUTIL_Bool CMDBM_DatabaseAddMapper(
		CMDBM_Database *db, const char *mapperfile)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	CMUTIL_Bool res = CMUTIL_False;
	CMUTIL_XmlNode *mapper = CMUTIL_XmlParseFile(mapperfile);
	if (mapper) {
		CMUTIL_Map *queries = CMUTIL_MapCreate();
		if (CMDBM_MapperRebuildItem(queries, mapper)) {
			CMDBM_MapperFile *mfile = CMDBM_MapperFileCreate(
						mapperfile, mapper, CMUTIL_False, queries);
			if (mfile != NULL) {
				CMUTIL_CALL(idb->rwlock, WriteLock);
				CMDBM_DatabaseRemoveFile(idb, mapperfile);
				CMUTIL_CALL(idb->mfiles, Put, mapperfile, mfile);
				CMUTIL_CALL(idb->queries, PutAll, queries);
				CMUTIL_CALL(idb->rwlock, WriteUnlock);
				queries = NULL;
				res = CMUTIL_True;
			} else {
				CMLogError("mapper file not exists(%s).", mapperfile);
			}
		} else {
			CMLogError("invalid mapper structure(%s).", mapperfile);
		}
		if (queries)
			CMUTIL_CALL(queries, Destroy);
	} else {
		CMLogError("XML file(%s) parse failed. skipping.", mapperfile);
	}
	if (!res && mapper) CMUTIL_CALL(mapper, Destroy);
	return res;
}

CMDBM_STATIC int CMDBM_DatabaseMapperComp(const void *a, const void *b)
{
	CMDBM_MapperFile *fa = (CMDBM_MapperFile*)a;
	CMDBM_MapperFile *fb = (CMDBM_MapperFile*)b;
	return strcmp(fa->fpath, fb->fpath);
}

CMDBM_STATIC void CMDBM_MapperFileSetDestroy(void *data)
{
	CMDBM_MapperFileSet *mfset = (CMDBM_MapperFileSet*)data;
	if (mfset) {
		if (mfset->dpath) CMFree(mfset->dpath);
		if (mfset->fpattern) CMFree(mfset->fpattern);
		if (mfset->mfileset) CMUTIL_CALL(mfset->mfileset, Destroy);
		if (mfset->queries) CMUTIL_CALL(mfset->queries, Destroy);
		CMFree(mfset);
	}
}

CMDBM_STATIC CMDBM_MapperFileSet *CMDBM_DatabaseBuidlMapperSet(
		const char *dpath, const char  *fpattern, CMUTIL_Bool recursive)
{
	int i;
	CMUTIL_File *dfile = CMUTIL_FileCreate(dpath);
	CMUTIL_FileList *flist = CMUTIL_CALL(dfile, Find, fpattern, recursive);
	CMDBM_MapperFileSet *mset = CMAlloc(sizeof(CMDBM_MapperFileSet));
	memset(mset, 0x0, sizeof(CMDBM_MapperFileSet));
	mset->dpath = CMStrdup(dpath);
	mset->fpattern = CMStrdup(fpattern);
	mset->recursive = recursive;
	mset->mfileset = CMUTIL_ArrayCreateEx(
				10, CMDBM_DatabaseMapperComp, CMDBM_MapperFileDestroy);
	mset->queries = CMUTIL_MapCreate();
	for (i=0; i<CMUTIL_CALL(flist, Count); i++) {
		CMUTIL_Bool succ = CMUTIL_False;
		CMUTIL_File *cur = CMUTIL_CALL(flist, GetAt, i);
		const char *fpath = CMUTIL_CALL(cur, GetFullPath);
		CMUTIL_XmlNode *mapper = CMUTIL_XmlParseFile(fpath);
		if (mapper) {
			CMUTIL_Map *fqrys = CMUTIL_MapCreate();
			if (CMDBM_MapperRebuildItem(fqrys, mapper)) {
				CMDBM_MapperFile *mfile = CMDBM_MapperFileCreate(
							fpath, mapper, CMUTIL_True, fqrys);
				CMUTIL_CALL(mset->mfileset, Add, mfile);
				CMUTIL_CALL(mset->queries, PutAll, fqrys);
				fqrys = NULL;
				succ = CMUTIL_True;
			} else {
				CMLogError("invalid mapper structure(%s). skipped.", fpath);
			}
			if (fqrys)
				CMUTIL_CALL(fqrys, Destroy);
		} else {
			CMLogError("invliad xml(%s). skipped",
					   CMUTIL_CALL(cur, GetFullPath));
		}
		if (!succ && mapper)
			CMUTIL_CALL(mapper, Destroy);
	}
	CMUTIL_CALL(flist, Destroy);
	CMUTIL_CALL(dfile, Destroy);
	return mset;
}

CMDBM_STATIC void CMDBM_DatabaseRemoveMapperSet(
		CMDBM_Database *db, const char *key)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	CMDBM_MapperFileSet *mset = NULL;

	mset = (CMDBM_MapperFileSet*)CMUTIL_CALL(idb->mfsets, Get, key);
	if (mset) {
		int i;
		CMUTIL_StringArray *qkeys = NULL;
		qkeys = CMUTIL_CALL(mset->queries, GetKeys);
		if (qkeys) {
			for (i=0; i<CMUTIL_CALL(qkeys, GetSize); i++) {
				const char *qkey = CMUTIL_CALL(qkeys, GetCString, i);
				CMUTIL_CALL(idb->queries, Remove, qkey);
			}
			CMUTIL_CALL(qkeys, Destroy);
		}
		CMUTIL_CALL(idb->mfsets, Remove, key);
		CMDBM_MapperFileDestroy(mset);
	} else {
		CMLogWarn("mapper set(%s) does not exists in this database",
				  key);
	}
}

CMDBM_STATIC CMUTIL_Bool CMDBM_DatabaseAddMapperSet(
        CMDBM_Database *db, const char *dpath, const char *fpattern,
        CMUTIL_Bool recursive)
{
    char key[1024];
    CMUTIL_Bool res = CMUTIL_True;
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	CMDBM_MapperFileSet *mset = CMDBM_DatabaseBuidlMapperSet(
				dpath, fpattern, recursive);
	sprintf(key, "%s;%s", dpath, fpattern);
	CMUTIL_CALL(idb->rwlock, WriteLock);
	CMUTIL_CALL(idb->mfsets, Put, key, mset);
	CMUTIL_CALL(idb->queries, PutAll, mset->queries);
	CMUTIL_CALL(idb->rwlock, WriteUnlock);
    return res;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_DatabaseSetMonitor(
		CMDBM_Database *db, int interval)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	idb->minterval = interval;
	return CMUTIL_True;
}

CMDBM_STATIC void *CMDBM_DatabasePoolCreateProc(void *data)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)data;
	void *rawconn = idb->modif->OpenConnection(idb->initres, idb->params);
	if (rawconn) {
		return CMDBM_ConnectionCreate((CMDBM_DatabaseEx*)idb, rawconn);
	} else {
		CMLogError("cannot open database connection(%s)", idb->sourceid);
		return NULL;
	}
}

CMDBM_STATIC void CMDBM_DatabasePoolDestroyProc(void *resource, void *data)
{
	CMDBM_Connection *conn = (CMDBM_Connection*)resource;
	if (conn)
		CMUTIL_CALL(conn, CloseReal);
	CMUTIL_UNUSED(data);
}

CMDBM_STATIC CMUTIL_Bool CMDBM_DatabasePoolTestProc(
		void *resource, void *data)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)data;
	CMDBM_Connection *conn = (CMDBM_Connection*)resource;
	CMUTIL_JsonValue *val = CMUTIL_CALL(
				conn, GetObject, idb->testqry, NULL, NULL);
	if (val) {
		CMUTIL_JsonDestroy(val);
		return CMUTIL_True;
	}
	return CMUTIL_False;
}

CMDBM_STATIC void CMDBM_DatabaseMapperReloader(void *data)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)data;
	CMUTIL_Array *toberep = CMUTIL_ArrayCreate();
	CMUTIL_Array *toberem = CMUTIL_ArrayCreate();
	CMUTIL_Iterator *iter = NULL;

	iter = CMUTIL_CALL(idb->mfiles, Iterator);
	while (CMUTIL_CALL(iter, HasNext)) {
		CMDBM_MapperFile *mf = (CMDBM_MapperFile*)CMUTIL_CALL(iter, Next);
		CMUTIL_File *f = CMUTIL_FileCreate(mf->fpath);
		if (CMUTIL_CALL(f, IsExists)) {
			if (CMUTIL_CALL(f, ModifiedTime) != mf->lastupdt)
				CMUTIL_CALL(toberep, Add, mf);
		} else {
			CMUTIL_CALL(toberem, Add, mf);
		}
		CMUTIL_CALL(f, Destroy);
	}
	CMUTIL_CALL(iter, Destroy);

	// replace existing mapper
	if (CMUTIL_CALL(toberep, GetSize) > 0) {
		CMUTIL_CALL(idb->rwlock, WriteLock);
		while (CMUTIL_CALL(toberep, GetSize) > 0) {
			CMDBM_MapperFile *mf =
					(CMDBM_MapperFile*)CMUTIL_CALL(toberep, RemoveAt, 0);
			CMLogInfo("mapper file changed: %s", mf->fpath);
			// mf->fpath will be freed while processing. so need to backup.
			char *fpath = CMStrdup(mf->fpath);
			CMDBM_DatabaseAddMapper((CMDBM_Database*)idb, fpath);
			CMFree(fpath);
		}
		CMUTIL_CALL(idb->rwlock, WriteUnlock);
	}

	// remove existing mapper
	if (CMUTIL_CALL(toberem, GetSize) > 0) {
		CMUTIL_CALL(idb->rwlock, WriteLock);
		while (CMUTIL_CALL(toberem, GetSize) > 0) {
			CMDBM_MapperFile *mf =
					(CMDBM_MapperFile*)CMUTIL_CALL(toberem, RemoveAt, 0);
			CMLogInfo("mapper file removed: %s", mf->fpath);
			CMDBM_DatabaseRemoveFile(idb, mf->fpath);
		}
		CMUTIL_CALL(idb->rwlock, WriteUnlock);
	}

	iter = CMUTIL_CALL(idb->mfsets, Iterator);
	while (CMUTIL_CALL(iter, HasNext)) {
		int i, j;
		CMDBM_MapperFileSet *mset =
				(CMDBM_MapperFileSet*)CMUTIL_CALL(iter, Next);
		CMUTIL_File *dir = CMUTIL_FileCreate(mset->dpath);
		CMUTIL_FileList *flist =
				CMUTIL_CALL(dir, Find, mset->fpattern, mset->recursive);
		CMUTIL_Bool ischanged = CMUTIL_False;

		// compare file lists
		for (i=0; !ischanged && i<CMUTIL_CALL(flist, Count); i++) {
			CMUTIL_File *f = CMUTIL_CALL(flist, GetAt, i);
			for (j=0; j<CMUTIL_CALL(mset->mfileset, GetSize); j++) {
				CMDBM_MapperFile *mf = (CMDBM_MapperFile*)
						CMUTIL_CALL(mset->mfileset, GetAt, j);
				CMLogTrace("comparing %s,%s",
						   CMUTIL_CALL(f, GetFullPath), mf->fpath);
				if (strcmp(CMUTIL_CALL(f, GetFullPath), mf->fpath) == 0) {
					if (CMUTIL_CALL(f, ModifiedTime) != mf->lastupdt)
						ischanged = CMUTIL_True;
					break;
				}
			}
		}

		if (ischanged)
			CMUTIL_CALL(toberep, Add, mset);
		CMUTIL_CALL(flist, Destroy);
		CMUTIL_CALL(dir, Destroy);
	}
	CMUTIL_CALL(iter, Destroy);

	// replace existing mapper file set
	if (CMUTIL_CALL(toberep, GetSize) > 0) {
		while (CMUTIL_CALL(toberep, GetSize) > 0) {
			CMDBM_MapperFileSet *mset = CMUTIL_CALL(toberep, RemoveAt, 0);
			CMDBM_MapperFileSet *newset = NULL;
			CMLogInfo("mapper set(%s/%s) changed. reloading...",
					  mset->dpath, mset->fpattern);
			newset = CMDBM_DatabaseBuidlMapperSet(
						mset->dpath, mset->fpattern, mset->recursive);
			if (newset) {
				char buf[1024];
				sprintf(buf, "%s;%s", mset->dpath, mset->fpattern);
				CMUTIL_CALL(idb->rwlock, WriteLock);
				CMDBM_DatabaseRemoveMapperSet((CMDBM_Database*)idb, buf);
				CMUTIL_CALL(idb->mfsets, Put, buf, mset);
				CMUTIL_CALL(idb->queries, PutAll, mset->queries);
				CMUTIL_CALL(idb->rwlock, WriteUnlock);
			}
		}
	}

	CMUTIL_CALL(toberep, Destroy);
	CMUTIL_CALL(toberem, Destroy);
}

CMDBM_STATIC CMUTIL_Bool CMDBM_DatabaseInitialize(
		CMDBM_DatabaseEx *db, CMUTIL_Timer *timer, const char *pgcs)
{
	long interval;
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	idb->pgcs = CMStrdup(pgcs);
	idb->initres = idb->modif->Initialize(idb->dbcs, idb->pgcs);
	idb->connpool = CMUTIL_PoolCreate(
				idb->poolconf->initcnt,
				idb->poolconf->maxcnt,
				CMDBM_DatabasePoolCreateProc,
				CMDBM_DatabasePoolDestroyProc,
				CMDBM_DatabasePoolTestProc,
				30, CMUTIL_True, idb, timer);

	// do initial mapper load
	CMDBM_DatabaseMapperReloader(idb);

	// schedule mapper reloader
	if (idb->minterval == 0)
		idb->minterval = 30;
	interval = idb->minterval * 1000;
	idb->monitor = CMUTIL_CALL(timer, ScheduleDelayRepeat, interval, interval,
							   CMDBM_DatabaseMapperReloader, idb);
	return idb->connpool == NULL? CMUTIL_False:CMUTIL_True;
}

CMDBM_STATIC CMUTIL_JsonObject *CMDBM_DatabaseGetParams(
		CMDBM_DatabaseEx *db)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	return idb->params;
}

CMDBM_STATIC CMDBM_ModuleInterface *CMDBM_DatabaseGetModuleIF(
		CMDBM_DatabaseEx *db)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	return idb->modif;
}

CMDBM_STATIC void *CMDBM_DatabaseGetInitResult(
		CMDBM_DatabaseEx *db)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	return idb->initres;
}

CMDBM_STATIC const char *CMDBM_DatabaseGetId(
		CMDBM_DatabaseEx *db)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	return idb->sourceid;
}

CMDBM_STATIC CMUTIL_XmlNode *CMDBM_DatabaseGetQuery(
		CMDBM_DatabaseEx *db, const char *id)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	CMUTIL_XmlNode *res = (CMUTIL_XmlNode*)CMUTIL_CALL(idb->queries, Get, id);
	if (res == NULL)
		CMLogErrorS("datasource '%s' has no query with id '%s'.",
					idb->sourceid, id);
	return res;
}

CMDBM_STATIC CMDBM_Connection *CMDBM_DatabaseGetConnection(
		CMDBM_DatabaseEx *db)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	CMDBM_Connection *res = CMUTIL_CALL(idb->connpool, CheckOut, 5000);
	if (res == NULL) {
		CMLogError("cannot get connection from source '%s' in 5 seconds",
				   idb->sourceid);
	}
	return res;
}

CMDBM_STATIC void CMDBM_DatabaseReleaseConnection(
		CMDBM_DatabaseEx *db, CMDBM_Connection *conn)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	CMUTIL_CALL(idb->connpool, Release, conn);
}

CMDBM_STATIC void CMDBM_DatabaseDestroy(
		CMDBM_Database *db)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	if (idb) {
		if (idb->sourceid) CMFree(idb->sourceid);
		if (idb->dbcs) CMFree(idb->dbcs);
		if (idb->pgcs) CMFree(idb->pgcs);
		if (idb->queries) CMUTIL_CALL(idb->queries, Destroy);
		if (idb->mfiles) CMUTIL_CALL(idb->mfiles, Destroy);
		if (idb->mfsets) CMUTIL_CALL(idb->mfsets, Destroy);
		if (idb->monitor) CMUTIL_CALL(idb->monitor, Cancel);
		if (idb->connpool) CMUTIL_CALL(idb->connpool, Destroy);
		if (idb->params) CMUTIL_JsonDestroy(idb->params);
		if (idb->testqry) CMUTIL_CALL(idb->testqry, Destroy);
		if (idb->poolconf) CMDBM_PoolConfigDestroy(idb->poolconf);
		if (idb->initres) idb->modif->CleanUp(idb->initres);
		if (idb->modif) CMFree(idb->modif);
		if (idb->rwlock) CMUTIL_CALL(idb->rwlock, Destroy);
		CMFree(idb);
	}
}

CMDBM_STATIC void CMDBM_DatabaseLockQueryItem(CMDBM_DatabaseEx *db)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	CMUTIL_CALL(idb->rwlock, ReadLock);
}

CMDBM_STATIC void CMDBM_DatabaseUnlockQueryItem(CMDBM_DatabaseEx *db)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	CMUTIL_CALL(idb->rwlock, ReadUnlock);
}

static CMDBM_DatabaseEx g_cmdbm_databse = {
	{
		CMDBM_DatabaseAddMapper,
		CMDBM_DatabaseAddMapperSet,
		CMDBM_DatabaseSetMonitor,
		CMDBM_DatabaseDestroy
	},
	CMDBM_DatabaseInitialize,
	CMDBM_DatabaseGetParams,
	CMDBM_DatabaseGetModuleIF,
	CMDBM_DatabaseGetInitResult,
	CMDBM_DatabaseGetId,
	CMDBM_DatabaseGetQuery,
	CMDBM_DatabaseGetConnection,
	CMDBM_DatabaseReleaseConnection,
	CMDBM_DatabaseLockQueryItem,
	CMDBM_DatabaseUnlockQueryItem
};

CMDBM_Database *CMDBM_DatabaseCreateCustom(
		const char *sourceid,
		CMDBM_ModuleInterface *modif,
		const char *dbcharset,
		CMDBM_PoolConfig *poolconf,
		CMUTIL_JsonObject *params)
{
	CMDBM_Database_Internal *res = CMAlloc(sizeof(CMDBM_Database_Internal));
	memset(res, 0x0, sizeof(CMDBM_Database_Internal));
	memcpy(res, &g_cmdbm_databse, sizeof(CMDBM_DatabaseEx));
	res->sourceid = CMStrdup(sourceid);
	res->dbcs = CMStrdup(dbcharset);
	res->mfiles = CMUTIL_MapCreateEx(64, CMDBM_MapperFileDestroy);
	res->mfsets = CMUTIL_MapCreateEx(64, CMDBM_MapperFileSetDestroy);
	res->modif = CMAlloc(sizeof(CMDBM_ModuleInterface));
	memcpy(res->modif, modif, sizeof(CMDBM_ModuleInterface));
	res->queries = CMUTIL_MapCreate();
	res->poolconf = CMDBM_PoolConfigClone(poolconf);
	res->params = (CMUTIL_JsonObject*)CMUTIL_CALL(&(params->parent), Clone);
	res->rwlock = CMUTIL_RWLockCreate();
	res->testqry = CMUTIL_StringCreateEx(64, modif->GetTestQuery());
	return (CMDBM_Database*)res;
}

#if defined(CMDBM_ODBC)
extern CMDBM_ModuleInterface g_cmdbm_odbc_interface;
#endif
#if defined(CMDBM_ORACLE)
extern CMDBM_ModuleInterface g_cmdbm_oracle_interface;
#endif
#if defined(CMDBM_MYSQL)
extern CMDBM_ModuleInterface g_cmdbm_mysql_interface;
#endif
#if defined(CMDBM_SQLITE)
extern CMDBM_ModuleInterface g_cmdbm_sqlite_interface;
#endif
#if defined(CMDBM_PGSQL)
extern CMDBM_ModuleInterface g_cmdbm_pgsql_interface;
#endif

static CMDBM_ModuleInterface *g_cmdbm_moduleifs[] = {
#if defined(CMDBM_ODBC)
	&g_cmdbm_odbc_interface,
#else
	NULL,
#endif
#if defined(CMDBM_ORACLE)
	&g_cmdbm_oracle_interface,
#else
	NULL,
#endif
#if defined(CMDBM_MYSQL)
	&g_cmdbm_mysql_interface,
#else
	NULL,
#endif
#if defined(CMDBM_SQLITE)
	&g_cmdbm_sqlite_interface,
#else
	NULL,
#endif
#if defined(CMDBM_PGSQL)
	&g_cmdbm_pgsql_interface,
#else
	NULL,
#endif
	NULL
};

static const char *g_cmdbm_dbtype_string[] = {
	"ODBC", "Oracle", "MySQL", "SQLite", "PgSQL", NULL
};

static const char *g_cmdbm_dbdefine[] = {
	"CMDBM_ODBC",
	"CMDBM_ORACLE",
	"CMDBM_MYSQL",
	"CMDBM_SQLITE",
	"CMDBM_PGSQL",
	NULL
};

CMDBM_DBType CMDBM_DatabaseType(const char *dbtype)
{
	int i;
	for (i=0; i<CMDBM_DBType_Unknown; i++) {
		if (strcasecmp(dbtype, g_cmdbm_dbtype_string[i]) == 0)
			return (CMDBM_DBType)i;
	}
	return CMDBM_DBType_Unknown;
}

CMDBM_Database *CMDBM_DatabaseCreate(
		const char *sourceid,
		CMDBM_DBType dbtype,
		const char *dbcharset,
		CMDBM_PoolConfig *poolconf,
		CMUTIL_JsonObject *params)
{
	if (dbtype < CMDBM_DBType_Custom) {
		if (g_cmdbm_moduleifs[dbtype]) {
			return CMDBM_DatabaseCreateCustom(
						sourceid, g_cmdbm_moduleifs[dbtype],
						dbcharset, poolconf, params);
		} else {
			CMLogError("db type(%s) is not supported. "
					   "recompile with '%s' flag.",
					   g_cmdbm_dbtype_string[dbtype],
					   g_cmdbm_dbdefine[dbtype]);
		}
	} else {
		CMLogError("unknown db type: %d", dbtype);
	}
	return NULL;
}

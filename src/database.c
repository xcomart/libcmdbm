
#include "functions.h"

CMUTIL_LogDefine("cmdbm.database")

typedef struct CMDBM_MapperFile {
	CMUTIL_XmlNode			*node;
	time_t					lastupdt;
	char					*fpath;
	CMUTIL_StringArray		*mapperids;
	CMUTIL_Map				*queries;
    CMBool				isinset;
    int                     dummy_padder;
} CMDBM_MapperFile;

typedef struct CMDBM_MapperFileSet {
	CMUTIL_Array			*mfileset;
	char					*dpath;
	char					*fpattern;
	CMUTIL_Map				*queries;
	CMBool				recursive;
    int                     dummy_padder;
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
	CMUTIL_JsonObject		*params;
	CMUTIL_String			*testqry;
    int						minterval;
    int                     dummy_padder;
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
			CMCall(mfile->mapperids, Destroy);
		if (mfile->node)
			CMCall(mfile->node, Destroy);
		if (mfile->queries)
			CMCall(mfile->queries, Destroy);
		if (mfile->fpath)
			CMFree(mfile->fpath);
		CMFree(mfile);
	}
}

CMDBM_STATIC CMDBM_MapperFile *CMDBM_MapperFileCreate(
		const char *fpath, CMUTIL_XmlNode *node,
		CMBool isinset, CMUTIL_Map *queries)
{
	CMDBM_MapperFile *res = NULL;
	CMUTIL_File *file = CMUTIL_FileCreate(fpath);
	if (CMCall(file, IsExists)) {
		res = CMAlloc(sizeof(CMDBM_MapperFile));
		memset(res, 0x0, sizeof(CMDBM_MapperFile));
		res->node = node;
		res->isinset = isinset;
		res->lastupdt = CMCall(file, ModifiedTime);
		res->mapperids = CMCall(queries, GetKeys);
		res->queries = queries;
		res->fpath = CMStrdup(fpath);
	}
	CMCall(file, Destroy);
	return res;
}

CMDBM_STATIC void CMDBM_DatabaseRemoveFile(
		CMDBM_Database_Internal *idb, const char *fpath)
{
    uint32_t i;
	CMDBM_MapperFile *mfile = NULL;
	mfile = (CMDBM_MapperFile*)CMCall(idb->mfiles, Remove, fpath);
	if (mfile) {
		for (i=0; i<CMCall(mfile->mapperids, GetSize); i++) {
			const CMUTIL_String *sid = CMCall(mfile->mapperids, GetAt, i);
			const char *id = CMCall(sid, GetCString);
			CMCall(idb->queries, Remove, id);
		}
		CMDBM_MapperFileDestroy(mfile);
	}
}

CMDBM_STATIC CMBool CMDBM_DatabaseAddMapper(
		CMDBM_Database *db, const char *mapperfile)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	CMBool res = CMFalse;
	CMUTIL_XmlNode *mapper = CMUTIL_XmlParseFile(mapperfile);
	if (mapper) {
		CMUTIL_Map *queries = CMUTIL_MapCreate();
		if (CMDBM_MapperRebuildItem(queries, mapper)) {
			CMDBM_MapperFile *mfile = CMDBM_MapperFileCreate(
						mapperfile, mapper, CMFalse, queries);
			if (mfile != NULL) {
				CMCall(idb->rwlock, WriteLock);
				CMDBM_DatabaseRemoveFile(idb, mapperfile);
				CMCall(idb->mfiles, Put, mapperfile, mfile, NULL);
				CMCall(idb->queries, PutAll, queries);
				CMCall(idb->rwlock, WriteUnlock);
				queries = NULL;
				res = CMTrue;
			} else {
				CMLogError("mapper file not exists(%s).", mapperfile);
			}
		} else {
			CMLogError("invalid mapper structure(%s).", mapperfile);
		}
		if (queries)
			CMCall(queries, Destroy);
	} else {
		CMLogError("XML file(%s) parse failed. skipping.", mapperfile);
	}
	if (!res && mapper) CMCall(mapper, Destroy);
	return res;
}

CMDBM_STATIC int CMDBM_DatabaseMapperComp(const void *a, const void *b)
{
    const CMDBM_MapperFile *fa = (const CMDBM_MapperFile*)a;
    const CMDBM_MapperFile *fb = (const CMDBM_MapperFile*)b;
	return strcmp(fa->fpath, fb->fpath);
}

CMDBM_STATIC void CMDBM_MapperFileSetDestroy(void *data)
{
	CMDBM_MapperFileSet *mfset = (CMDBM_MapperFileSet*)data;
	if (mfset) {
		if (mfset->dpath) CMFree(mfset->dpath);
		if (mfset->fpattern) CMFree(mfset->fpattern);
		if (mfset->mfileset) CMCall(mfset->mfileset, Destroy);
		if (mfset->queries) CMCall(mfset->queries, Destroy);
		CMFree(mfset);
	}
}

CMDBM_STATIC CMDBM_MapperFileSet *CMDBM_DatabaseBuidlMapperSet(
		const char *dpath, const char  *fpattern, CMBool recursive)
{
    uint32_t i;
	CMUTIL_File *dfile = CMUTIL_FileCreate(dpath);
	CMUTIL_FileList *flist = CMCall(dfile, Find, fpattern, recursive);
	CMDBM_MapperFileSet *mset = CMAlloc(sizeof(CMDBM_MapperFileSet));
	memset(mset, 0x0, sizeof(CMDBM_MapperFileSet));
	mset->dpath = CMStrdup(dpath);
	mset->fpattern = CMStrdup(fpattern);
	mset->recursive = recursive;
	mset->mfileset = CMUTIL_ArrayCreateEx(
				10, CMDBM_DatabaseMapperComp, CMDBM_MapperFileDestroy);
	mset->queries = CMUTIL_MapCreate();
	for (i=0; i<CMCall(flist, Count); i++) {
		CMBool succ = CMFalse;
		CMUTIL_File *cur = CMCall(flist, GetAt, i);
		const char *fpath = CMCall(cur, GetFullPath);
		CMUTIL_XmlNode *mapper = CMUTIL_XmlParseFile(fpath);
		if (mapper) {
			CMUTIL_Map *fqrys = CMUTIL_MapCreate();
			if (CMDBM_MapperRebuildItem(fqrys, mapper)) {
				CMDBM_MapperFile *mfile = CMDBM_MapperFileCreate(
							fpath, mapper, CMTrue, fqrys);
				CMCall(mset->mfileset, Add, mfile, NULL);
				CMCall(mset->queries, PutAll, fqrys);
				fqrys = NULL;
				succ = CMTrue;
			} else {
				CMLogError("invalid mapper structure(%s). skipped.", fpath);
			}
			if (fqrys)
				CMCall(fqrys, Destroy);
		} else {
			CMLogError("invliad xml(%s). skipped",
					   CMCall(cur, GetFullPath));
		}
		if (!succ && mapper)
			CMCall(mapper, Destroy);
	}
	CMCall(flist, Destroy);
	CMCall(dfile, Destroy);
	return mset;
}

CMDBM_STATIC void CMDBM_DatabaseRemoveMapperSet(
		CMDBM_Database *db, const char *key)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	CMDBM_MapperFileSet *mset = NULL;

	mset = (CMDBM_MapperFileSet*)CMCall(idb->mfsets, Get, key);
	if (mset) {
        uint32_t i;
		CMUTIL_StringArray *qkeys = NULL;
		qkeys = CMCall(mset->queries, GetKeys);
		if (qkeys) {
			for (i=0; i<CMCall(qkeys, GetSize); i++) {
				const char *qkey = CMCall(qkeys, GetCString, i);
				CMCall(idb->queries, Remove, qkey);
			}
			CMCall(qkeys, Destroy);
		}
		CMCall(idb->mfsets, Remove, key);
		CMDBM_MapperFileDestroy(mset);
	} else {
		CMLogWarn("mapper set(%s) does not exists in this database",
				  key);
	}
}

CMDBM_STATIC CMBool CMDBM_DatabaseAddMapperSet(
        CMDBM_Database *db, const char *dpath, const char *fpattern,
        CMBool recursive)
{
    char key[1024];
    CMBool res = CMTrue;
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	CMDBM_MapperFileSet *mset = CMDBM_DatabaseBuidlMapperSet(
				dpath, fpattern, recursive);
	sprintf(key, "%s;%s", dpath, fpattern);
	CMCall(idb->rwlock, WriteLock);
	CMCall(idb->mfsets, Put, key, mset, NULL);
	CMCall(idb->queries, PutAll, mset->queries);
	CMCall(idb->rwlock, WriteUnlock);
    return res;
}

CMDBM_STATIC CMBool CMDBM_DatabaseSetMonitor(
		CMDBM_Database *db, int interval)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	idb->minterval = interval;
	return CMTrue;
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
		CMCall(conn, CloseReal);
	CMUTIL_UNUSED(data);
}

CMDBM_STATIC CMBool CMDBM_DatabasePoolTestProc(
		void *resource, void *data)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)data;
	CMDBM_Connection *conn = (CMDBM_Connection*)resource;
	CMUTIL_JsonValue *val = CMCall(
				conn, GetObject, idb->testqry, NULL, NULL);
	if (val) {
		CMUTIL_JsonDestroy(val);
		return CMTrue;
	}
	return CMFalse;
}

CMDBM_STATIC void CMDBM_DatabaseMapperReloader(void *data)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)data;
	CMUTIL_Array *toberep = CMUTIL_ArrayCreate();
	CMUTIL_Array *toberem = CMUTIL_ArrayCreate();
	CMUTIL_Iterator *iter = NULL;

	iter = CMCall(idb->mfiles, Iterator);
	while (CMCall(iter, HasNext)) {
		CMDBM_MapperFile *mf = (CMDBM_MapperFile*)CMCall(iter, Next);
		CMUTIL_File *f = CMUTIL_FileCreate(mf->fpath);
		if (CMCall(f, IsExists)) {
			if (CMCall(f, ModifiedTime) != mf->lastupdt)
				CMCall(toberep, Add, mf, NULL);
		} else {
			CMCall(toberem, Add, mf, NULL);
		}
		CMCall(f, Destroy);
	}
	CMCall(iter, Destroy);

	// replace existing mapper
	if (CMCall(toberep, GetSize) > 0) {
		CMCall(idb->rwlock, WriteLock);
		while (CMCall(toberep, GetSize) > 0) {
			CMDBM_MapperFile *mf =
					(CMDBM_MapperFile*)CMCall(toberep, RemoveAt, 0);
			CMLogInfo("mapper file changed: %s", mf->fpath);
			// mf->fpath will be freed while processing. so need to backup.
			char *fpath = CMStrdup(mf->fpath);
			CMDBM_DatabaseAddMapper((CMDBM_Database*)idb, fpath);
			CMFree(fpath);
		}
		CMCall(idb->rwlock, WriteUnlock);
	}

	// remove existing mapper
	if (CMCall(toberem, GetSize) > 0) {
		CMCall(idb->rwlock, WriteLock);
		while (CMCall(toberem, GetSize) > 0) {
			CMDBM_MapperFile *mf =
					(CMDBM_MapperFile*)CMCall(toberem, RemoveAt, 0);
			CMLogInfo("mapper file removed: %s", mf->fpath);
			CMDBM_DatabaseRemoveFile(idb, mf->fpath);
		}
		CMCall(idb->rwlock, WriteUnlock);
	}

	iter = CMCall(idb->mfsets, Iterator);
	while (CMCall(iter, HasNext)) {
        uint32_t i, j;
		CMDBM_MapperFileSet *mset =
				(CMDBM_MapperFileSet*)CMCall(iter, Next);
		CMUTIL_File *dir = CMUTIL_FileCreate(mset->dpath);
		CMUTIL_FileList *flist =
				CMCall(dir, Find, mset->fpattern, mset->recursive);
		CMBool ischanged = CMFalse;

		// compare file lists
		for (i=0; !ischanged && i<CMCall(flist, Count); i++) {
			CMUTIL_File *f = CMCall(flist, GetAt, i);
			for (j=0; j<CMCall(mset->mfileset, GetSize); j++) {
				CMDBM_MapperFile *mf = (CMDBM_MapperFile*)
						CMCall(mset->mfileset, GetAt, j);
				CMLogTrace("comparing %s,%s",
						   CMCall(f, GetFullPath), mf->fpath);
				if (strcmp(CMCall(f, GetFullPath), mf->fpath) == 0) {
					if (CMCall(f, ModifiedTime) != mf->lastupdt)
						ischanged = CMTrue;
					break;
				}
			}
		}

		if (ischanged)
			CMCall(toberep, Add, mset, NULL);
		CMCall(flist, Destroy);
		CMCall(dir, Destroy);
	}
	CMCall(iter, Destroy);

	// replace existing mapper file set
	if (CMCall(toberep, GetSize) > 0) {
		while (CMCall(toberep, GetSize) > 0) {
			CMDBM_MapperFileSet *mset = CMCall(toberep, RemoveAt, 0);
			CMDBM_MapperFileSet *newset = NULL;
			CMLogInfo("mapper set(%s/%s) changed. reloading...",
					  mset->dpath, mset->fpattern);
			newset = CMDBM_DatabaseBuidlMapperSet(
						mset->dpath, mset->fpattern, mset->recursive);
			if (newset) {
				char buf[1024];
				sprintf(buf, "%s;%s", mset->dpath, mset->fpattern);
				CMCall(idb->rwlock, WriteLock);
				CMDBM_DatabaseRemoveMapperSet((CMDBM_Database*)idb, buf);
				CMCall(idb->mfsets, Put, buf, mset, NULL);
				CMCall(idb->queries, PutAll, mset->queries);
				CMCall(idb->rwlock, WriteUnlock);
			}
		}
	}

	CMCall(toberep, Destroy);
	CMCall(toberem, Destroy);
}

CMDBM_STATIC CMBool CMDBM_DatabaseInitialize(
		CMDBM_DatabaseEx *db, CMUTIL_Timer *timer, const char *pgcs)
{
	long interval;
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	idb->pgcs = CMStrdup(pgcs);
	idb->initres = idb->modif->Initialize(idb->dbcs, idb->pgcs);
	idb->connpool = CMUTIL_PoolCreate(
                (int)idb->poolconf->initcnt,
                (int)idb->poolconf->maxcnt,
				CMDBM_DatabasePoolCreateProc,
				CMDBM_DatabasePoolDestroyProc,
				CMDBM_DatabasePoolTestProc,
				30, CMTrue, idb, timer);

	// do initial mapper load
	CMDBM_DatabaseMapperReloader(idb);

	// schedule mapper reloader
	if (idb->minterval == 0)
		idb->minterval = 30;
	interval = idb->minterval * 1000;
	idb->monitor = CMCall(timer, ScheduleDelayRepeat, interval, interval,
							   CMTrue, CMDBM_DatabaseMapperReloader, idb);
	return idb->connpool == NULL? CMFalse:CMTrue;
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
	CMUTIL_XmlNode *res = (CMUTIL_XmlNode*)CMCall(idb->queries, Get, id);
	if (res == NULL)
		CMLogErrorS("datasource '%s' has no query with id '%s'.",
					idb->sourceid, id);
	return res;
}

CMDBM_STATIC CMDBM_Connection *CMDBM_DatabaseGetConnection(
		CMDBM_DatabaseEx *db)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	CMDBM_Connection *res = CMCall(idb->connpool, CheckOut, 5000);
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
	CMCall(idb->connpool, Release, conn);
}

CMDBM_STATIC void CMDBM_DatabaseDestroy(
		CMDBM_Database *db)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	if (idb) {
		if (idb->sourceid) CMFree(idb->sourceid);
		if (idb->dbcs) CMFree(idb->dbcs);
		if (idb->pgcs) CMFree(idb->pgcs);
		if (idb->queries) CMCall(idb->queries, Destroy);
		if (idb->mfiles) CMCall(idb->mfiles, Destroy);
		if (idb->mfsets) CMCall(idb->mfsets, Destroy);
		if (idb->monitor) CMCall(idb->monitor, Cancel);
		if (idb->connpool) CMCall(idb->connpool, Destroy);
		if (idb->params) CMUTIL_JsonDestroy(idb->params);
		if (idb->testqry) CMCall(idb->testqry, Destroy);
		if (idb->poolconf) CMDBM_PoolConfigDestroy(idb->poolconf);
		if (idb->initres) idb->modif->CleanUp(idb->initres);
		if (idb->modif) CMFree(idb->modif);
		if (idb->rwlock) CMCall(idb->rwlock, Destroy);
		CMFree(idb);
	}
}

CMDBM_STATIC void CMDBM_DatabaseLockQueryItem(CMDBM_DatabaseEx *db)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	CMCall(idb->rwlock, ReadLock);
}

CMDBM_STATIC void CMDBM_DatabaseUnlockQueryItem(CMDBM_DatabaseEx *db)
{
	CMDBM_Database_Internal *idb = (CMDBM_Database_Internal*)db;
	CMCall(idb->rwlock, ReadUnlock);
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
    res->mfiles = CMUTIL_MapCreateEx(
                64, CMFalse, CMDBM_MapperFileDestroy, 0.75f);
    res->mfsets = CMUTIL_MapCreateEx(
                64, CMFalse, CMDBM_MapperFileSetDestroy, 0.75f);
	res->modif = CMAlloc(sizeof(CMDBM_ModuleInterface));
	memcpy(res->modif, modif, sizeof(CMDBM_ModuleInterface));
	res->queries = CMUTIL_MapCreate();
	res->poolconf = CMDBM_PoolConfigClone(poolconf);
	res->params = (CMUTIL_JsonObject*)CMCall(&(params->parent), Clone);
	res->rwlock = CMUTIL_RWLockCreate();
	res->testqry = CMUTIL_StringCreateEx(64, modif->GetTestQuery());
	return (CMDBM_Database*)res;
}

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

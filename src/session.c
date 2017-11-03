
#include "functions.h"

CMUTIL_LogDefine("cmdbm.session")

typedef struct CMDBM_Session_Internal {
	CMDBM_Session	base;
	CMUTIL_Map		*conns;
	CMDBM_ContextEx	*ctx;
	CMUTIL_Bool		istrans;
    int             dummy_padder;
} CMDBM_Session_Internal;

#define CMDBM_SessionTrans(isess, method) do {\
	if (CMCall(isess->conns, GetSize) > 0) {\
		CMUTIL_Iterator *iter = CMCall(isess->conns, Iterator);\
		while (CMCall(iter, HasNext)) {\
			CMDBM_Connection *conn =\
					(CMDBM_Connection*)CMCall(iter, Next);\
			CMCall(conn, method);\
		}\
		CMCall(iter, Destroy);\
	}\
} while(0)

CMDBM_STATIC CMUTIL_Bool CMDBM_SessionBeginTransaction(CMDBM_Session *sess)
{
	CMDBM_Session_Internal *isess = (CMDBM_Session_Internal*)sess;
	if (!isess->istrans) {
		CMDBM_SessionTrans(isess, BeginTransaction);
		isess->istrans = CMTrue;
	} else {
		CMLogWarnS("transaction started already.");
	}
	return isess->istrans;
}

CMDBM_STATIC void CMDBM_SessionEndTransaction(CMDBM_Session *sess)
{
	CMDBM_Session_Internal *isess = (CMDBM_Session_Internal*)sess;
	if (isess->istrans) {
		CMDBM_SessionTrans(isess, EndTransaction);
		isess->istrans = CMFalse;
	} else {
		CMLogWarnS("transaction not started.");
	}
}

CMDBM_STATIC CMUTIL_Bool CMDBM_SessionCommit(CMDBM_Session *sess)
{
	CMDBM_Session_Internal *isess = (CMDBM_Session_Internal*)sess;
	if (isess->istrans) {
		CMDBM_SessionTrans(isess, Commit);
		return CMTrue;
	} else {
		CMLogWarnS("transaction not started.");
		return CMFalse;
	}
}

CMDBM_STATIC void CMDBM_SessionRollback(CMDBM_Session *sess)
{
	CMDBM_Session_Internal *isess = (CMDBM_Session_Internal*)sess;
	if (isess->istrans) {
		CMDBM_SessionTrans(isess, Rollback);
	} else {
		CMLogWarnS("transaction not started.");
	}
}

CMDBM_STATIC void CMDBM_SessionClose(CMDBM_Session *sess)
{
	CMDBM_Session_Internal *isess = (CMDBM_Session_Internal*)sess;
	if (isess) {
		CMDBM_SessionTrans(isess, Close);
		CMCall(isess->conns, Destroy);
		CMFree(isess);
	}
}

CMDBM_STATIC CMDBM_Connection *CMDBM_SessionGetConnection(
		CMDBM_Session_Internal *isess, const char *dbid)
{
	CMDBM_Connection *conn = CMCall(isess->conns, Get, dbid);
	if (conn == NULL) {
		CMDBM_DatabaseEx *db = CMCall(isess->ctx, GetDatabase, dbid);
		if (db) {
			conn = CMCall(db, GetConnection);
			if (conn)
				CMCall(isess->conns, Put, dbid, conn);
			else
				CMLogErrorS("cannot get connection from source '%s'", dbid);
		}
	}
	return conn;
}

CMDBM_STATIC CMUTIL_String *CMDBM_SessionGetQuery(
		CMDBM_Session *sess, const char *dbid, const char *sqlid,
		CMUTIL_JsonObject *params, CMUTIL_JsonArray **binds,
		CMUTIL_JsonObject **outs, CMUTIL_List **after, CMUTIL_List **rembuf)
{
	CMDBM_Session_Internal *isess = (CMDBM_Session_Internal*)sess;
	CMDBM_DatabaseEx *db = CMCall(isess->ctx, GetDatabase, dbid);
	CMDBM_Connection *conn = NULL;
	CMUTIL_String *query = NULL;
	CMUTIL_XmlNode *xqry = NULL;
	CMUTIL_Bool succ = CMFalse;
	if (!db) {
		CMLogErrorS("unknown datasource id: %s.", dbid);
		goto ENDPOINT;
	}
	xqry = CMCall(db, GetQuery, sqlid);
	if (!xqry) {
		CMLogErrorS("unknown query id '%s' in datasource %s.", sqlid, dbid);
		goto ENDPOINT;
	}
	query = CMUTIL_StringCreate();
	*binds = CMUTIL_JsonArrayCreate();
	*outs = CMUTIL_JsonObjectCreate();
	*after = CMUTIL_ListCreate();
	*rembuf = CMUTIL_ListCreateEx(CMFree);

	conn = CMDBM_SessionGetConnection(isess, dbid);
	if (!conn) goto ENDPOINT;

	CMCall(db, LockQueryItem);
	succ = CMDBM_BuildNode(sess, conn, xqry, params, *binds, *after,
						   query, *outs, *rembuf);
ENDPOINT:
	if (!succ) {
		if (db)
			CMCall(db, UnlockQueryItem);
		if (*outs) CMUTIL_JsonDestroy(*outs);
		if (*after) CMCall(*after, Destroy);
		if (*rembuf) CMCall(*rembuf, Destroy);
		if (*binds) CMUTIL_JsonDestroy(*binds);
		if (query) CMCall(query, Destroy);
		*outs = NULL;
		*after = NULL;
		*rembuf = NULL;
		*binds = NULL;
		query = NULL;
	}
	CMLogDebug("%s.%s - %s", dbid, sqlid, CMCall(query, GetCString));
	return query;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_SessionExecAfters(
		CMDBM_Session *sess, const char *dbid, CMUTIL_JsonObject *params,
		CMUTIL_List *after, CMUTIL_List *rembuf)
{
	CMUTIL_Bool res = CMFalse;
	CMDBM_Session_Internal *isess = (CMDBM_Session_Internal*)sess;
	CMDBM_Connection *conn = CMDBM_SessionGetConnection(isess, dbid);
	CMUTIL_String *dummy = CMUTIL_StringCreate();
	if (conn) {
		res = CMTrue;
		while (res && CMCall(after, GetSize) > 0) {
			CMUTIL_JsonArray *binds = CMUTIL_JsonArrayCreate();
			// must call GetFront not RemoveFront. front node will be removed
			// from after list automatically in CMDBM_BuildNode.
			CMUTIL_XmlNode *qry =
					(CMUTIL_XmlNode*)CMCall(after, GetFront);
			res = CMDBM_BuildNode(sess, conn, qry, params, binds, after,
								  dummy, NULL, rembuf);
			CMUTIL_JsonDestroy(binds);
		}
	} else {
		CMLogError("cannot get connection from source: %s", dbid);
	}
	CMCall(dummy, Destroy);
	return res;
}

CMDBM_STATIC void CMDBM_SessionCleanUp(
		CMDBM_Session_Internal *isess, const char *dbid, CMUTIL_String *query,
		CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs,
		CMUTIL_List *after, CMUTIL_List *rembuf)
{
	CMDBM_DatabaseEx *db = CMCall(isess->ctx, GetDatabase, dbid);
	CMCall(db, UnlockQueryItem);
	if (outs) {
        uint32_t i;
		CMUTIL_StringArray *keyset = CMCall(outs, GetKeys);
		for (i=0; i<CMCall(keyset, GetSize); i++) {
			const char *key = CMCall(keyset, GetCString, i);
			CMCall(outs, Remove, key);
		}
		CMCall(keyset, Destroy);
		CMUTIL_JsonDestroy(outs);
	}
	if (binds) CMUTIL_JsonDestroy(binds);
	if (after) CMCall(after, Destroy);
	if (rembuf) CMCall(rembuf, Destroy);
	if (query) CMCall(query, Destroy);
}

#define CMDBM_SessionRun(t,i,m,d) do {\
	t res = i;\
	CMDBM_Session_Internal *isess = (CMDBM_Session_Internal*)sess;\
	CMDBM_Connection *conn = CMDBM_SessionGetConnection(isess, dbid);\
	CMUTIL_JsonObject *outs = NULL;\
	CMUTIL_JsonArray *binds = NULL;\
	CMUTIL_List *after = NULL, *rembuf = NULL;\
	CMUTIL_String *query = NULL;\
	query = CMDBM_SessionGetQuery(\
					sess, dbid, sqlid, params, &binds, &outs, &after, &rembuf);\
	if (query) {\
		res = conn->m(conn, query, binds, outs);\
		if (res != i) {\
			if (!CMDBM_SessionExecAfters(sess, dbid, params, after, rembuf)) {\
				CMLogErrorS("selectKey part of %s.%s execution failed.",\
							dbid, sqlid);\
				d(res);\
				res = i;\
			}\
		} else {\
			CMLogErrorS("%s.%s query execution failed. -> %s",\
						dbid, sqlid, CMCall(query, GetCString));\
		}\
		CMDBM_SessionCleanUp(isess, dbid, query, binds, outs, after, rembuf);\
	}\
	return res;\
} while(0)

CMDBM_STATIC void CMDBM_SessionItemDestroyerDummy(int a)
{
	(void)a;
}

CMDBM_STATIC void CMDBM_SessionItemDestroyerJson(void *json)
{
	CMUTIL_JsonDestroy(json);
}

CMDBM_STATIC int CMDBM_SessionExecute(
		CMDBM_Session *sess, const char *dbid,
		const char*sqlid, CMUTIL_JsonObject *params)
{
	CMDBM_SessionRun(int, -1, Execute, CMDBM_SessionItemDestroyerDummy);
}

CMDBM_STATIC CMUTIL_JsonValue *CMDBM_SessionGetObject(
		CMDBM_Session *sess, const char *dbid,
		const char *sqlid, CMUTIL_JsonObject *params)
{
	CMDBM_SessionRun(CMUTIL_JsonValue*, NULL, GetObject,
					 CMDBM_SessionItemDestroyerJson);
}

CMDBM_STATIC CMUTIL_JsonObject *CMDBM_SessionGetRow(
		CMDBM_Session *sess, const char *dbid,
		const char *sqlid, CMUTIL_JsonObject *params)
{
	CMDBM_SessionRun(CMUTIL_JsonObject*, NULL, GetRow,
					 CMDBM_SessionItemDestroyerJson);
}

CMDBM_STATIC CMUTIL_JsonArray *CMDBM_SessionGetRowSet(
		CMDBM_Session *sess, const char *dbid,
		const char *sqlid, CMUTIL_JsonObject *params)
{
	/*
	CMDBM_SessionRun(CMUTIL_JsonArray*, NULL, GetList,
					 CMDBM_SessionItemDestroyerJson);
	*/
	CMUTIL_JsonArray* res = NULL;
	CMDBM_Session_Internal *isess = (CMDBM_Session_Internal*)sess;
	CMDBM_Connection *conn = CMDBM_SessionGetConnection(isess, dbid);
	CMUTIL_JsonObject *outs = NULL;
	CMUTIL_JsonArray *binds = NULL;
	CMUTIL_List *after = NULL, *rembuf = NULL;
	CMUTIL_String *query = CMDBM_SessionGetQuery(
				sess, dbid, sqlid, params, &binds, &outs, &after, &rembuf);
	if (query) {
		res = conn->GetList(conn, query, binds, outs);
		if (res != NULL) {
			if (!CMDBM_SessionExecAfters(sess, dbid, params, after, rembuf)) {
				CMLogErrorS("selectKey part of %s.%s execution failed.",
							dbid, sqlid);
				CMDBM_SessionItemDestroyerJson(res);
				res = NULL;
			}
		} else {
			CMLogErrorS("%s.%s query execution failed. -> %s",
						dbid, sqlid, CMCall(query, GetCString));
		}
		CMDBM_SessionCleanUp(isess, dbid, query, binds, outs, after, rembuf);
	}
	return res;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_SessionForEachRow(
		CMDBM_Session *sess, const char *dbid, const char *sqlid,
		CMUTIL_JsonObject *params, void *udata,
        CMUTIL_Bool (*rowcb)(CMUTIL_JsonObject*, uint32_t, void*))
{
	CMUTIL_Bool res = CMFalse;
	CMDBM_Session_Internal *isess = (CMDBM_Session_Internal*)sess;
	CMDBM_Connection *conn = CMDBM_SessionGetConnection(isess, dbid);
	CMUTIL_JsonObject *outs = NULL;
	CMUTIL_JsonArray *binds = NULL;
	CMUTIL_List *after = NULL, *rembuf = NULL;
	CMUTIL_String *query = CMDBM_SessionGetQuery(
				sess, dbid, sqlid, params, &binds, &outs, &after, &rembuf);
	if (query) {
		CMDBM_Cursor *csr = conn->OpenCursor(conn, query, binds, outs);
		if (csr != NULL) {
			if (!CMDBM_SessionExecAfters(sess, dbid, params, after, rembuf)) {
				CMLogErrorS("selectKey part of %s.%s execution failed.",
							dbid, sqlid);
			} else {
                uint32_t idx = 0;
				CMUTIL_JsonObject *row = NULL;
				CMUTIL_Bool cont = CMTrue;
				while (cont && ((row = CMCall(csr, GetNext)) != NULL)) {
					cont = rowcb(row, idx++, udata);
					CMUTIL_JsonDestroy(row);
				}
				res = CMTrue;
			}
		} else {
			CMLogErrorS("%s.%s query execution failed. -> %s",
						dbid, sqlid, CMCall(query, GetCString));
		}
		CMDBM_SessionCleanUp(isess, dbid, query, binds, outs, after, rembuf);
	}
	return res;
}

static CMDBM_Session g_cmdbm_session = {
	CMDBM_SessionBeginTransaction,
	CMDBM_SessionEndTransaction,
	CMDBM_SessionExecute,
	CMDBM_SessionGetObject,
	CMDBM_SessionGetRow,
	CMDBM_SessionGetRowSet,
	CMDBM_SessionForEachRow,
	CMDBM_SessionCommit,
	CMDBM_SessionRollback,
	CMDBM_SessionClose
};

CMDBM_Session *CMDBM_SessionCreate(CMDBM_ContextEx *ctx)
{
	CMDBM_Session_Internal *res = CMAlloc(sizeof(CMDBM_Session_Internal));
	memset(res, 0x0, sizeof(CMDBM_Session_Internal));
	memcpy(res, &g_cmdbm_session, sizeof(CMDBM_Session));
	res->conns = CMUTIL_MapCreate();
	res->ctx = ctx;
	return (CMDBM_Session*)res;
}

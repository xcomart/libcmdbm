
#include "functions.h"

CMUTIL_LogDefine("cmdbm.session")

typedef struct CMDBM_Session_Internal {
    CMDBM_Session   base;
    CMUTIL_Map      *conns;
    CMDBM_ContextEx *ctx;
    CMBool          istrans;
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

CMDBM_STATIC CMBool CMDBM_SessionBeginTransaction(CMDBM_Session *sess)
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

CMDBM_STATIC CMBool CMDBM_SessionCommit(CMDBM_Session *sess)
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
        if (isess->istrans) {
            // a connection must never go back to the pool with an
            // uncommitted transaction on it.
            CMLogWarnS("session closed in a transaction. rolling back.");
            CMDBM_SessionTrans(isess, Rollback);
            CMDBM_SessionTrans(isess, EndTransaction);
            isess->istrans = CMFalse;
        }
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
            if (conn) {
                CMCall(isess->conns, Put, dbid, conn, NULL);
                // connections are borrowed lazily, so a connection acquired
                // after BeginTransaction must join the running transaction.
                if (isess->istrans) {
                    if (!CMCall(conn, BeginTransaction))
                        CMLogErrorS("cannot start transaction on source '%s'",
                                    dbid);
                }
            } else {
                CMLogErrorS("cannot get connection from source '%s'", dbid);
            }
        }
    }
    return conn;
}

// bind/out values are references owned by the caller's params object:
// they must be detached before their container is destroyed.
CMDBM_STATIC void CMDBM_SessionClearBinds(CMUTIL_JsonArray *binds)
{
    size_t size;
    while ((size = CMCall(binds, GetSize)) > 0)
        CMCall(binds, Remove, (uint32_t)(size-1));
}

// attribute of the executed statement tag. attribute names are matched
// case sensitively by the xml parser, so the lowercased spelling is
// accepted too.
CMDBM_STATIC const char *CMDBM_SessionAttribute(
        CMUTIL_XmlNode *node, const char *aname, const char *alower)
{
    CMUTIL_String *sattr = NULL;
    if (node == NULL)
        return NULL;
    sattr = CMCall(node, GetAttribute, aname);
    if (sattr == NULL)
        sattr = CMCall(node, GetAttribute, alower);
    return sattr? CMCall(sattr, GetCString):NULL;
}

// 'fetchSize' attribute, 0 when it is not given.
CMDBM_STATIC uint32_t CMDBM_SessionFetchSize(CMUTIL_XmlNode *node)
{
    long fsize;
    const char *sfsize = CMDBM_SessionAttribute(
                node, "fetchSize", "fetchsize");
    if (sfsize == NULL)
        return 0;
    fsize = strtol(sfsize, NULL, 10);
    if (fsize <= 0) {
        CMLogWarnS("invalid fetchSize attribute: '%s'. ignored.", sfsize);
        return 0;
    }
    return (uint32_t)fsize;
}

// 'resultType' attribute. 'value' reduces each row to its first column.
CMDBM_STATIC CMBool CMDBM_SessionIsValueResult(CMUTIL_XmlNode *node)
{
    const char *rtype = CMDBM_SessionAttribute(
                node, "resultType", "resulttype");
    if (rtype == NULL)
        return CMFalse;
    if (strcasecmp(rtype, "value") == 0)
        return CMTrue;
    if (strcasecmp(rtype, "map") != 0)
        CMLogWarnS("unknown resultType attribute: '%s'. 'map' is used.", rtype);
    return CMFalse;
}

// builds a value list out of a row list, destroying the given row list.
// each row contributes the value of its first column, null when the row
// has no column at all.
CMDBM_STATIC CMUTIL_JsonArray *CMDBM_SessionToValueList(
        CMUTIL_JsonArray *rows)
{
    uint32_t i, size = (uint32_t)CMCall(rows, GetSize);
    CMUTIL_JsonArray *res = CMUTIL_JsonArrayCreate();
    for (i=0; i<size; i++) {
        CMUTIL_Json *item = CMCall(rows, Get, i);
        CMUTIL_Json *value = NULL;
        if (CMCall(item, GetType) == CMJsonTypeObject) {
            CMUTIL_JsonObject *row = (CMUTIL_JsonObject*)item;
            CMUTIL_StringArray *keys = CMCall(row, GetKeys);
            if (CMCall(keys, GetSize) > 0) {
                const char *key = CMCall(keys, GetCString, 0);
                // detach: the value is owned by the result list from now on.
                value = CMCall(row, Remove, key);
            }
            CMCall(keys, Destroy);
        }
        if (value == NULL) {
            CMUTIL_JsonValue *nval = CMUTIL_JsonValueCreate();
            CMLogWarnS("row has no column. null is used for value result.");
            CMCall(nval, SetNull);
            value = (CMUTIL_Json*)nval;
        }
        CMCall(res, Add, value);
    }
    // rows are empty objects now, destroying them is safe.
    CMUTIL_JsonDestroy(rows);
    return res;
}

CMDBM_STATIC CMUTIL_String *CMDBM_SessionGetQuery(
        CMDBM_Session *sess, const char *dbid, const char *sqlid,
        CMUTIL_JsonObject *params, CMUTIL_JsonArray **binds,
        CMUTIL_JsonObject **outs, CMUTIL_List **after, CMUTIL_List **rembuf,
        CMUTIL_XmlNode **qnode)
{
    CMDBM_Session_Internal *isess = (CMDBM_Session_Internal*)sess;
    CMDBM_DatabaseEx *db = CMCall(isess->ctx, GetDatabase, dbid);
    CMDBM_Connection *conn = NULL;
    CMUTIL_String *query = NULL;
    CMUTIL_XmlNode *xqry = NULL;
    CMBool succ = CMFalse, locked = CMFalse;
    if (!db) {
        CMLogErrorS("unknown datasource id: %s.", dbid);
        goto ENDPOINT;
    }
    // lock must be held before touching query items, otherwise the
    // mapper reloader can destroy the node while it is in use.
    CMCall(db, LockQueryItem);
    locked = CMTrue;
    xqry = CMCall(db, GetQuery, sqlid);
    if (!xqry) {
        CMLogErrorS("unknown query id '%s' in datasource %s.", sqlid, dbid);
        goto ENDPOINT;
    }
    // the node stays valid until the query lock is released by
    // CMDBM_SessionCleanUp, its attributes may be read until then.
    *qnode = xqry;
    query = CMUTIL_StringCreate();
    *binds = CMUTIL_JsonArrayCreate();
    *outs = CMUTIL_JsonObjectCreate();
    *after = CMUTIL_ListCreate();
    *rembuf = CMUTIL_ListCreateEx(CMFree);

    conn = CMDBM_SessionGetConnection(isess, dbid);
    if (!conn) goto ENDPOINT;

    succ = CMDBM_BuildNode(sess, conn, xqry, params, *binds, *after,
                           query, *outs, *rembuf);
ENDPOINT:
    if (!succ) {
        if (locked)
            CMCall(db, UnlockQueryItem);
        if (*outs) CMUTIL_JsonDestroy(*outs);
        if (*after) CMCall(*after, Destroy);
        if (*rembuf) CMCall(*rembuf, Destroy);
        if (*binds) {
            CMDBM_SessionClearBinds(*binds);
            CMUTIL_JsonDestroy(*binds);
        }
        if (query) CMCall(query, Destroy);
        *outs = NULL;
        *after = NULL;
        *rembuf = NULL;
        *binds = NULL;
        *qnode = NULL;
        query = NULL;
    }
    if (query)
        CMLogDebug("%s.%s - %s", dbid, sqlid, CMCall(query, GetCString));
    return query;
}

CMDBM_STATIC CMBool CMDBM_SessionExecAfters(
        CMDBM_Session *sess, const char *dbid, CMUTIL_JsonObject *params,
        CMUTIL_List *after, CMUTIL_List *rembuf)
{
    CMBool res = CMFalse;
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
            CMDBM_SessionClearBinds(binds);
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
    if (binds) {
        CMDBM_SessionClearBinds(binds);
        CMUTIL_JsonDestroy(binds);
    }
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
    CMUTIL_XmlNode *qnode = NULL;\
    query = CMDBM_SessionGetQuery(sess, dbid, sqlid, params, &binds,\
                    &outs, &after, &rembuf, &qnode);\
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
    CMUTIL_XmlNode *qnode = NULL;
    CMUTIL_String *query = CMDBM_SessionGetQuery(
                sess, dbid, sqlid, params, &binds, &outs, &after, &rembuf,
                &qnode);
    if (query) {
        res = conn->GetList(conn, query, binds, outs);
        if (res != NULL) {
            if (!CMDBM_SessionExecAfters(sess, dbid, params, after, rembuf)) {
                CMLogErrorS("selectKey part of %s.%s execution failed.",
                            dbid, sqlid);
                CMDBM_SessionItemDestroyerJson(res);
                res = NULL;
            } else if (CMDBM_SessionIsValueResult(qnode)) {
                // resultType='value': each row becomes its first column.
                res = CMDBM_SessionToValueList(res);
            }
        } else {
            CMLogErrorS("%s.%s query execution failed. -> %s",
                        dbid, sqlid, CMCall(query, GetCString));
        }
        CMDBM_SessionCleanUp(isess, dbid, query, binds, outs, after, rembuf);
    }
    return res;
}

CMDBM_STATIC CMBool CMDBM_SessionForEachRow(
        CMDBM_Session *sess, const char *dbid, const char *sqlid,
        CMUTIL_JsonObject *params, void *udata,
        CMBool (*rowcb)(CMUTIL_JsonObject*, uint32_t, void*))
{
    CMBool res = CMFalse;
    CMDBM_Session_Internal *isess = (CMDBM_Session_Internal*)sess;
    CMDBM_Connection *conn = CMDBM_SessionGetConnection(isess, dbid);
    CMUTIL_JsonObject *outs = NULL;
    CMUTIL_JsonArray *binds = NULL;
    CMUTIL_List *after = NULL, *rembuf = NULL;
    CMUTIL_XmlNode *qnode = NULL;
    CMUTIL_String *query = CMDBM_SessionGetQuery(
                sess, dbid, sqlid, params, &binds, &outs, &after, &rembuf,
                &qnode);
    if (query) {
        uint32_t fetchsize = CMDBM_SessionFetchSize(qnode);
        CMDBM_Cursor *csr = NULL;
        if (fetchsize > 0 && CMCall(after, GetSize) > 0) {
            // a streaming cursor may keep the connection busy, which would
            // block the selectKey statements running right after it.
            CMLogWarnS("fetchSize of %s.%s is ignored: the statement has "
                       "a selectKey to be executed after it.", dbid, sqlid);
            fetchsize = 0;
        }
        if (CMDBM_SessionIsValueResult(qnode))
            CMLogWarnS("resultType='value' cannot be applied to row "
                       "iteration of %s.%s. ignored.", dbid, sqlid);
        csr = conn->OpenCursor(conn, query, binds, outs, fetchsize);
        if (csr != NULL) {
            if (!CMDBM_SessionExecAfters(sess, dbid, params, after, rembuf)) {
                CMLogErrorS("selectKey part of %s.%s execution failed.",
                            dbid, sqlid);
            } else {
                uint32_t idx = 0;
                CMUTIL_JsonObject *row = NULL;
                CMBool cont = CMTrue;
                while (cont && ((row = CMCall(csr, GetNext)) != NULL)) {
                    cont = rowcb(row, idx++, udata);
                    CMUTIL_JsonDestroy(row);
                }
                res = CMTrue;
            }
            CMCall(csr, Close);
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

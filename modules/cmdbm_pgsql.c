
#include "../src/functions.h"

#ifdef CMDBM_PGSQL

#include <libpq-fe.h>

CMUTIL_LogDefine("cmdbm.module.pgsql")

CMDBM_STATIC void CMDBM_PgSQL_LibraryInit()
{
	// does nothing
}

CMDBM_STATIC void CMDBM_PgSQL_LibraryClear()
{
	// does nothing
}

CMDBM_STATIC const char *CMDBM_PgSQL_GetDBMSKey()
{
	return "PGSQL";
}

typedef struct CMDBM_PgSQLCtx {
	char *prcs;
} CMDBM_PgSQLCtx;

typedef struct CMDBM_PgSQLConn {
    PGconn *conn;
    CMBool autocommit;
    int dummy_padder;
} CMDBM_PgSQLConn;

CMDBM_STATIC void *CMDBM_PgSQL_Initialize(
		const char *dbcs, const char *prcs)
{
	CMDBM_PgSQLCtx *res = CMAlloc(sizeof(CMDBM_PgSQLCtx));
	memset(res, 0x0, sizeof(CMDBM_PgSQLCtx));
	res->prcs = CMStrdup(prcs);
    CMUTIL_UNUSED(dbcs);
	return res;
}

CMDBM_STATIC void CMDBM_PgSQL_CleanUp(void *initres)
{
	CMDBM_PgSQLCtx *ctx = (CMDBM_PgSQLCtx*)initres;
	if (ctx) {
		if (ctx->prcs) CMFree(ctx->prcs);
		CMFree(ctx);
	}
}

CMDBM_STATIC char *CMDBM_PgSQL_GetBindString(
        void *initres, uint32_t index, char *buffer, CMJsonValueType vtype)
{
    const char *typestr = NULL;
    CMUTIL_UNUSED(initres);
    switch (vtype) {
    case CMJsonValueLong:
        typestr = "int8";
        break;
    case CMJsonValueDouble:
        typestr = "float8";
        break;
    case CMJsonValueBoolean:
        typestr = "bool";
        break;
    default:
        typestr = "varchar";
        break;
    }
    sprintf(buffer, "$%d::%s", (index+1), typestr);
	return buffer;
}

CMDBM_STATIC const char *CMDBM_PgSQL_GetTestQuery()
{
	return "SELECT 1";
}

CMDBM_STATIC void CMDBM_PgSQL_CloseConnection(
        void *initres, void *connection)
{
    CMDBM_PgSQLConn *sess = (CMDBM_PgSQLConn*)connection;
    if (sess) {
        PQfinish(sess->conn);
        CMFree(sess);
    }
    CMUTIL_UNUSED(initres);
}

#define CMDBM_PGSQL_MAX_PAIRS   128

CMDBM_STATIC void *CMDBM_PgSQL_OpenConnection(
		void *initres, CMUTIL_JsonObject *params)
{
    size_t i, minsz;
    // +2: one slot for client_encoding, one for the NULL terminator
    const char *key[CMDBM_PGSQL_MAX_PAIRS+2], *value[CMDBM_PGSQL_MAX_PAIRS+2];
    CMUTIL_StringArray *keys = CMCall(params, GetKeys);
    PGconn *conn = NULL;
    CMDBM_PgSQLConn *res = NULL;
    CMDBM_PgSQLCtx *ires = (CMDBM_PgSQLCtx*)initres;
    minsz = CMCall(keys, GetSize);
    if (minsz > CMDBM_PGSQL_MAX_PAIRS) {
        CMLogWarn("Too many PgSQL connection parameters. "
                  "Max pair count is %d, rest of parameters are ignored.",
                  CMDBM_PGSQL_MAX_PAIRS);
        minsz = CMDBM_PGSQL_MAX_PAIRS;
    }
    for (i=0; i<minsz; i++) {
        key[i] = CMCall(keys, GetCString, (uint32_t)i);
        value[i] = CMCall(params, GetCString, key[i]);
	}
    key[i] = "client_encoding";
    value[i++] = ires->prcs;
	key[i] = value[i] = NULL;

    conn = PQconnectdbParams(key, value, 0);
    if (conn == NULL) {
        CMUTIL_String *sbuf = CMUTIL_StringCreate();
        CMCall(((CMUTIL_Json*)params), ToString, sbuf, CMTrue);
        CMLogError("cannot connect to PgSQL database with parameters: %s",
                   CMCall(sbuf, GetCString));
        CMCall(sbuf, Destroy);
    }

    if (conn && PQstatus(conn) != CONNECTION_OK) {
        CMUTIL_String *sbuf = CMUTIL_StringCreate();
        CMCall(((CMUTIL_Json*)params), ToString, sbuf, CMTrue);
        CMLogError("cannot connect to PgSQL database with parameters: "
                   "%s\ndatabase message: %s" ,CMCall(sbuf, GetCString),
                   PQerrorMessage(conn));
        CMCall(sbuf, Destroy);
        PQfinish(conn);
        conn = NULL;
    }

    if (conn) {
        res = CMAlloc(sizeof(CMDBM_PgSQLConn));
        memset(res, 0x0, sizeof(CMDBM_PgSQLConn));
        res->conn = conn;
        res->autocommit = CMTrue;
    }

    CMCall(keys, Destroy);
    CMUTIL_UNUSED(initres);
    return res;
}

#define CMDBM_PgSQLCheck(c,l,f,...) do{   \
    CMBool rv = CMTrue; \
    PGresult *pr = NULL; \
    pr = (f)(__VA_ARGS__);  \
    if (PQresultStatus(pr) != PGRES_COMMAND_OK) {   \
        CMLogError("%s failed: %s", #f, PQerrorMessage(c)); \
        rv = CMFalse;   \
    }   \
    PQclear(pr);    \
    if (!rv) goto l;    \
} while(0)

#define CMDBM_PgSQLResult(c,l,r,f,...) do{   \
    r = (f)(__VA_ARGS__);  \
    if (PQresultStatus(r) != PGRES_COMMAND_OK) {   \
        CMLogError("%s failed: %s", #f, PQerrorMessage(c)); \
        goto l;   \
    }   \
} while(0)

CMDBM_STATIC CMBool CMDBM_PgSQL_StartTransaction(
        void *initres, void *connection)
{
    CMDBM_PgSQLConn *sess = (CMDBM_PgSQLConn*)connection;
    CMUTIL_UNUSED(initres);
    CMDBM_PgSQLCheck(sess->conn, FAILED, PQexec, sess->conn, "BEGIN");
    sess->autocommit = CMFalse;
    return CMTrue;
FAILED:
    return CMFalse;
}

CMDBM_STATIC void CMDBM_PgSQL_EndTransaction(
        void *initres, void *connection)
{
    CMDBM_PgSQLConn *sess = (CMDBM_PgSQLConn*)connection;
    sess->autocommit = CMTrue;
    CMUTIL_UNUSED(initres);
}

CMDBM_STATIC CMBool CMDBM_PgSQL_CommitTransaction(
        void *initres, void *connection)
{
    CMDBM_PgSQLConn *conn = (CMDBM_PgSQLConn*)connection;
    CMDBM_PgSQLCheck(conn->conn, FAILED, PQexec, conn->conn, "COMMIT");
    CMUTIL_UNUSED(initres);
    return CMTrue;
FAILED:
    return CMFalse;
}

CMDBM_STATIC void CMDBM_PgSQL_RollbackTransaction(
        void *initres, void *connection)
{
    CMDBM_PgSQLConn *sess = (CMDBM_PgSQLConn*)connection;
    CMDBM_PgSQLCheck(sess->conn, FAILED, PQexec, sess->conn, "ROLLBACK");
    CMUTIL_UNUSED(initres);
FAILED:;
}

// PostgreSQL built-in type OIDs (from server catalog pg_type.h,
// stable across all PostgreSQL versions).
#define CMDBM_PGSQL_BOOLOID     16
#define CMDBM_PGSQL_INT8OID     20
#define CMDBM_PGSQL_INT2OID     21
#define CMDBM_PGSQL_INT4OID     23
#define CMDBM_PGSQL_OIDOID      26
#define CMDBM_PGSQL_FLOAT4OID   700
#define CMDBM_PGSQL_FLOAT8OID   701
#define CMDBM_PGSQL_NUMERICOID  1700

CMDBM_STATIC PGresult *CMDBM_PgSQL_ExecuteBase(
        CMDBM_PgSQLConn *sess, CMUTIL_String *query,
        CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    PGresult *pres = NULL;
    ExecStatusType status;
    int nparams = 0;
    const char **values = NULL;
    // PostgreSQL has no out-binding: procedure results come back
    // as an ordinary result set.
    CMUTIL_UNUSED(outs);

    if (binds) nparams = (int)CMCall(binds, GetSize);
    if (nparams > 0) {
        int i;
        values = CMAlloc(sizeof(char*) * (size_t)nparams);
        for (i=0; i<nparams; i++) {
            CMUTIL_Json *json = CMCall(binds, Get, (uint32_t)i);
            CMUTIL_JsonValue *jval;
            if (CMCall(json, GetType) != CMJsonTypeValue) {
                CMLogError("binding variable is not value type JSON.");
                CMFree(values);
                return NULL;
            }
            jval = (CMUTIL_JsonValue*)json;
            switch (CMCall(jval, GetValueType)) {
            case CMJsonValueNull:
                values[i] = NULL;
                break;
            case CMJsonValueBoolean:
                values[i] = CMCall(jval, GetBoolean)? "true":"false";
                break;
            default:
                values[i] = CMCall(jval, GetCString);
                break;
            }
        }
    }

    pres = PQexecParams(sess->conn, CMCall(query, GetCString),
                        nparams, NULL, values, NULL, NULL, 0);
    if (values) CMFree(values);
    status = PQresultStatus(pres);
    if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK) {
        CMLogError("query execution failed: %s", PQerrorMessage(sess->conn));
        PQclear(pres);
        pres = NULL;
    }
    return pres;
}

CMDBM_STATIC void CMDBM_PgSQL_FetchRow(
        PGresult *pres, int rowidx, CMUTIL_JsonObject *row)
{
    int i, nfields = PQnfields(pres);
    for (i=0; i<nfields; i++) {
        const char *name = PQfname(pres, i);
        if (PQgetisnull(pres, rowidx, i)) {
            CMCall(row, PutNull, name);
        } else {
            const char *val = PQgetvalue(pres, rowidx, i);
            switch (PQftype(pres, i)) {
            case CMDBM_PGSQL_BOOLOID:
                CMCall(row, PutBoolean, name,
                       (*val == 't')? CMTrue:CMFalse);
                break;
            case CMDBM_PGSQL_INT2OID:
            case CMDBM_PGSQL_INT4OID:
            case CMDBM_PGSQL_INT8OID:
            case CMDBM_PGSQL_OIDOID:
                CMCall(row, PutLong, name, (int64_t)strtoll(val, NULL, 10));
                break;
            case CMDBM_PGSQL_FLOAT4OID:
            case CMDBM_PGSQL_FLOAT8OID:
            case CMDBM_PGSQL_NUMERICOID:
                CMCall(row, PutDouble, name, strtod(val, NULL));
                break;
            default:
                CMCall(row, PutString, name, val);
                break;
            }
        }
    }
}

CMDBM_STATIC CMUTIL_JsonObject *CMDBM_PgSQL_GetRow(
        void *initres, void *connection,
        CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    CMDBM_PgSQLConn *sess = (CMDBM_PgSQLConn*)connection;
    CMUTIL_JsonObject *res = NULL;
    PGresult *pres = CMDBM_PgSQL_ExecuteBase(sess, query, binds, outs);
    CMUTIL_UNUSED(initres);
    if (pres == NULL)
        return NULL;
    if (PQresultStatus(pres) == PGRES_TUPLES_OK && PQntuples(pres) > 0) {
        res = CMUTIL_JsonObjectCreate();
        CMDBM_PgSQL_FetchRow(pres, 0, res);
    } else {
        CMLogError("query did not return any row.");
    }
    PQclear(pres);
    return res;
}

CMDBM_STATIC CMUTIL_JsonValue *CMDBM_PgSQL_GetOneValue(
        void *initres, void *connection,
        CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    CMUTIL_JsonValue *res = NULL;
    CMUTIL_JsonObject *row =
            CMDBM_PgSQL_GetRow(initres, connection, query, binds, outs);
    if (row) {
        CMUTIL_StringArray *keys = CMCall(row, GetKeys);
        if (CMCall(keys, GetSize) > 0) {
            const char *key = CMCall(keys, GetCString, 0);
            res = (CMUTIL_JsonValue*)CMCall(row, Remove, key);
        } else {
            CMLogError("row does not contain any fields.");
        }
        CMCall(keys, Destroy);
        CMUTIL_JsonDestroy(row);
    }
    return res;
}

CMDBM_STATIC CMUTIL_JsonArray *CMDBM_PgSQL_GetList(
        void *initres, void *connection,
        CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    CMDBM_PgSQLConn *sess = (CMDBM_PgSQLConn*)connection;
    CMUTIL_JsonArray *res = NULL;
    PGresult *pres = CMDBM_PgSQL_ExecuteBase(sess, query, binds, outs);
    CMUTIL_UNUSED(initres);
    if (pres == NULL)
        return NULL;
    res = CMUTIL_JsonArrayCreate();
    if (PQresultStatus(pres) == PGRES_TUPLES_OK) {
        int i, ntuples = PQntuples(pres);
        for (i=0; i<ntuples; i++) {
            CMUTIL_JsonObject *row = CMUTIL_JsonObjectCreate();
            CMDBM_PgSQL_FetchRow(pres, i, row);
            CMCall(res, Add, (CMUTIL_Json*)row);
        }
    }
    PQclear(pres);
    return res;
}

CMDBM_STATIC int CMDBM_PgSQL_Execute(
        void *initres, void *connection,
        CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    CMDBM_PgSQLConn *sess = (CMDBM_PgSQLConn*)connection;
    PGresult *pres = CMDBM_PgSQL_ExecuteBase(sess, query, binds, outs);
    int res = -1;
    CMUTIL_UNUSED(initres);
    if (pres) {
        const char *afr = PQcmdTuples(pres);
        res = (afr && *afr)? (int)strtol(afr, NULL, 10) : 0;
        PQclear(pres);
    }
    return res;
}

typedef struct CMDBM_PgSQL_Cursor {
    PGresult    *pres;
    int         ntuples;
    int         currow;
} CMDBM_PgSQL_Cursor;

CMDBM_STATIC void *CMDBM_PgSQL_OpenCursor(
        void *initres, void *connection,
        CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    CMDBM_PgSQLConn *sess = (CMDBM_PgSQLConn*)connection;
    PGresult *pres = CMDBM_PgSQL_ExecuteBase(sess, query, binds, outs);
    CMDBM_PgSQL_Cursor *res = NULL;
    CMUTIL_UNUSED(initres);
    if (pres == NULL)
        return NULL;
    if (PQresultStatus(pres) != PGRES_TUPLES_OK) {
        CMLogError("cursor query did not return a result set.");
        PQclear(pres);
        return NULL;
    }
    res = CMAlloc(sizeof(CMDBM_PgSQL_Cursor));
    memset(res, 0x0, sizeof(CMDBM_PgSQL_Cursor));
    res->pres = pres;
    res->ntuples = PQntuples(pres);
    return res;
}

CMDBM_STATIC void CMDBM_PgSQL_CloseCursor(void *cursor)
{
    CMDBM_PgSQL_Cursor *csr = (CMDBM_PgSQL_Cursor*)cursor;
    if (csr) {
        if (csr->pres) PQclear(csr->pres);
        CMFree(csr);
    }
}

CMDBM_STATIC CMUTIL_JsonObject *CMDBM_PgSQL_CursorNextRow(void *cursor)
{
    CMDBM_PgSQL_Cursor *csr = (CMDBM_PgSQL_Cursor*)cursor;
    if (csr && csr->currow < csr->ntuples) {
        CMUTIL_JsonObject *row = CMUTIL_JsonObjectCreate();
        CMDBM_PgSQL_FetchRow(csr->pres, csr->currow++, row);
        return row;
    }
    return NULL;
}

CMDBM_ModuleInterface g_cmdbm_pgsql_interface = {
    CMDBM_PgSQL_LibraryInit,
    CMDBM_PgSQL_LibraryClear,
    CMDBM_PgSQL_GetDBMSKey,
    CMDBM_PgSQL_Initialize,
    CMDBM_PgSQL_CleanUp,
    CMDBM_PgSQL_GetBindString,
    CMDBM_PgSQL_GetTestQuery,
    CMDBM_PgSQL_OpenConnection,
    CMDBM_PgSQL_CloseConnection,
    CMDBM_PgSQL_StartTransaction,
    CMDBM_PgSQL_EndTransaction,
    CMDBM_PgSQL_CommitTransaction,
    CMDBM_PgSQL_RollbackTransaction,
    CMDBM_PgSQL_GetOneValue,
    CMDBM_PgSQL_GetRow,
    CMDBM_PgSQL_GetList,
    CMDBM_PgSQL_Execute,
    CMDBM_PgSQL_OpenCursor,
    CMDBM_PgSQL_CloseCursor,
    CMDBM_PgSQL_CursorNextRow
};

#endif

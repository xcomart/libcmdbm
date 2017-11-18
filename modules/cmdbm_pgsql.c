
#include "functions.h"

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
		void *initres, int index, char *buffer)
{
    CMUTIL_UNUSED(initres);
	sprintf(buffer, "$%d", (index+1));
	return buffer;
}

CMDBM_STATIC const char *CMDBM_PgSQL_GetTestQuery()
{
	return "SELECT 1";
}

CMDBM_STATIC void CMDBM_PgSQL_CloseConnection(
        void *initres, void *connection)
{
    CMDBM_PgSQLConn *conn = (CMDBM_PgSQLConn*)connection;
    if (conn) {
        PQfinish(conn->conn);
        CMFree(conn);
    }
    CMUTIL_UNUSED(initres);
}

#define CMDBM_PGSQL_MAX_PAIRS   128

CMDBM_STATIC void *CMDBM_PgSQL_OpenConnection(
		void *initres, CMUTIL_JsonObject *params)
{
    size_t i, minsz;
    const char *key[CMDBM_PGSQL_MAX_PAIRS+1], *value[CMDBM_PGSQL_MAX_PAIRS+1];
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

    if (PQstatus(conn) != CONNECTION_OK) {
        CMUTIL_String *sbuf = CMUTIL_StringCreate();
        CMCall(((CMUTIL_Json*)params), ToString, sbuf, CMTrue);
        CMLogError("cannot connect to PgSQL database with parameters: "
                   "%s\ndatabase message: %s" ,CMCall(sbuf, GetCString),
                   PQerrorMessage(conn));
        CMCall(sbuf, Destroy);
        CMDBM_PgSQL_CloseConnection(initres, conn);
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

CMDBM_STATIC CMBool CMDBM_PgSQL_StartTransaction(
        void *initres, void *connection)
{
    CMDBM_PgSQLConn *conn = (CMDBM_PgSQLConn*)connection;
    CMUTIL_UNUSED(initres);
    conn->autocommit = CMFalse;
    CMDBM_PgSQLCheck(conn->conn, FAILED, PQexec, conn->conn, "BEGIN");
    return CMTrue;
FAILED:
    return CMFalse;
}

CMDBM_STATIC void CMDBM_PgSQL_EndTransaction(
        void *initres, void *connection)
{
    CMDBM_PgSQLConn *conn = (CMDBM_PgSQLConn*)connection;
    conn->autocommit = CMTrue;
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
    CMDBM_PgSQLConn *conn = (CMDBM_PgSQLConn*)connection;
    CMDBM_PgSQLCheck(conn->conn, FAILED, PQexec, conn->conn, "ROLLBACK");
    CMUTIL_UNUSED(initres);
FAILED:;
}

#endif

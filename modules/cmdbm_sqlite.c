
#include "../src/functions.h"

#ifdef CMDBM_SQLITE

#include <sqlite3.h>

CMUTIL_LogDefine("cmdbm.module.sqlite")

#define SQLITE_LOGERROR(sess,...) do {\
    char buf[4096]; snprintf(buf,sizeof(buf),##__VA_ARGS__);\
    if (sess && sess->conn) {\
        CMLogError("%s: %s", buf, sqlite3_errmsg(sess->conn));\
    } else CMLogError("%s", buf);\
} while(0)

// how long a connection waits for a lock held by another connection.
#define CMDBM_SQLITE_BUSY_TIMEOUT   5000

typedef struct CMDBM_SQLiteCtx {
    char *prcs;
} CMDBM_SQLiteCtx;

typedef struct CMDBM_SQLiteConn {
    sqlite3 *conn;
} CMDBM_SQLiteConn;

CMDBM_STATIC void CMDBM_SQLite_LibraryInit()
{
    if (sqlite3_initialize() != SQLITE_OK)
        CMLogError("sqlite3_initialize() failed.");
}

CMDBM_STATIC void CMDBM_SQLite_LibraryClear()
{
    // sqlite3_shutdown() is deliberately not called: the application may
    // use SQLite on its own, and shutting the library down would pull it
    // out from under it. SQLite releases its global state at process exit.
}

CMDBM_STATIC const char *CMDBM_SQLite_GetDBMSKey()
{
    return "SQLITE";
}

CMDBM_STATIC void *CMDBM_SQLite_Initialize(
        const char *dbcs, const char *prcs)
{
    CMDBM_SQLiteCtx *res = CMAlloc(sizeof(CMDBM_SQLiteCtx));
    memset(res, 0x0, sizeof(CMDBM_SQLiteCtx));
    res->prcs = CMStrdup(prcs);
    // SQLite stores text as UTF-8, there is nothing to negotiate.
    if (dbcs && strcasecmp(dbcs, "utf-8") != 0 &&
            strcasecmp(dbcs, "utf8") != 0)
        CMLogWarn("SQLite database character set is always UTF-8, "
                  "configured '%s' is ignored.", dbcs);
    return res;
}

CMDBM_STATIC void CMDBM_SQLite_CleanUp(void *initres)
{
    CMDBM_SQLiteCtx *ctx = (CMDBM_SQLiteCtx*)initres;
    if (ctx) {
        if (ctx->prcs) CMFree(ctx->prcs);
        CMFree(ctx);
    }
}

CMDBM_STATIC char *CMDBM_SQLite_GetBindString(
        void *initres, uint32_t index, char *buffer, CMJsonValueType vtype)
{
    CMUTIL_UNUSED(initres, vtype);
    // SQLite parameter indexes are 1 based.
    sprintf(buffer, "?%u", (index+1));
    return buffer;
}

CMDBM_STATIC const char *CMDBM_SQLite_GetTestQuery()
{
    return "select 1";
}

CMDBM_STATIC void *CMDBM_SQLite_OpenConnection(
        void *initres, CMUTIL_JsonObject *params)
{
    CMDBM_SQLiteConn *res = NULL;
    sqlite3 *conn = NULL;
    int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
    CMUTIL_JsonValue *file =
            (CMUTIL_JsonValue*)CMCall(params, Get, "file");
    const char *sfile = NULL;
    CMUTIL_UNUSED(initres);

    if (file == NULL) {
        CMLogError("SQLite connection requires 'file' "
                   "(a path or ':memory:')");
        return NULL;
    }
    sfile = CMCall(file, GetCString);

    // pooled connections are handed to whichever thread borrows them, so
    // they are serialized unless the configuration opts out.
    if (CMCall(params, Get, "serialize") &&
            !CMCall(params, GetBoolean, "serialize"))
        flags |= SQLITE_OPEN_NOMUTEX;
    else
        flags |= SQLITE_OPEN_FULLMUTEX;

    if (sqlite3_open_v2(sfile, &conn, flags, NULL) != SQLITE_OK) {
        CMLogError("cannot open SQLite database '%s': %s", sfile,
                   conn? sqlite3_errmsg(conn):"out of memory");
        if (conn) sqlite3_close(conn);
        return NULL;
    }
    sqlite3_busy_timeout(conn, CMDBM_SQLITE_BUSY_TIMEOUT);

    res = CMAlloc(sizeof(CMDBM_SQLiteConn));
    memset(res, 0x0, sizeof(CMDBM_SQLiteConn));
    res->conn = conn;
    CMLogTrace("SQLite connection created.");
    return res;
}

CMDBM_STATIC void CMDBM_SQLite_CloseConnection(
        void *initres, void *connection)
{
    CMDBM_SQLiteConn *sess = (CMDBM_SQLiteConn*)connection;
    CMUTIL_UNUSED(initres);
    if (sess) {
        if (sess->conn && sqlite3_close(sess->conn) != SQLITE_OK)
            SQLITE_LOGERROR(sess, "cannot close SQLite database");
        CMLogTrace("SQLite connection closed.");
        CMFree(sess);
    }
}

CMDBM_STATIC CMBool CMDBM_SQLite_RunCommand(
        CMDBM_SQLiteConn *sess, const char *sql)
{
    char *errmsg = NULL;
    if (sqlite3_exec(sess->conn, sql, NULL, NULL, &errmsg) != SQLITE_OK) {
        CMLogError("%s failed: %s", sql, errmsg? errmsg:"unknown error");
        if (errmsg) sqlite3_free(errmsg);
        return CMFalse;
    }
    return CMTrue;
}

CMDBM_STATIC CMBool CMDBM_SQLite_StartTransaction(
        void *initres, void *connection)
{
    CMDBM_SQLiteConn *sess = (CMDBM_SQLiteConn*)connection;
    CMUTIL_UNUSED(initres);
    return CMDBM_SQLite_RunCommand(sess, "BEGIN");
}

CMDBM_STATIC void CMDBM_SQLite_EndTransaction(
        void *initres, void *connection)
{
    CMDBM_SQLiteConn *sess = (CMDBM_SQLiteConn*)connection;
    CMUTIL_UNUSED(initres);
    // an unfinished transaction must not survive on a pooled connection.
    if (!sqlite3_get_autocommit(sess->conn)) {
        CMLogWarn("transaction ended without commit. rolling back.");
        CMDBM_SQLite_RunCommand(sess, "ROLLBACK");
    }
}

CMDBM_STATIC CMBool CMDBM_SQLite_CommitTransaction(
        void *initres, void *connection)
{
    CMDBM_SQLiteConn *sess = (CMDBM_SQLiteConn*)connection;
    CMUTIL_UNUSED(initres);
    // committing twice is not an error: SQLite ends the transaction with
    // the first commit and runs in autocommit mode afterwards.
    if (sqlite3_get_autocommit(sess->conn))
        return CMTrue;
    return CMDBM_SQLite_RunCommand(sess, "COMMIT");
}

CMDBM_STATIC void CMDBM_SQLite_RollbackTransaction(
        void *initres, void *connection)
{
    CMDBM_SQLiteConn *sess = (CMDBM_SQLiteConn*)connection;
    CMUTIL_UNUSED(initres);
    if (!sqlite3_get_autocommit(sess->conn))
        CMDBM_SQLite_RunCommand(sess, "ROLLBACK");
}

// binds the values built by the sql builder. SQLite parameter indexes
// are 1 based, bind index 0 is parameter 1.
CMDBM_STATIC CMBool CMDBM_SQLite_BindParams(
        CMDBM_SQLiteConn *sess, sqlite3_stmt *stmt, CMUTIL_JsonArray *binds)
{
    uint32_t i, cnt = 0;
    if (binds) cnt = (uint32_t)CMCall(binds, GetSize);
    for (i=0; i<cnt; i++) {
        CMUTIL_Json *json = CMCall(binds, Get, i);
        CMUTIL_JsonValue *jval;
        int idx = (int)i + 1;
        int rv;
        if (CMCall(json, GetType) != CMJsonTypeValue) {
            CMLogError("binding variable is not value type JSON.");
            return CMFalse;
        }
        jval = (CMUTIL_JsonValue*)json;
        switch (CMCall(jval, GetValueType)) {
        case CMJsonValueNull:
            rv = sqlite3_bind_null(stmt, idx);
            break;
        case CMJsonValueLong: {
            int64_t lval = CMCall(jval, GetLong);
            rv = sqlite3_bind_int64(stmt, idx, (sqlite3_int64)lval);
            break;
        }
        case CMJsonValueDouble: {
            double dval = CMCall(jval, GetDouble);
            rv = sqlite3_bind_double(stmt, idx, dval);
            break;
        }
        case CMJsonValueBoolean: {
            CMBool bval = CMCall(jval, GetBoolean);
            rv = sqlite3_bind_int(stmt, idx, bval? 1:0);
            break;
        }
        default: {
            const char *sval = CMCall(jval, GetCString);
            // SQLITE_TRANSIENT: the bind value belongs to the caller and
            // may be gone before the statement is stepped.
            rv = sqlite3_bind_text(stmt, idx, sval, -1, SQLITE_TRANSIENT);
            break;
        }
        }
        if (rv != SQLITE_OK) {
            SQLITE_LOGERROR(sess, "cannot bind parameter %d", idx);
            return CMFalse;
        }
    }
    return CMTrue;
}

CMDBM_STATIC sqlite3_stmt *CMDBM_SQLite_ExecuteBase(
        CMDBM_SQLiteConn *sess, CMUTIL_String *query,
        CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    sqlite3_stmt *stmt = NULL;
    const char *sql = CMCall(query, GetCString);
    // SQLite has no out binding: results always come back as a result set.
    CMUTIL_UNUSED(outs);

    if (sqlite3_prepare_v2(sess->conn, sql, -1, &stmt, NULL) != SQLITE_OK) {
        SQLITE_LOGERROR(sess, "prepare statement failed. -> %s", sql);
        return NULL;
    }
    if (!CMDBM_SQLite_BindParams(sess, stmt, binds)) {
        sqlite3_finalize(stmt);
        return NULL;
    }
    return stmt;
}

CMDBM_STATIC void CMDBM_SQLite_FetchRow(
        sqlite3_stmt *stmt, CMUTIL_JsonObject *row)
{
    int i, ncols = sqlite3_column_count(stmt);
    for (i=0; i<ncols; i++) {
        const char *name = sqlite3_column_name(stmt, i);
        switch (sqlite3_column_type(stmt, i)) {
        case SQLITE_NULL:
            CMCall(row, PutNull, name);
            break;
        case SQLITE_INTEGER:
            CMCall(row, PutLong, name,
                   (int64_t)sqlite3_column_int64(stmt, i));
            break;
        case SQLITE_FLOAT:
            CMCall(row, PutDouble, name, sqlite3_column_double(stmt, i));
            break;
        default: {
            // text and blob: blobs are handed over in their text form,
            // binary content is not representable in a JSON string.
            const char *val = (const char*)sqlite3_column_text(stmt, i);
            CMCall(row, PutString, name, val? val:"");
            break;
        }
        }
    }
}

// steps a statement which returns rows. returns SQLITE_ROW, SQLITE_DONE
// or the error code.
CMDBM_STATIC int CMDBM_SQLite_Step(
        CMDBM_SQLiteConn *sess, sqlite3_stmt *stmt)
{
    int rv = sqlite3_step(stmt);
    if (rv != SQLITE_ROW && rv != SQLITE_DONE)
        SQLITE_LOGERROR(sess, "query execution failed");
    return rv;
}

CMDBM_STATIC CMUTIL_JsonObject *CMDBM_SQLite_GetRow(
        void *initres, void *connection,
        CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    CMDBM_SQLiteConn *sess = (CMDBM_SQLiteConn*)connection;
    CMUTIL_JsonObject *res = NULL;
    sqlite3_stmt *stmt = CMDBM_SQLite_ExecuteBase(sess, query, binds, outs);
    CMUTIL_UNUSED(initres);
    if (stmt == NULL)
        return NULL;
    if (CMDBM_SQLite_Step(sess, stmt) == SQLITE_ROW) {
        res = CMUTIL_JsonObjectCreate();
        CMDBM_SQLite_FetchRow(stmt, res);
    } else {
        CMLogError("query did not return any row.");
    }
    sqlite3_finalize(stmt);
    return res;
}

CMDBM_STATIC CMUTIL_JsonValue *CMDBM_SQLite_GetOneValue(
        void *initres, void *connection,
        CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    CMUTIL_JsonValue *res = NULL;
    CMUTIL_JsonObject *row =
            CMDBM_SQLite_GetRow(initres, connection, query, binds, outs);
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

CMDBM_STATIC CMUTIL_JsonArray *CMDBM_SQLite_GetList(
        void *initres, void *connection,
        CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    CMDBM_SQLiteConn *sess = (CMDBM_SQLiteConn*)connection;
    CMUTIL_JsonArray *res = NULL;
    int rv;
    sqlite3_stmt *stmt = CMDBM_SQLite_ExecuteBase(sess, query, binds, outs);
    CMUTIL_UNUSED(initres);
    if (stmt == NULL)
        return NULL;

    res = CMUTIL_JsonArrayCreate();
    while ((rv = CMDBM_SQLite_Step(sess, stmt)) == SQLITE_ROW) {
        CMUTIL_JsonObject *row = CMUTIL_JsonObjectCreate();
        CMDBM_SQLite_FetchRow(stmt, row);
        CMCall(res, Add, (CMUTIL_Json*)row);
    }
    sqlite3_finalize(stmt);
    if (rv != SQLITE_DONE) {
        CMUTIL_JsonDestroy(res);
        res = NULL;
    }
    return res;
}

CMDBM_STATIC int CMDBM_SQLite_Execute(
        void *initres, void *connection,
        CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    CMDBM_SQLiteConn *sess = (CMDBM_SQLiteConn*)connection;
    int res = -1, rv;
    sqlite3_stmt *stmt = CMDBM_SQLite_ExecuteBase(sess, query, binds, outs);
    CMUTIL_UNUSED(initres);
    if (stmt == NULL)
        return -1;

    // a statement may return rows even when it is executed for its effect
    // (insert ... returning), so step it to the end.
    while ((rv = CMDBM_SQLite_Step(sess, stmt)) == SQLITE_ROW);
    if (rv == SQLITE_DONE)
        res = sqlite3_changes(sess->conn);
    sqlite3_finalize(stmt);
    return res;
}

typedef struct CMDBM_SQLite_Cursor {
    CMDBM_SQLiteConn    *sess;
    sqlite3_stmt        *stmt;
    CMBool              isend;
    int                 dummy_padder;
} CMDBM_SQLite_Cursor;

CMDBM_STATIC void *CMDBM_SQLite_OpenCursor(
        void *initres, void *connection,
        CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs,
        uint32_t fetchsize)
{
    CMDBM_SQLiteConn *sess = (CMDBM_SQLiteConn*)connection;
    CMDBM_SQLite_Cursor *res = NULL;
    sqlite3_stmt *stmt = CMDBM_SQLite_ExecuteBase(sess, query, binds, outs);
    CMUTIL_UNUSED(initres);
    if (stmt == NULL)
        return NULL;
    if (fetchsize > 0) {
        // an embedded engine has no round trip to batch: sqlite3_step
        // produces exactly one row at a time.
        CMLogDebug("fetchSize has no meaning for SQLite. ignored.");
    }
    res = CMAlloc(sizeof(CMDBM_SQLite_Cursor));
    memset(res, 0x0, sizeof(CMDBM_SQLite_Cursor));
    res->sess = sess;
    res->stmt = stmt;
    return res;
}

CMDBM_STATIC void CMDBM_SQLite_CloseCursor(void *cursor)
{
    CMDBM_SQLite_Cursor *csr = (CMDBM_SQLite_Cursor*)cursor;
    if (csr) {
        if (csr->stmt) sqlite3_finalize(csr->stmt);
        CMFree(csr);
    }
}

CMDBM_STATIC CMUTIL_JsonObject *CMDBM_SQLite_CursorNextRow(void *cursor)
{
    CMDBM_SQLite_Cursor *csr = (CMDBM_SQLite_Cursor*)cursor;
    CMUTIL_JsonObject *res = NULL;
    if (csr == NULL || csr->isend)
        return NULL;
    if (CMDBM_SQLite_Step(csr->sess, csr->stmt) == SQLITE_ROW) {
        res = CMUTIL_JsonObjectCreate();
        CMDBM_SQLite_FetchRow(csr->stmt, res);
    } else {
        csr->isend = CMTrue;
    }
    return res;
}

CMDBM_ModuleInterface g_cmdbm_sqlite_interface = {
    CMDBM_SQLite_LibraryInit,
    CMDBM_SQLite_LibraryClear,
    CMDBM_SQLite_GetDBMSKey,
    CMDBM_SQLite_Initialize,
    CMDBM_SQLite_CleanUp,
    CMDBM_SQLite_GetBindString,
    CMDBM_SQLite_GetTestQuery,
    CMDBM_SQLite_OpenConnection,
    CMDBM_SQLite_CloseConnection,
    CMDBM_SQLite_StartTransaction,
    CMDBM_SQLite_EndTransaction,
    CMDBM_SQLite_CommitTransaction,
    CMDBM_SQLite_RollbackTransaction,
    CMDBM_SQLite_GetOneValue,
    CMDBM_SQLite_GetRow,
    CMDBM_SQLite_GetList,
    CMDBM_SQLite_Execute,
    CMDBM_SQLite_OpenCursor,
    CMDBM_SQLite_CloseCursor,
    CMDBM_SQLite_CursorNextRow
};

#endif

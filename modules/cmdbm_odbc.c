
#include "functions.h"

#ifdef CMDBM_ODBC

CMUTIL_LogDefine("cmdbm.module.odbc")

#if defined(MSWIN)
# ifdef UNICODE
#  undef UNICODE
# endif
# include <windows.h>
#endif
#include <sqlext.h>

/*******************************************/
/* Macro to call ODBC functions and        */
/* report an error on failure.             */
/* Takes handle, handle type, and stmt     */
/*******************************************/

#define TRYODBC(h, ht, x)   \
    do {   SQLRETURN rc = x;\
        if (rc != SQL_SUCCESS) { \
            SQLSMALLINT iRec = 0; \
            SQLINTEGER  iError; \
            SQLCHAR     wszMessage[1000]; \
            SQLCHAR     wszState[SQL_SQLSTATE_SIZE+1]; \
            if (rc == SQL_INVALID_HANDLE) { \
                CMLogError("Invalid handle!"); \
                goto FAILED;  \
            } \
            while (SQLGetDiagRec(ht, \
                                 h, \
                                 ++iRec, \
                                 wszState, \
                                 &iError, \
                                 wszMessage, \
                                 (SQLSMALLINT)sizeof(wszMessage), \
                                 (SQLSMALLINT *)NULL) == SQL_SUCCESS) { \
                if (strncmp((char*)wszState, "01004", 5)) { \
                    CMLogError("[%5.5s] %s (%d)", \
                                wszState, wszMessage, iError); \
                    goto FAILED;  \
                } \
            } \
        } \
        if (rc == SQL_ERROR) { \
            CMLogError("Error in " #x); \
            goto FAILED;  \
        }  \
    } while(0)

typedef struct CMDBM_ODBCCtx {
    CMUTIL_String   *prcs;
    CMUTIL_String   *dbcs;
    SQLHENV         env;
} CMDBM_ODBCCtx;

typedef struct CMDBM_ODBCSession {
    CMDBM_ODBCCtx   *ctx;
    SQLHDBC         conn;
} CMDBM_ODBCSession;

CMDBM_STATIC const char *CMDBM_ODBC_GetDBMSKey()
{
    return "ODBC";
}

CMDBM_STATIC void CMDBM_ODBC_CleanUp(
        void *initres)
{
    if (initres) {
        CMDBM_ODBCCtx *ctx = (CMDBM_ODBCCtx*)initres;
        if (ctx->prcs) CMCall(ctx->prcs, Destroy);
        if (ctx->dbcs) CMCall(ctx->dbcs, Destroy);
        if (ctx->env)  SQLFreeHandle(SQL_HANDLE_ENV, ctx->env);
        CMFree(ctx);
    }
}

CMDBM_STATIC void *CMDBM_ODBC_Initialize(const char *dbcs, const char *prcs)
{
    SQLRETURN sr;
    CMDBM_ODBCCtx *res = CMAlloc(sizeof(CMDBM_ODBCCtx));
    memset(res, 0x0, sizeof(CMDBM_ODBCCtx));
    res->prcs = CMUTIL_StringCreateEx(10, prcs);
    res->dbcs = CMUTIL_StringCreateEx(10, dbcs);
    sr = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &res->env);

    if (sr == SQL_ERROR)
        goto FAILED;

    TRYODBC(res->env, SQL_HANDLE_ENV, SQLSetEnvAttr(
                res->env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0));
    return res;
FAILED:
    CMDBM_ODBC_CleanUp(res);
    return NULL;
}

CMDBM_STATIC char *CMDBM_ODBC_GetBindString(
        void *initres, uint32_t index, char *buffer)
{
    CMUTIL_UNUSED(initres, index);
    strcpy(buffer, "?");
    return buffer;
}

CMDBM_STATIC const char *CMDBM_ODBC_GetTestQuery()
{
    return "select 1";
}

CMDBM_STATIC void CMDBM_ODBC_CloseConnection(
        void *initres, void *connection)
{
    CMDBM_ODBCSession *sess = (CMDBM_ODBCSession*)connection;
    if (sess) {
        if (sess->conn)
            SQLFreeHandle(SQL_HANDLE_DBC, sess->conn);
        CMLogTrace("ODBC connection closed.");
        CMFree(sess);
    }
    CMUTIL_UNUSED(initres);
}

CMDBM_STATIC void *CMDBM_ODBC_OpenConnection(
        void *initres, CMUTIL_JsonObject *params)
{
    CMDBM_ODBCSession *sess = NULL;
    CMUTIL_String *dsnstr = NULL;
    CMUTIL_JsonValue *dsn =
            (CMUTIL_JsonValue*)CMCall(params, Get, "dsn");
    CMUTIL_JsonValue *user =
            (CMUTIL_JsonValue*)CMCall(params, Get, "user");
    CMUTIL_JsonValue *pass =
            (CMUTIL_JsonValue*)CMCall(params, Get, "password");

    if (dsn && user && pass) {
        const char *sdsn  = CMCall(dsn , GetCString);
        const char *suser = CMCall(user, GetCString);
        const char *spass = CMCall(pass, GetCString);
        sess = CMAlloc(sizeof(CMDBM_ODBCSession));
        memset(sess, 0x0, sizeof(CMDBM_ODBCSession));
        sess->ctx = (CMDBM_ODBCCtx*)initres;
        TRYODBC(sess->ctx->env, SQL_HANDLE_ENV, SQLAllocHandle(
                    SQL_HANDLE_DBC, sess->ctx->env, &sess->conn));

        dsnstr = CMUTIL_StringCreate();
        CMCall(dsnstr, AddPrint, "DSN=%s;Uid=%s;Pwd=%s;",
                    sdsn, suser, spass);

        TRYODBC(sess->conn, SQL_HANDLE_DBC, SQLDriverConnect(
                    sess->conn, NULL, ((SQLCHAR*)CMCall(dsnstr, GetCString)),
                    SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE));

        CMLogTrace("ODBC connection created.");
    } else {
        CMLogError("ODBC connection requires "
                   "'dsn', 'user', 'password'");
        goto FAILED;
    }

    goto CLEANUP;
FAILED:
    CMDBM_ODBC_CloseConnection(initres, sess);
    sess = NULL;
CLEANUP:
    if (dsnstr) CMCall(dsnstr, Destroy);
    return sess;
}

CMDBM_STATIC CMBool CMDBM_ODBC_StartTransaction(
        void *initres, void *connection)
{
    CMDBM_ODBCSession *sess = (CMDBM_ODBCSession*)connection;
    CMUTIL_UNUSED(initres);

    TRYODBC(sess->conn, SQL_HANDLE_DBC, SQLSetConnectAttr(
                sess->conn, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)0, 0));

    return CMTrue;
FAILED:
    return CMFalse;
}

CMDBM_STATIC void CMDBM_ODBC_EndTransaction(
        void *initres, void *connection)
{
    CMDBM_ODBCSession *sess = (CMDBM_ODBCSession*)connection;
    TRYODBC(sess->conn, SQL_HANDLE_DBC, SQLSetConnectAttr(
                sess->conn, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)1, 0));
FAILED:
    CMUTIL_UNUSED(initres);
}

CMDBM_STATIC CMBool CMDBM_ODBC_CommitTransaction(
        void *initres, void *connection)
{
    CMDBM_ODBCSession *sess = (CMDBM_ODBCSession*)connection;
    CMUTIL_UNUSED(initres);
    TRYODBC(sess->conn, SQL_HANDLE_DBC, SQLEndTran(
                SQL_HANDLE_DBC, sess->conn, SQL_COMMIT));
    return CMTrue;
FAILED:
    return CMFalse;
}

CMDBM_STATIC void CMDBM_ODBC_RollbackTransaction(
        void *initres, void *connection)
{
    CMDBM_ODBCSession *sess = (CMDBM_ODBCSession*)connection;
    CMUTIL_UNUSED(initres);
    TRYODBC(sess->conn, SQL_HANDLE_DBC, SQLEndTran(
                SQL_HANDLE_DBC, sess->conn, SQL_ROLLBACK));
FAILED:
    CMUTIL_UNUSED(initres);
}

typedef struct CMDBM_ODBC_BindField CMDBM_ODBC_BindField;
struct CMDBM_ODBC_BindField {
    int64_t                 longVal;
    double                  doubleVal;
    char                    *strVal;
    char                    *name;
    void                    (*fassign)(
                                    CMDBM_ODBC_BindField*, SQLHSTMT,
                                    CMUTIL_JsonObject*, SQLUSMALLINT);
    SQLLEN                  outLen;
    CMUTIL_JsonValueType    jtype;
    short                   boolVal;
    short                   dummy_padder;
};

CMDBM_ODBC_BindField *CMDBM_ODBC_BindFieldCreate(CMUTIL_JsonValueType jtype) {
    CMDBM_ODBC_BindField *res = CMAlloc(sizeof(CMDBM_ODBC_BindField));
    memset(res, 0x0, sizeof(CMDBM_ODBC_BindField));
    res->jtype = jtype;
    return res;
}

void CMDBM_ODBC_BindFieldDestroy(void *data) {
    CMDBM_ODBC_BindField *field = (CMDBM_ODBC_BindField*)data;
    if (field) {
        if (field->strVal) CMFree(field->strVal);
        if (field->name) CMFree(field->name);
        CMFree(field);
    }
}

CMDBM_STATIC CMBool CMDBM_ODBC_BindLong(
        SQLHSTMT stmt, CMUTIL_JsonValue *jval,
        CMUTIL_Array *bufarr, CMUTIL_Json *out, uint32_t index)
{
    SQLLEN vlen = sizeof(int64_t);
    CMDBM_ODBC_BindField *bfield =
            CMDBM_ODBC_BindFieldCreate(CMUTIL_JsonValueLong);
    SQLSMALLINT inout = out? SQL_PARAM_INPUT:SQL_PARAM_OUTPUT;
    bfield->longVal = CMCall(jval, GetLong);
    TRYODBC(stmt, SQL_HANDLE_STMT, SQLBindParameter(
                stmt, (SQLUSMALLINT)(index+1), inout, SQL_C_SBIGINT,
                SQL_BIGINT, (uint64_t)vlen, 0, &bfield->longVal,
                vlen, &bfield->outLen));
    CMCall(bufarr, Add, bfield);
    return CMTrue;
FAILED:
    CMDBM_ODBC_BindFieldDestroy(bfield);
    return CMFalse;
}

CMDBM_STATIC CMBool CMDBM_ODBC_BindDouble(
        SQLHSTMT stmt, CMUTIL_JsonValue *jval,
        CMUTIL_Array *bufarr, CMUTIL_Json *out, uint32_t index)
{
    SQLLEN vlen = sizeof(double);
    CMDBM_ODBC_BindField *bfield =
            CMDBM_ODBC_BindFieldCreate(CMUTIL_JsonValueDouble);
    SQLSMALLINT inout = out? SQL_PARAM_INPUT:SQL_PARAM_OUTPUT;
    bfield->doubleVal = CMCall(jval, GetDouble);
    TRYODBC(stmt, SQL_HANDLE_STMT, SQLBindParameter(
                stmt, (SQLUSMALLINT)(index+1), inout, SQL_C_DOUBLE,
                SQL_DOUBLE, (uint64_t)vlen, 0, &bfield->doubleVal,
                vlen, &bfield->outLen));
    CMCall(bufarr, Add, bfield);
    return CMTrue;
FAILED:
    CMDBM_ODBC_BindFieldDestroy(bfield);
    return CMFalse;
}

CMDBM_STATIC CMBool CMDBM_ODBC_BindString(
        SQLHSTMT stmt, CMUTIL_JsonValue *jval,
        CMUTIL_Array *bufarr, CMUTIL_Json *out, uint32_t index)
{
    CMUTIL_String *str = CMCall(jval, GetString);
    SQLLEN osize = (SQLLEN)CMCall(str, GetSize);
    CMDBM_ODBC_BindField *bfield =
            CMDBM_ODBC_BindFieldCreate(CMUTIL_JsonValueString);
    SQLSMALLINT inout = out? SQL_PARAM_INPUT:SQL_PARAM_OUTPUT;
    if (out && osize < 4096)
        osize = 4096;
    bfield->strVal = CMAlloc((uint64_t)osize+1);
    memcpy(bfield->strVal, CMCall(str, GetCString), CMCall(str, GetSize)+1);
    TRYODBC(stmt, SQL_HANDLE_STMT, SQLBindParameter(
                stmt, (SQLUSMALLINT)(index+1), inout, SQL_C_CHAR,
                SQL_VARCHAR, (uint64_t)CMCall(str, GetSize), 0, bfield->strVal,
                osize, &bfield->outLen));
    CMCall(bufarr, Add, bfield);
    return CMTrue;
FAILED:
    CMDBM_ODBC_BindFieldDestroy(bfield);
    return CMFalse;
}

CMDBM_STATIC CMBool CMDBM_ODBC_BindBoolean(
        SQLHSTMT stmt, CMUTIL_JsonValue *jval,
        CMUTIL_Array *bufarr, CMUTIL_Json *out, uint32_t index)
{
    SQLLEN vlen = sizeof(short);
    CMDBM_ODBC_BindField *bfield =
            CMDBM_ODBC_BindFieldCreate(CMUTIL_JsonValueBoolean);
    SQLSMALLINT inout = out? SQL_PARAM_INPUT:SQL_PARAM_OUTPUT;
    bfield->boolVal = CMCall(jval, GetBoolean);
    TRYODBC(stmt, SQL_HANDLE_STMT, SQLBindParameter(
                stmt, (SQLUSMALLINT)(index+1), inout, SQL_C_SSHORT,
                SQL_SMALLINT, (uint64_t)vlen, 0, &bfield->boolVal,
                vlen, &bfield->outLen));
    CMCall(bufarr, Add, bfield);
    return CMTrue;
FAILED:
    CMDBM_ODBC_BindFieldDestroy(bfield);
    return CMFalse;
}

CMDBM_STATIC CMBool CMDBM_ODBC_BindNull(
        SQLHSTMT stmt, CMUTIL_JsonValue *jval,
        CMUTIL_Array *bufarr, CMUTIL_Json *out, uint32_t index)
{
    CMDBM_ODBC_BindField *bfield =
            CMDBM_ODBC_BindFieldCreate(CMUTIL_JsonValueNull);
    bfield->outLen = SQL_NULL_DATA;
    if (out) {
        // change to string type for output
        SQLLEN osize = 4096;
        bfield->jtype = CMUTIL_JsonValueString;
        bfield->strVal = CMAlloc((uint64_t)osize);
        TRYODBC(stmt, SQL_HANDLE_STMT, SQLBindParameter(
                    stmt, (SQLUSMALLINT)(index+1), SQL_PARAM_OUTPUT, SQL_C_CHAR,
                    SQL_VARCHAR, 1, 0, bfield->strVal,
                    osize, &bfield->outLen));
    } else {
        TRYODBC(stmt, SQL_HANDLE_STMT, SQLBindParameter(
                    stmt, (SQLUSMALLINT)(index+1), SQL_PARAM_INPUT, SQL_C_CHAR,
                    SQL_VARCHAR, 1, 0, 0, 0, &bfield->outLen));
    }
    CMCall(bufarr, Add, bfield);
    CMUTIL_UNUSED(jval, out);
    return CMTrue;
FAILED:
    CMDBM_ODBC_BindFieldDestroy(bfield);
    return CMFalse;
}

typedef CMBool (*CMDBM_ODBC_BindProc)(
        SQLHSTMT,CMUTIL_JsonValue*,CMUTIL_Array*,CMUTIL_Json*,uint32_t);
static CMDBM_ODBC_BindProc g_cmdbm_odbc_bindprocs[]={
    CMDBM_ODBC_BindLong,
    CMDBM_ODBC_BindDouble,
    CMDBM_ODBC_BindString,
    CMDBM_ODBC_BindBoolean,
    CMDBM_ODBC_BindNull
};

CMDBM_STATIC void CMDBM_ODBC_BindSetOutLong(
        CMDBM_ODBC_BindField *bfield, CMUTIL_JsonValue *jval)
{
    CMCall(jval, SetLong, bfield->longVal);
}

CMDBM_STATIC void CMDBM_ODBC_BindSetOutDouble(
        CMDBM_ODBC_BindField *bfield, CMUTIL_JsonValue *jval)
{
    CMCall(jval, SetDouble, bfield->doubleVal);
}

CMDBM_STATIC void CMDBM_ODBC_BindSetOutString(
        CMDBM_ODBC_BindField *bfield, CMUTIL_JsonValue *jval)
{
    *(bfield->strVal + bfield->outLen) = 0x0;
    CMCall(jval, SetString, bfield->strVal);
}

CMDBM_STATIC void CMDBM_ODBC_BindSetOutBoolean(
        CMDBM_ODBC_BindField *bfield, CMUTIL_JsonValue *jval)
{
    CMCall(jval, SetBoolean, (CMBool)bfield->boolVal);
}

CMDBM_STATIC void CMDBM_ODBC_BindSetOutNull(
        CMDBM_ODBC_BindField *bfield, CMUTIL_JsonValue *jval)
{
    CMUTIL_UNUSED(bfield);
    CMCall(jval, SetNull);
}

typedef void (*CMDBM_ODBC_BindSetOutput)(
        CMDBM_ODBC_BindField *bfield, CMUTIL_JsonValue *jval);
static CMDBM_ODBC_BindSetOutput g_cmdbm_odbc_bindoutprocs[]={
    CMDBM_ODBC_BindSetOutLong,
    CMDBM_ODBC_BindSetOutDouble,
    CMDBM_ODBC_BindSetOutString,
    CMDBM_ODBC_BindSetOutBoolean,
    CMDBM_ODBC_BindSetOutNull
};

CMDBM_STATIC void CMDBM_ODBC_SetOutValue(
        CMDBM_ODBC_BindField *bfield, CMUTIL_JsonValue *jval)
{

    if (bfield->outLen == 0) {
        CMCall(jval, SetNull);
    } else {
        g_cmdbm_odbc_bindoutprocs[bfield->jtype](bfield, jval);
    }
}

CMDBM_STATIC SQLHSTMT CMDBM_ODBC_ExecuteBase(
        CMDBM_ODBCSession *sess, CMUTIL_String *query,
        CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    uint32_t i;
    size_t bsize = 0;
    CMBool succ = CMFalse;
    SQLHSTMT stmt = NULL;
    CMUTIL_Array *array = NULL;

    TRYODBC(sess->conn, SQL_HANDLE_DBC, SQLAllocHandle(
                SQL_HANDLE_STMT, sess->conn, &stmt));

    TRYODBC(stmt, SQL_HANDLE_STMT, SQLPrepare(
                stmt, (SQLCHAR*)CMCall(query, GetCString),
                (SQLINTEGER)CMCall(query, GetSize)));

    if (binds) {
        array = CMUTIL_ArrayCreateEx(
                    CMCall(binds, GetSize), NULL, CMDBM_ODBC_BindFieldDestroy);
        bsize = CMCall(binds, GetSize);
    }

    // bind variables.
    for (i=0; i<bsize; i++) {
        char ibuf[20];
        CMUTIL_Json *json = CMCall(binds, Get, i);
        sprintf(ibuf, "%d", i);
        if (CMCall(json, GetType) == CMUTIL_JsonTypeValue) {
            CMUTIL_JsonValue *jval = (CMUTIL_JsonValue*)json;
            CMUTIL_JsonValueType jtype = CMCall(jval, GetValueType);
            CMUTIL_Json *out = CMCall(outs, Get, ibuf);
            // type of json value
            g_cmdbm_odbc_bindprocs[jtype](stmt, jval, array, out, i);
        } else {
            CMLogError("binding variable is not value type JSON.");
            goto FAILED;
        }
    }

    TRYODBC(stmt, SQL_HANDLE_STMT, SQLExecute (stmt));

    if (binds && outs) {
        CMUTIL_StringArray *keys = CMCall(outs, GetKeys);
        for (i=0; i<CMCall(keys, GetSize); i++) {
            CMUTIL_String *sidx = CMCall(keys, GetAt, i);
            const char *cidx = CMCall(sidx, GetCString);
            CMUTIL_JsonValue *jval =
                    (CMUTIL_JsonValue*)CMCall(outs, Get, cidx);
            CMDBM_ODBC_BindField *bfield = NULL;
            uint32_t idx = (uint32_t)atoi(cidx);
            bfield = (CMDBM_ODBC_BindField*)CMCall(array, GetAt, idx);
            CMDBM_ODBC_SetOutValue(bfield, jval);
        }
        CMCall(keys, Destroy);
    }
    succ = CMTrue;
FAILED:
    if (!succ) {
        if (stmt) SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        stmt = NULL;
    }
    if (array) CMCall(array, Destroy);
    return stmt;
}

CMDBM_STATIC void CMDBM_ODBC_ResultAssignLong(
        CMDBM_ODBC_BindField *finfo, SQLHSTMT stmt,
        CMUTIL_JsonObject *row, SQLUSMALLINT idx)
{
    CMUTIL_UNUSED(stmt, idx);
    CMCall(row, PutLong, finfo->name, finfo->longVal);
}

CMDBM_STATIC void CMDBM_ODBC_ResultAssignDouble(
        CMDBM_ODBC_BindField *finfo, SQLHSTMT stmt,
        CMUTIL_JsonObject *row, SQLUSMALLINT idx)
{
    CMUTIL_UNUSED(stmt, idx);
    CMCall(row, PutDouble, finfo->name, finfo->doubleVal);
}

CMDBM_STATIC void CMDBM_ODBC_ResultAssignString(
        CMDBM_ODBC_BindField *finfo, SQLHSTMT stmt,
        CMUTIL_JsonObject *row, SQLUSMALLINT idx)
{
    SQLRETURN sr = SQL_SUCCESS;
    SQLLEN size = 0, bytes = 0;
    CMUTIL_String *strbuf = NULL;
    SQLCHAR buf[1024];
    CMCall(row, PutString, finfo->name, "");
    strbuf = CMCall(row, GetString, finfo->name);
    while (CMTrue) {
        sr = SQLGetData(stmt, idx, SQL_C_CHAR, buf, sizeof(buf), &size);
        if (sr == SQL_NO_DATA_FOUND)
            break;
        TRYODBC(stmt, SQL_HANDLE_STMT, sr);
        if (size == SQL_NULL_DATA) {
            CMCall(row, PutNull, finfo->name);
            break;
        }
        bytes = (size > 1024) || (size == SQL_NO_TOTAL) ?
                          1024 : size;
        CMCall(strbuf, AddNString, (char*)buf, (uint64_t)bytes);
    }
FAILED:
    // does nothing to do
    ;
}

CMDBM_STATIC void CMDBM_ODBC_ResultAssignBoolean(
        CMDBM_ODBC_BindField *finfo, SQLHSTMT stmt,
        CMUTIL_JsonObject *row, SQLUSMALLINT idx)
{
    CMBool bval = finfo->boolVal? CMTrue:CMFalse;
    CMUTIL_UNUSED(stmt, idx);
    CMCall(row, PutBoolean, finfo->name, bval);
}

CMDBM_STATIC SQLHSTMT CMDBM_ODBC_SelectBase(
        CMDBM_ODBCSession *sess, CMUTIL_String *query, CMUTIL_JsonArray *binds,
        CMUTIL_JsonObject *outs, CMUTIL_Array *fields)
{
    SQLHSTMT stmt = CMDBM_ODBC_ExecuteBase(sess, query, binds, outs);
    if (stmt) {
        int i;
        SQLSMALLINT numcols;
        CMBool succ = CMFalse;

        TRYODBC(stmt, SQL_HANDLE_STMT, SQLNumResultCols (stmt, &numcols));

        for (i=0; i<numcols; i++) {
            SQLCHAR name[2048];
            SQLSMALLINT namelen = 0;
            SQLSMALLINT dtype = 0;
            SQLULEN dsize = 0;
            SQLSMALLINT digits = 0;
            SQLSMALLINT nullable = 0;

            CMDBM_ODBC_BindField *col = NULL;
            TRYODBC(stmt, SQL_HANDLE_STMT, SQLDescribeCol (
                                stmt,               // Prepared Statement
                                (SQLUSMALLINT)i+1,  // Columnn Number
                                name,               // Column Name (returned)
                                2048,               // size of Column Name buffer
                                &namelen,           // Actual size of column name
                                &dtype,             // SQL Data type of column
                                &dsize,             // Data size of column in table
                                &digits,            // Number of decimal digits
                                &nullable));
            col = CMDBM_ODBC_BindFieldCreate(CMUTIL_JsonValueNull);
            col->name = CMAlloc((uint64_t)namelen+1);
            memcpy(col->name, name, (uint64_t)namelen);
            *(col->name + namelen) = 0x0;
            CMCall(fields, Add, col);
            switch (dtype) {
            case SQL_DECIMAL:
            case SQL_NUMERIC:
            case SQL_REAL:
            case SQL_FLOAT:
            case SQL_DOUBLE:
                col->jtype = CMUTIL_JsonValueDouble;
                col->fassign = CMDBM_ODBC_ResultAssignDouble;
                TRYODBC(stmt, SQL_HANDLE_STMT, SQLBindCol(
                            stmt, (SQLUSMALLINT)i+1, SQL_C_DOUBLE,
                            &col->doubleVal, sizeof(double), &col->outLen));
                break;
            case SQL_SMALLINT:
            case SQL_INTEGER:
            case SQL_TINYINT:
            case SQL_BIGINT:
            // TODO: treat as long type currently
            case SQL_TYPE_DATE:
            case SQL_TYPE_TIME:
            case SQL_TYPE_TIMESTAMP:
            // TODO: treat as long type currently
            case SQL_INTERVAL_MONTH:
            case SQL_INTERVAL_YEAR:
            case SQL_INTERVAL_YEAR_TO_MONTH:
            case SQL_INTERVAL_DAY:
            case SQL_INTERVAL_HOUR:
            case SQL_INTERVAL_MINUTE:
            case SQL_INTERVAL_SECOND:
            case SQL_INTERVAL_DAY_TO_HOUR:
            case SQL_INTERVAL_DAY_TO_MINUTE:
            case SQL_INTERVAL_DAY_TO_SECOND:
            case SQL_INTERVAL_HOUR_TO_MINUTE:
            case SQL_INTERVAL_HOUR_TO_SECOND:
            case SQL_INTERVAL_MINUTE_TO_SECOND:
                col->jtype = CMUTIL_JsonValueLong;
                col->fassign = CMDBM_ODBC_ResultAssignLong;
                TRYODBC(stmt, SQL_HANDLE_STMT, SQLBindCol(
                            stmt, (SQLUSMALLINT)i+1, SQL_C_LONG,
                            &col->longVal, sizeof(int64_t), &col->outLen));
                break;
            case SQL_BIT:
                col->jtype = CMUTIL_JsonValueBoolean;
                col->fassign = CMDBM_ODBC_ResultAssignBoolean;
                TRYODBC(stmt, SQL_HANDLE_STMT, SQLBindCol(
                            stmt, (SQLUSMALLINT)i+1, SQL_C_LONG,
                            &col->boolVal, sizeof(short), &col->outLen));
                break;
            case SQL_CHAR:
            case SQL_VARCHAR:
            case SQL_LONGVARCHAR:
            case SQL_WCHAR:
            case SQL_WVARCHAR:
            case SQL_WLONGVARCHAR:
            case SQL_BINARY:
            case SQL_VARBINARY:
            case SQL_LONGVARBINARY:
            default:
                col->jtype = CMUTIL_JsonValueString;
                col->fassign = CMDBM_ODBC_ResultAssignString;
//                TRYODBC(stmt, SQL_HANDLE_STMT, SQLBindCol(
//                            stmt, (SQLUSMALLINT)i+1, SQL_C_CHAR, NULL,
//                            0, &col->outLen));
                break;
            }
        }

        succ = CMTrue;
FAILED:
        if (!succ) {
            if (stmt) SQLFreeHandle(SQL_HANDLE_STMT, stmt);
            stmt = NULL;
        }
    }
    return stmt;
}

CMDBM_STATIC void CMUTIL_ODBC_RowSetFields(
        CMUTIL_Array *finfos, SQLHSTMT stmt, CMUTIL_JsonObject *row)
{
    SQLUSMALLINT i;
    size_t numfields = CMCall(finfos, GetSize);
    for (i=0; i<numfields; i++) {
        CMDBM_ODBC_BindField *finfo =
                (CMDBM_ODBC_BindField*)CMCall(finfos, GetAt, i);
        if (finfo->outLen == SQL_NULL_DATA) {
            CMCall(row, PutNull, finfo->name);
        } else {
            finfo->fassign(finfo, stmt, row, i+1);
        }
    }
}

CMDBM_STATIC CMUTIL_JsonObject *CMDBM_ODBC_GetRow(
        void *initres, void *connection,
        CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    CMDBM_ODBCSession *sess = (CMDBM_ODBCSession*)connection;
    CMUTIL_Array *fields = CMUTIL_ArrayCreateEx(
                10, NULL, CMDBM_ODBC_BindFieldDestroy);
    SQLHSTMT stmt = CMDBM_ODBC_SelectBase(sess, query, binds, outs, fields);
    CMUTIL_JsonObject *res = NULL;
    CMBool succ = CMFalse;

    if (stmt) {
        TRYODBC(stmt, SQL_HANDLE_STMT, SQLFetch(stmt));
        res = CMUTIL_JsonObjectCreate();
        CMUTIL_ODBC_RowSetFields(fields, stmt, res);
    } else {
        goto FAILED;
    }

    succ = CMTrue;
FAILED:
    if (stmt) {
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    }
    if (fields)
        CMCall(fields, Destroy);
    if (!succ && res) {
        CMUTIL_JsonDestroy(res);
        res = NULL;
    }
    CMUTIL_UNUSED(initres);
    return res;
}

CMDBM_STATIC CMUTIL_JsonValue *CMDBM_ODBC_GetOneValue(
        void *initres, void *connection,
        CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    CMUTIL_JsonValue *res = NULL;
    CMUTIL_JsonObject *row =
            CMDBM_ODBC_GetRow(initres, connection, query, binds, outs);
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

CMDBM_STATIC CMUTIL_JsonArray *CMDBM_ODBC_GetList(
        void *initres, void *connection,
        CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    CMDBM_ODBCSession *sess = (CMDBM_ODBCSession*)connection;
    CMUTIL_Array *fields = CMUTIL_ArrayCreateEx(
                10, NULL, CMDBM_ODBC_BindFieldDestroy);
    SQLHSTMT stmt = CMDBM_ODBC_SelectBase(sess, query, binds, outs, fields);
    CMUTIL_JsonArray *res = CMUTIL_JsonArrayCreate();
    CMBool succ = CMFalse;

    if (stmt) {
        int rcnt = 0;
        SQLRETURN sr = SQL_SUCCESS;
        while ((sr = SQLFetch(stmt)) == SQL_SUCCESS) {
            CMUTIL_JsonObject *obj = CMUTIL_JsonObjectCreate();
            CMUTIL_ODBC_RowSetFields(fields, stmt,  obj);
            CMCall(res, Add, (CMUTIL_Json*)obj);
            rcnt++;
        }
        if (sr == SQL_ERROR)
            TRYODBC(stmt, SQL_HANDLE_STMT, sr);
    } else {
        goto FAILED;
    }

    succ = CMTrue;
FAILED:
    if (stmt)
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    if (fields)
        CMCall(fields, Destroy);
    if (!succ && res) {
        CMUTIL_JsonDestroy(res);
        res = NULL;
    }
    CMUTIL_UNUSED(initres);
    return res;
}

CMDBM_STATIC int CMDBM_ODBC_Execute(
        void *initres, void *connection,
        CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    CMDBM_ODBCSession *sess = (CMDBM_ODBCSession*)connection;
    SQLHSTMT stmt = CMDBM_ODBC_ExecuteBase(sess, query, binds, outs);
    if (stmt) {
        SQLLEN rcnt = 0;
        TRYODBC(stmt, SQL_HANDLE_STMT, SQLRowCount(stmt, &rcnt));
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        return (int)rcnt;
    }
FAILED:
    if (stmt)
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    CMUTIL_UNUSED(initres);
    return -1;
}

typedef struct CMDBM_ODBC_Cursor {
    CMDBM_ODBCSession	*sess;
    SQLHSTMT			stmt;
    CMUTIL_Array		*fields;
} CMDBM_ODBC_Cursor;

CMDBM_STATIC void *CMDBM_ODBC_OpenCursor(
        void *initres, void *connection,
        CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    CMDBM_ODBCSession *sess = (CMDBM_ODBCSession*)connection;
    CMUTIL_Array *fields = CMUTIL_ArrayCreateEx(
                10, NULL, CMDBM_ODBC_BindFieldDestroy);
    SQLHSTMT stmt = CMDBM_ODBC_SelectBase(sess, query, binds, outs, fields);
    if (stmt) {
        CMDBM_ODBC_Cursor *res = CMAlloc(sizeof(CMDBM_ODBC_Cursor));
        memset(res, 0x0, sizeof(CMDBM_ODBC_Cursor));
        res->sess = sess;
        res->stmt = stmt;
        res->fields = fields;
        return res;
    }
    if (stmt)
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    if (fields)
        CMCall(fields, Destroy);
    CMUTIL_UNUSED(initres);
    return NULL;
}

CMDBM_STATIC void CMDBM_ODBC_CloseCursor(void *cursor)
{
    CMDBM_ODBC_Cursor *csr = (CMDBM_ODBC_Cursor*)cursor;
    if (csr) {
        if (csr->fields) CMCall(csr->fields, Destroy);
        if (csr->stmt)
            SQLFreeHandle(SQL_HANDLE_STMT, csr->stmt);
        if (csr->fields)
            CMCall(csr->fields, Destroy);
        CMFree(csr);
    }
}

CMDBM_STATIC CMUTIL_JsonObject *CMDBM_ODBC_CursorNextRow(void *cursor)
{
    CMDBM_ODBC_Cursor *csr = (CMDBM_ODBC_Cursor*)cursor;
    if (csr) {
        CMUTIL_JsonObject *res = NULL;
        TRYODBC(csr->stmt, SQL_HANDLE_STMT, SQLFetch(csr->stmt));
        res = CMUTIL_JsonObjectCreate();
        CMUTIL_ODBC_RowSetFields(csr->fields, csr->stmt, res);
        return res;
    }
    FAILED:
    return NULL;
}

CMDBM_STATIC void CMDBM_ODBC_LibraryInit()
{
}

CMDBM_STATIC void CMDBM_ODBC_LibraryClear()
{
}

CMDBM_ModuleInterface g_cmdbm_odbc_interface = {
    CMDBM_ODBC_LibraryInit,
    CMDBM_ODBC_LibraryClear,
    CMDBM_ODBC_GetDBMSKey,
    CMDBM_ODBC_Initialize,
    CMDBM_ODBC_CleanUp,
    CMDBM_ODBC_GetBindString,
    CMDBM_ODBC_GetTestQuery,
    CMDBM_ODBC_OpenConnection,
    CMDBM_ODBC_CloseConnection,
    CMDBM_ODBC_StartTransaction,
    CMDBM_ODBC_EndTransaction,
    CMDBM_ODBC_CommitTransaction,
    CMDBM_ODBC_RollbackTransaction,
    CMDBM_ODBC_GetOneValue,
    CMDBM_ODBC_GetRow,
    CMDBM_ODBC_GetList,
    CMDBM_ODBC_Execute,
    CMDBM_ODBC_OpenCursor,
    CMDBM_ODBC_CloseCursor,
    CMDBM_ODBC_CursorNextRow
};

#endif

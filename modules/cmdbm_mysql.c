
#include "functions.h"

#ifdef CMDBM_MYSQL

CMUTIL_LogDefine("cmdbm.module.mysql")

#include <mysql.h>

#define MYSQL_LOGERROR(sess,...) do {\
	char buf[4096]; sprintf(buf,##__VA_ARGS__);\
	if (sess) {\
		CMLogError("%s: %s", buf, mysql_error(sess->conn));\
	} else CMLogError("%s", buf);\
} while(0)

typedef struct CMDBM_MySQLCtx {
	CMUTIL_String	*prcs;
	CMUTIL_String	*dbcs;
} CMDBM_MySQLCtx;

typedef struct CMDBM_MySQLSession {
	CMDBM_MySQLCtx	*ctx;
	MYSQL			*conn;
} CMDBM_MySQLSession;

CMDBM_STATIC const char *CMDBM_MySQL_GetDBMSKey()
{
	return "MYSQL";
}

CMDBM_STATIC void *CMDBM_MySQL_Initialize(const char *dbcs, const char *prcs)
{
	CMDBM_MySQLCtx *res = CMAlloc(sizeof(CMDBM_MySQLCtx));
	CMUTIL_String *temp;
	memset(res, 0x0, sizeof(CMDBM_MySQLCtx));
	// mysql does not allow dash in character set name.
	temp = CMUTIL_StringCreateEx(10, prcs);
    res->prcs = CMCall(temp, Replace, "-", "");
    CMCall(temp, Destroy);
	temp = CMUTIL_StringCreateEx(10, dbcs);
    res->dbcs = CMCall(temp, Replace, "-", "");
    CMCall(temp, Destroy);
	return res;
}

CMDBM_STATIC void CMDBM_MySQL_CleanUp(
		void *initres)
{
	if (initres) {
		CMDBM_MySQLCtx *ctx = (CMDBM_MySQLCtx*)initres;
        if (ctx->prcs) CMCall(ctx->prcs, Destroy);
        if (ctx->dbcs) CMCall(ctx->dbcs, Destroy);
		CMFree(ctx);
	}
}

CMDBM_STATIC char *CMDBM_MySQL_GetBindString(
		void *initres, int index, char *buffer)
{
	CMUTIL_UNUSED(initres, index);
	strcpy(buffer, "?");
	return buffer;
}

CMDBM_STATIC const char *CMDBM_MySQL_GetTestQuery()
{
	return "select 1";
}

CMDBM_STATIC void *CMDBM_MySQL_OpenConnection(
		void *initres, CMUTIL_JsonObject *params)
{
	CMUTIL_JsonValue *host =
            (CMUTIL_JsonValue*)CMCall(params, Get, "host");
	CMUTIL_JsonValue *user =
            (CMUTIL_JsonValue*)CMCall(params, Get, "user");
	CMUTIL_JsonValue *pass =
            (CMUTIL_JsonValue*)CMCall(params, Get, "password");
	CMUTIL_JsonValue *db   =
            (CMUTIL_JsonValue*)CMCall(params, Get, "database");
	unsigned int port = 3306;

    if (CMCall(params, Get, "port"))
        port = (unsigned int)CMCall(params, GetLong, "port");

    if (host && user && pass && db) {
        const char *shost = CMCall(host, GetCString);
        const char *suser = CMCall(user, GetCString);
        const char *spass = CMCall(pass, GetCString);
        const char *sdb   = CMCall(db  , GetCString);
		CMDBM_MySQLSession *sess = CMAlloc(sizeof(CMDBM_MySQLSession));
		memset(sess, 0x0, sizeof(CMDBM_MySQLSession));
		sess->conn = mysql_init(NULL);
		sess->ctx = (CMDBM_MySQLCtx*)initres;
		if (mysql_real_connect(
					sess->conn, shost, suser, spass, sdb, port, NULL, 0)) {
			mysql_set_character_set(
                        sess->conn, CMCall(sess->ctx->prcs, GetCString));
			CMLogTrace("MySQL connection created.");
			return sess;
		} else {
			MYSQL_LOGERROR(sess, "connot connect to database");
			mysql_close(sess->conn);
			CMFree(sess);
			return NULL;
		}
	} else {
		CMLogError("MySQL connection requires "
				   "'host', 'user', 'password', 'database'");
		return NULL;
	}
}

CMDBM_STATIC void CMDBM_MySQL_CloseConnection(
		void *initres, void *connection)
{
	CMDBM_MySQLSession *sess = (CMDBM_MySQLSession*)connection;
	if (sess) {
		mysql_close(sess->conn);
		CMLogTrace("MySQL connection closed.");
		CMFree(sess);
	}
	CMUTIL_UNUSED(initres);
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MySQL_StartTransaction(
		void *initres, void *connection)
{
	CMDBM_MySQLSession *sess = (CMDBM_MySQLSession*)connection;
	CMUTIL_UNUSED(initres);
	if (mysql_query(sess->conn, "SET autocommit=0")) {
		MYSQL_LOGERROR(sess,
					   "current MySQL database does not support transaction.");
        return CMFalse;
	}
	if (mysql_query(sess->conn, "START TRANSACTION")) {
		MYSQL_LOGERROR(sess,
					   "current MySQL database does not support transaction.");
        return CMFalse;
	}
    return CMTrue;
}

CMDBM_STATIC void CMDBM_MySQL_EndTransaction(
		void *initres, void *connection)
{
	CMDBM_MySQLSession *sess = (CMDBM_MySQLSession*)connection;
	mysql_query(sess->conn, "SET autocommit=1");
	CMUTIL_UNUSED(initres);
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MySQL_CommitTransaction(
		void *initres, void *connection)
{
	CMDBM_MySQLSession *sess = (CMDBM_MySQLSession*)connection;
	CMUTIL_UNUSED(initres);
	if (mysql_query(sess->conn, "COMMIT"))
        return CMTrue;
	else
		MYSQL_LOGERROR(sess, "commit failed.");
    return CMFalse;
}

CMDBM_STATIC void CMDBM_MySQL_RollbackTransaction(
		void *initres, void *connection)
{
	CMDBM_MySQLSession *sess = (CMDBM_MySQLSession*)connection;
	if (mysql_query(sess->conn, "ROLLBACK"))
		MYSQL_LOGERROR(sess, "rollback failed.");
	CMUTIL_UNUSED(initres);
}

CMDBM_STATIC void CMDBM_MySQL_BindLong(
		MYSQL_BIND *bind, CMUTIL_JsonValue *jval,
		CMUTIL_Array *bufarr, CMUTIL_Json *out)
{
    int64_t *pval = CMAlloc(sizeof(int64_t));
    *pval = CMCall(jval, GetLong);
	bind->buffer_type = MYSQL_TYPE_LONGLONG;
	bind->buffer = pval;
    bind->buffer_length = sizeof(int64_t);
    CMCall(bufarr, Add, pval);
	CMUTIL_UNUSED(out);
}

CMDBM_STATIC void CMDBM_MySQL_BindDouble(
		MYSQL_BIND *bind, CMUTIL_JsonValue *jval,
		CMUTIL_Array *bufarr, CMUTIL_Json *out)
{
	double *pval = CMAlloc(sizeof(double));
    *pval = CMCall(jval, GetDouble);
	bind->buffer_type = MYSQL_TYPE_DOUBLE;
	bind->buffer = pval;
	bind->buffer_length = sizeof(double);
    CMCall(bufarr, Add, pval);
	CMUTIL_UNUSED(out);
}

CMDBM_STATIC void CMDBM_MySQL_BindString(
		MYSQL_BIND *bind, CMUTIL_JsonValue *jval,
		CMUTIL_Array *bufarr, CMUTIL_Json *out)
{
	char buf[4096] = {0,};
    CMUTIL_String *sval = CMCall(jval, GetString);
    if (out) CMCall(sval, AddNString, buf, 4096);
	bind->buffer_type = MYSQL_TYPE_VARCHAR;
    bind->buffer = (void*)CMCall(sval, GetCString);
    bind->buffer_length = (ulong)CMCall(sval, GetSize);
	CMUTIL_UNUSED(bufarr);
}

CMDBM_STATIC void CMDBM_MySQL_BindBoolean(
		MYSQL_BIND *bind, CMUTIL_JsonValue *jval,
		CMUTIL_Array *bufarr, CMUTIL_Json *out)
{
	char *pval = CMAlloc(1);
    *pval = (char)CMCall(jval, GetBoolean);
	bind->buffer_type = MYSQL_TYPE_TINY;
	bind->buffer = pval;
	bind->buffer_length = 1;
    CMCall(bufarr, Add, pval);
	CMUTIL_UNUSED(out);
}
CMDBM_STATIC void CMDBM_MySQL_BindNull(
		MYSQL_BIND *bind, CMUTIL_JsonValue *jval,
		CMUTIL_Array *bufarr, CMUTIL_Json *out)
{
	bind->buffer_type = MYSQL_TYPE_NULL;
	CMUTIL_UNUSED(jval, bufarr, out);
}

typedef void (*CMDBM_MySQL_BindProc)(
		MYSQL_BIND*,CMUTIL_JsonValue*,CMUTIL_Array*,CMUTIL_Json*);
static CMDBM_MySQL_BindProc g_cmdbm_mysql_bindprocs[]={
	CMDBM_MySQL_BindLong,
	CMDBM_MySQL_BindDouble,
	CMDBM_MySQL_BindString,
	CMDBM_MySQL_BindBoolean,
	CMDBM_MySQL_BindNull
};

CMDBM_STATIC void CMDBM_MySQL_SetOutValue(
		MYSQL_BIND *bind, CMUTIL_JsonValue *jval)
{
	if (bind->is_null_value) {
        CMCall(jval, SetNull);
	} else {
		CMUTIL_String *temp;
		switch (bind->buffer_type) {
		case MYSQL_TYPE_TINY:
            CMCall(jval, SetBoolean, (CMUTIL_Bool)*((char*)bind->buffer));
			break;
		case MYSQL_TYPE_LONGLONG:
            CMCall(jval, SetLong, (int64_t)*((int64_t*)bind->buffer));
			break;
		case MYSQL_TYPE_DOUBLE:
            CMCall(jval, SetDouble, (double)*((double*)bind->buffer));
			break;
		case MYSQL_TYPE_VARCHAR:
            temp = CMCall(jval, GetString);
            CMCall(temp, CutTailOff, (size_t)bind->length_value);
			break;
		default:
			CMLogErrorS("unknown type %d", bind->buffer_type);
		}
	}
}

CMDBM_STATIC MYSQL_STMT *CMDBM_MySQL_ExecuteBase(
		CMDBM_MySQLSession *sess, CMUTIL_String *query,
		CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    uint i;
    size_t bsize = 0;
    CMUTIL_Bool succ = CMFalse;
	MYSQL_STMT *stmt = mysql_stmt_init(sess->conn);
	MYSQL_BIND *buffers = NULL;
	CMUTIL_Array *array = NULL;

	if (binds) {
		array = CMUTIL_ArrayCreateEx(
                    CMCall(binds, GetSize), NULL, CMFree);
        bsize = CMCall(binds, GetSize);
        buffers = CMAlloc(sizeof(MYSQL_BIND) * (ulong)bsize);
        memset(buffers, 0x0, sizeof(MYSQL_BIND) * (ulong)bsize);
	}

    if (mysql_stmt_prepare(stmt, CMCall(query, GetCString),
                           (unsigned long)CMCall(query, GetSize))) {
		MYSQL_LOGERROR(sess, "prepare statement failed.");
		goto FAILEDPOINT;
	}
	// bind variables.
	for (i=0; i<bsize; i++) {
		char ibuf[20];
        CMUTIL_Json *json = CMCall(binds, Get, i);
		sprintf(ibuf, "%d", i);
        if (CMCall(json, GetType) == CMUTIL_JsonTypeValue) {
			CMUTIL_JsonValue *jval = (CMUTIL_JsonValue*)json;
            CMUTIL_Json *out = CMCall(outs, Get, ibuf);
			// type of json value
            g_cmdbm_mysql_bindprocs[CMCall(jval, GetValueType)](
						&buffers[i], jval, array, out);
		} else {
			CMLogError("binding variable is not value type JSON.");
			goto FAILEDPOINT;
		}
	}

	if (bsize > 0) {
		if (mysql_stmt_bind_param(stmt, buffers) != 0) {
			MYSQL_LOGERROR(sess, "variable binding failed.");
			goto FAILEDPOINT;
		}
	}

	if (mysql_stmt_execute(stmt) == 0) {
		if (outs) {
            CMUTIL_StringArray *keys = CMCall(outs, GetKeys);
            for (i=0; i<CMCall(keys, GetSize); i++) {
                CMUTIL_String *sidx = CMCall(keys, GetAt, i);
                const char *cidx = CMCall(sidx, GetCString);
				CMUTIL_JsonValue *jval =
                        (CMUTIL_JsonValue*)CMCall(outs, Get, cidx);
				int idx = atoi(cidx);
				CMDBM_MySQL_SetOutValue(&buffers[idx], jval);
			}
            CMCall(keys, Destroy);
		}
	} else {
		MYSQL_LOGERROR(sess, "execute statement failed.");
		goto FAILEDPOINT;
	}
    succ = CMTrue;
FAILEDPOINT:
	if (!succ) {
		if (stmt) mysql_stmt_close(stmt);
		stmt = NULL;
	}
	if (buffers) CMFree(buffers);
    if (array) CMCall(array, Destroy);
	return stmt;
}

typedef struct CMDBM_MySQL_FieldInfo CMDBM_MySQL_FieldInfo;
struct CMDBM_MySQL_FieldInfo {
    char name[2048];
    double doubleVal;
    int64_t longVal;
    unsigned long length;
    MYSQL_BIND *bind;
    CMDBM_MySQLSession *sess;
    void (*fassign)(CMDBM_MySQL_FieldInfo*, MYSQL_STMT*, CMUTIL_JsonObject*);
    int index;
    CMUTIL_JsonValueType jtype;
    my_bool isnull;
    my_bool error;
    char    dummy_padder[6];
};

CMDBM_STATIC void CMDBM_MySQL_ResultAssignLong(
		CMDBM_MySQL_FieldInfo *finfo, MYSQL_STMT *stmt, CMUTIL_JsonObject *row)
{
	CMUTIL_UNUSED(stmt);
    CMCall(row, PutLong, finfo->name, finfo->longVal);
}

CMDBM_STATIC void CMDBM_MySQL_ResultAssignDouble(
		CMDBM_MySQL_FieldInfo *finfo, MYSQL_STMT *stmt, CMUTIL_JsonObject *row)
{
	CMUTIL_UNUSED(stmt);
    CMCall(row, PutDouble, finfo->name, finfo->doubleVal);
}

CMDBM_STATIC void CMDBM_MySQL_ResultAssignString(
		CMDBM_MySQL_FieldInfo *finfo, MYSQL_STMT *stmt, CMUTIL_JsonObject *row)
{
    MYSQL_BIND *bind = finfo->bind;
	if (finfo->length > 0) {
        int ival;
        char *buffer = CMAlloc(finfo->length*2+1);
        bind->buffer = buffer;
        bind->buffer_length = finfo->length*2;
        memset(buffer, 0x0, finfo->length*2+1);
        ival = mysql_stmt_fetch_column(
                    stmt, finfo->bind, (uint)finfo->index, 0);
        if (ival != 0) {
            MYSQL_LOGERROR(finfo->sess, "mysql_stmt_fetch_column() failed.");
        } else {
            CMCall(row, PutString, finfo->name, buffer);
        }
        // reset buffer
        CMFree(buffer);
        bind->buffer = NULL;
        bind->buffer_length = 0;
//		CMUTIL_CALL(row, PutString, finfo->name, (char*)bind->buffer);
//		memset(bind->buffer, 0x0, 4096);
	} else {
        CMCall(row, PutNull, finfo->name);
	}
	CMUTIL_UNUSED(stmt);
}

CMDBM_STATIC void CMDBM_MySQL_ResultAssignBoolean(
		CMDBM_MySQL_FieldInfo *finfo, MYSQL_STMT *stmt, CMUTIL_JsonObject *row)
{
    CMUTIL_Bool bval = finfo->longVal? CMTrue:CMFalse;
	CMUTIL_UNUSED(stmt);
    CMCall(row, PutBoolean, finfo->name, bval);
}

CMDBM_STATIC MYSQL_STMT *CMDBM_MySQL_SelectBase(
		CMDBM_MySQLSession *sess, CMUTIL_String *query, CMUTIL_JsonArray *binds,
		CMUTIL_JsonObject *outs, CMUTIL_Array *fields, MYSQL_RES **meta,
		MYSQL_BIND **resbuf)
{
	MYSQL_STMT *stmt = CMDBM_MySQL_ExecuteBase(sess, query, binds, outs);
	if (stmt) {
		int i, fieldcnt;
		MYSQL_FIELD *ofields = NULL;
        CMUTIL_Bool succ = CMFalse;

		if (mysql_stmt_store_result(stmt) != 0) {
			MYSQL_LOGERROR(sess, "execute statement failed.");
			goto FAILEDPOINT;
		}

		*meta = mysql_stmt_result_metadata(stmt);
		if (*meta == NULL) {
			MYSQL_LOGERROR(sess, "cannot get result metadata.");
			goto FAILEDPOINT;
		}

        fieldcnt = (int)mysql_field_count(sess->conn);
		ofields = mysql_fetch_fields(*meta);
        *resbuf = CMAlloc(sizeof(MYSQL_BIND) * (ulong)fieldcnt);
        memset(*resbuf, 0x0, sizeof(MYSQL_BIND) * (ulong)fieldcnt);
		for (i=0; i<fieldcnt; i++) {
			MYSQL_FIELD *f = &ofields[i];
			MYSQL_BIND *b = &((*resbuf)[i]);
			CMDBM_MySQL_FieldInfo *finfo =
					CMAlloc(sizeof(CMDBM_MySQL_FieldInfo));
			memset(finfo, 0x0, sizeof(CMDBM_MySQL_FieldInfo));
			strncat(finfo->name, f->name, f->name_length);
			finfo->index = i;
			switch(f->type) {
			case MYSQL_TYPE_BIT:
				// treat as boolean
				finfo->fassign = CMDBM_MySQL_ResultAssignBoolean;
				b->buffer_type = MYSQL_TYPE_LONGLONG;
				b->buffer = &finfo->longVal;
				finfo->jtype = CMUTIL_JsonValueBoolean;
				break;

			case MYSQL_TYPE_TINY:
			case MYSQL_TYPE_SHORT:
			case MYSQL_TYPE_LONG:
			case MYSQL_TYPE_TIMESTAMP:
			case MYSQL_TYPE_LONGLONG:
			case MYSQL_TYPE_INT24:
			case MYSQL_TYPE_ENUM:
			case MYSQL_TYPE_TIMESTAMP2:
				// treat as long
				finfo->fassign = CMDBM_MySQL_ResultAssignLong;
				b->buffer_type = MYSQL_TYPE_LONGLONG;
				b->buffer = &finfo->longVal;
				finfo->jtype = CMUTIL_JsonValueLong;
				break;

			case MYSQL_TYPE_FLOAT:
			case MYSQL_TYPE_DOUBLE:
			case MYSQL_TYPE_DECIMAL:
			case MYSQL_TYPE_NEWDECIMAL:
				// treat as double
				finfo->fassign = CMDBM_MySQL_ResultAssignDouble;
				b->buffer_type = MYSQL_TYPE_DOUBLE;
				b->buffer = &finfo->doubleVal;
				finfo->jtype = CMUTIL_JsonValueDouble;
				break;

			default:
				// treat as string
				finfo->fassign = CMDBM_MySQL_ResultAssignString;
				b->buffer_type = MYSQL_TYPE_STRING;
                b->buffer = NULL;
                b->buffer_length = 0;
				finfo->jtype = CMUTIL_JsonValueString;
				break;
			}
			b->length = &(finfo->length);
			b->is_null = &(finfo->isnull);
			b->error = &(finfo->error);
            finfo->bind = b;
			finfo->sess = sess;
            CMCall(fields, Add, finfo);
		}
		if (mysql_stmt_bind_result(stmt, *resbuf) != 0) {
			MYSQL_LOGERROR(sess, "cannot bind result buffers.");
			goto FAILEDPOINT;
		}

        succ = CMTrue;
FAILEDPOINT:
		if (!succ) {
			mysql_stmt_close(stmt);
			stmt = NULL;
		}
	}
	return stmt;
}

CMDBM_STATIC void CMUTIL_MySQL_RowSetFields(
		CMUTIL_Array *finfos, MYSQL_STMT *stmt, CMUTIL_JsonObject *row)
{
    uint i;
    size_t numfields = CMCall(finfos, GetSize);
	for (i=0; i<numfields; i++) {
		CMDBM_MySQL_FieldInfo *finfo =
                (CMDBM_MySQL_FieldInfo*)CMCall(finfos, GetAt, i);
		if (finfo->isnull) {
            CMCall(row, PutNull, finfo->name);
		} else {
			finfo->fassign(finfo, stmt, row);
		}
	}
}

CMDBM_STATIC void CMDBM_MySQL_FieldDestroy(void *data)
{
	CMDBM_MySQL_FieldInfo *finfo = (CMDBM_MySQL_FieldInfo*)data;
	if (finfo) {
		if (finfo->jtype == CMUTIL_JsonValueString && finfo->bind->buffer)
            CMFree(finfo->bind->buffer);
		CMFree(finfo);
	}
}

CMDBM_STATIC CMUTIL_JsonObject *CMDBM_MySQL_GetRow(
		void *initres, void *connection,
		CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
	CMDBM_MySQLSession *sess = (CMDBM_MySQLSession*)connection;
	CMUTIL_Array *fields = CMUTIL_ArrayCreateEx(
				10, NULL, CMDBM_MySQL_FieldDestroy);
	MYSQL_RES *meta = NULL;
	MYSQL_BIND *resb = NULL;
	MYSQL_STMT *stmt = CMDBM_MySQL_SelectBase(
				sess, query, binds, outs, fields, &meta, &resb);
	CMUTIL_JsonObject *res = NULL;
    CMUTIL_Bool succ = CMFalse;

	if (stmt) {
		if (mysql_stmt_fetch(stmt) != 0) {
			MYSQL_LOGERROR(sess, "cannot fetch row.");
			goto FAILEDPOINT;
		}
		res = CMUTIL_JsonObjectCreate();
		CMUTIL_MySQL_RowSetFields(fields, stmt, res);
	} else {
		goto FAILEDPOINT;
	}

    succ = CMTrue;
FAILEDPOINT:
	if (meta) mysql_free_result(meta);
	if (stmt) {
		mysql_stmt_free_result(stmt);
		mysql_stmt_close(stmt);
	}
	if (fields)
        CMCall(fields, Destroy);
	if (resb) CMFree(resb);
	if (!succ && res) {
		CMUTIL_JsonDestroy(res);
		res = NULL;
	}
	CMUTIL_UNUSED(initres);
	return res;
}

CMDBM_STATIC CMUTIL_JsonValue *CMDBM_MySQL_GetOneValue(
		void *initres, void *connection,
		CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
	CMUTIL_JsonValue *res = NULL;
	CMUTIL_JsonObject *row =
			CMDBM_MySQL_GetRow(initres, connection, query, binds, outs);
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

CMDBM_STATIC CMUTIL_JsonArray *CMDBM_MySQL_GetList(
		void *initres, void *connection,
		CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
	CMDBM_MySQLSession *sess = (CMDBM_MySQLSession*)connection;
	CMUTIL_Array *fields = CMUTIL_ArrayCreateEx(
				10, NULL, CMDBM_MySQL_FieldDestroy);
	MYSQL_RES *meta = NULL;
	MYSQL_BIND *resb = NULL;
	MYSQL_STMT *stmt = CMDBM_MySQL_SelectBase(
				sess, query, binds, outs, fields, &meta, &resb);
	CMUTIL_JsonArray *res = CMUTIL_JsonArrayCreate();
    CMUTIL_Bool succ = CMFalse;

	if (stmt) {
		int rcnt = 0, rval;
		while ((rval = mysql_stmt_fetch(stmt)) == 0 ||
			   rval == MYSQL_DATA_TRUNCATED) {
			CMUTIL_JsonObject *obj = CMUTIL_JsonObjectCreate();
			CMUTIL_MySQL_RowSetFields(fields, stmt,  obj);
            CMCall(res, Add, (CMUTIL_Json*)obj);
			rcnt++;
		}
		if (rval == 1) {
			MYSQL_LOGERROR(sess, "fetch row failed.%d", rval);
		}
	} else {
		goto FAILEDPOINT;
	}

    succ = CMTrue;
FAILEDPOINT:
	if (meta) mysql_free_result(meta);
	if (stmt) {
		mysql_stmt_free_result(stmt);
		mysql_stmt_close(stmt);
	}
	if (fields)
        CMCall(fields, Destroy);
	if (resb) CMFree(resb);
	if (!succ && res) {
		CMUTIL_JsonDestroy(res);
		res = NULL;
	}
	CMUTIL_UNUSED(initres);
	return res;
}

CMDBM_STATIC int CMDBM_MySQL_Execute(
		void *initres, void *connection,
		CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
	CMDBM_MySQLSession *sess = (CMDBM_MySQLSession*)connection;
	MYSQL_STMT *stmt = CMDBM_MySQL_ExecuteBase(sess, query, binds, outs);
	if (stmt) {
		int res = (int)mysql_stmt_affected_rows(stmt);
		mysql_stmt_close(stmt);
		return res;
	}
	CMUTIL_UNUSED(initres);
	return -1;
}

typedef struct CMDBM_MySQL_Cursor {
	CMDBM_MySQLSession	*sess;
	MYSQL_STMT			*stmt;
	MYSQL_RES			*meta;
	MYSQL_BIND			*resb;
	CMUTIL_Array		*fields;
} CMDBM_MySQL_Cursor;

CMDBM_STATIC void *CMDBM_MySQL_OpenCursor(
		void *initres, void *connection,
		CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
	CMDBM_MySQLSession *sess = (CMDBM_MySQLSession*)connection;
	CMUTIL_Array *fields = CMUTIL_ArrayCreateEx(
				10, NULL, CMDBM_MySQL_FieldDestroy);
	MYSQL_RES *meta = NULL;
	MYSQL_BIND *resb = NULL;
	MYSQL_STMT *stmt = CMDBM_MySQL_SelectBase(
				sess, query, binds, outs, fields, &meta, &resb);
	if (stmt) {
		CMDBM_MySQL_Cursor *res = CMAlloc(sizeof(CMDBM_MySQL_Cursor));
		memset(res, 0x0, sizeof(CMDBM_MySQL_Cursor));
		res->sess = sess;
		res->stmt = stmt;
		res->meta = meta;
		res->resb = resb;
		res->fields = fields;
		return res;
	}
	if (meta)
		mysql_free_result(meta);
	if (stmt) {
		mysql_stmt_free_result(stmt);
		mysql_stmt_close(stmt);
	}
	if (fields)
        CMCall(fields, Destroy);
	if (resb) CMFree(resb);
	CMUTIL_UNUSED(initres);
	return NULL;
}

CMDBM_STATIC void CMDBM_MySQL_CloseCursor(void *cursor)
{
	CMDBM_MySQL_Cursor *csr = (CMDBM_MySQL_Cursor*)cursor;
	if (csr) {
        if (csr->fields) CMCall(csr->fields, Destroy);
		if (csr->meta) mysql_free_result(csr->meta);
		if (csr->stmt) {
			mysql_stmt_free_result(csr->stmt);
			mysql_stmt_close(csr->stmt);
		}
		if (csr->fields)
            CMCall(csr->fields, Destroy);
		if (csr->resb) CMFree(csr->resb);
		CMFree(csr);
	}
}

CMDBM_STATIC CMUTIL_JsonObject *CMDBM_MySQL_CursorNextRow(void *cursor)
{
	CMDBM_MySQL_Cursor *csr = (CMDBM_MySQL_Cursor*)cursor;
	if (csr) {
		if (mysql_stmt_fetch(csr->stmt) == 0) {
			CMUTIL_JsonObject *res = CMUTIL_JsonObjectCreate();
			CMUTIL_MySQL_RowSetFields(csr->fields, csr->stmt, res);
			return res;
		}
	}
	return NULL;
}

CMDBM_STATIC void CMDBM_MySQL_LibraryInit()
{
	mysql_library_init(0, NULL, NULL);
}

CMDBM_STATIC void CMDBM_MySQL_LibraryClear()
{
	mysql_library_end();
}

CMDBM_ModuleInterface g_cmdbm_mysql_interface = {
	CMDBM_MySQL_LibraryInit,
	CMDBM_MySQL_LibraryClear,
	CMDBM_MySQL_GetDBMSKey,
	CMDBM_MySQL_Initialize,
	CMDBM_MySQL_CleanUp,
	CMDBM_MySQL_GetBindString,
	CMDBM_MySQL_GetTestQuery,
	CMDBM_MySQL_OpenConnection,
	CMDBM_MySQL_CloseConnection,
	CMDBM_MySQL_StartTransaction,
	CMDBM_MySQL_EndTransaction,
	CMDBM_MySQL_CommitTransaction,
	CMDBM_MySQL_RollbackTransaction,
	CMDBM_MySQL_GetOneValue,
	CMDBM_MySQL_GetRow,
	CMDBM_MySQL_GetList,
	CMDBM_MySQL_Execute,
	CMDBM_MySQL_OpenCursor,
	CMDBM_MySQL_CloseCursor,
	CMDBM_MySQL_CursorNextRow
};

#endif

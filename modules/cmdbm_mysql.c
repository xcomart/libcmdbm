
#include "../src/functions.h"

#if defined(CMDBM_MYSQL) || defined(CMDBM_MARIA)

#if defined(CMDBM_MARIA)
CMUTIL_LogDefine("cmdbm.module.maria")
#else
CMUTIL_LogDefine("cmdbm.module.mysql")
#endif

#include <mysql.h>

// my_bool was removed from MySQL client headers in 8.0
// (MariaDB still provides it).
#if !defined(MARIADB_BASE_VERSION) && defined(MYSQL_VERSION_ID) && \
    MYSQL_VERSION_ID >= 80000
# include <stdbool.h>
typedef bool my_bool;
#endif

#define MYSQL_LOGERROR(sess,...) do {\
	char buf[4096]; snprintf(buf,sizeof(buf),##__VA_ARGS__);\
    if (sess && sess->conn) {\
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

CMDBM_STATIC char *CMDBM_MySQL_Charset(const char *ocharset, char *outbuf);

CMDBM_STATIC void *CMDBM_MySQL_Initialize(const char *dbcs, const char *prcs)
{
	CMDBM_MySQLCtx *res = CMAlloc(sizeof(CMDBM_MySQLCtx));
	char buf[128];
	memset(res, 0x0, sizeof(CMDBM_MySQLCtx));
	// mysql does not allow a separator in a character set name, and the
	// name the connection is opened with has to match the one the pool was
	// initialized with - so both are stripped through the same helper.
	res->prcs = CMUTIL_StringCreateEx(
				10, CMDBM_MySQL_Charset(prcs? prcs:"utf8", buf));
	res->dbcs = CMUTIL_StringCreateEx(
				10, CMDBM_MySQL_Charset(dbcs? dbcs:"utf8", buf));
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
        void *initres, uint32_t index, char *buffer, CMJsonValueType vtype)
{
    CMUTIL_UNUSED(initres, index, vtype);
	strcpy(buffer, "?");
	return buffer;
}

CMDBM_STATIC const char *CMDBM_MySQL_GetTestQuery()
{
	return "select 1";
}

CMDBM_STATIC char *CMDBM_MySQL_Charset(const char *ocharset, char *outbuf)
{
    char *p = outbuf;
    const char *q = ocharset;
    while (*q) {
        if (strchr("-_/\\", *q) == NULL)
            *p++ = *q;
        q++;
    }
    *p = 0x0;
    return outbuf;
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
		if (sess->conn == NULL) {
			CMLogError("mysql_init() failed. (out of memory)");
			CMFree(sess);
			return NULL;
		}
		sess->ctx = (CMDBM_MySQLCtx*)initres;
		if (mysql_real_connect(
					sess->conn, shost, suser, spass, sdb, port, NULL,
                    CLIENT_MULTI_STATEMENTS)) {
            // Initialize already stripped the separators.
            const char *cs = CMCall(sess->ctx->prcs, GetCString);
			mysql_set_character_set(sess->conn, cs);
			CMLogTrace("MySQL connection created.");
			return sess;
		} else {
			MYSQL_LOGERROR(sess, "cannot connect to database");
			mysql_close(sess->conn);
			CMFree(sess);
			return NULL;
		}
	} else {
		CMLogError("MySQL connection requires "
				   "'host', 'user', 'password', 'database' "
				   "and optional 'port'");
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

CMDBM_STATIC CMBool CMDBM_MySQL_StartTransaction(
		void *initres, void *connection)
{
	CMDBM_MySQLSession *sess = (CMDBM_MySQLSession*)connection;
	if (sess == NULL || sess->conn == NULL) {
		CMLogError("Invalid parameter.");
        return CMFalse;
	}
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
	if (sess == NULL || sess->conn == NULL) {
		CMLogError("Invalid parameter.");
		return;
	}
	mysql_query(sess->conn, "SET autocommit=1");
	CMUTIL_UNUSED(initres);
}

CMDBM_STATIC CMBool CMDBM_MySQL_CommitTransaction(
		void *initres, void *connection)
{
	CMDBM_MySQLSession *sess = (CMDBM_MySQLSession*)connection;
	if (sess == NULL || sess->conn == NULL) {
		CMLogError("Invalid parameter.");
		return CMFalse;
	}
	CMUTIL_UNUSED(initres);
	if (mysql_query(sess->conn, "COMMIT")) {
		MYSQL_LOGERROR(sess, "commit failed.");
		return CMFalse;
	}
    return CMTrue;
}

CMDBM_STATIC void CMDBM_MySQL_RollbackTransaction(
		void *initres, void *connection)
{
	CMDBM_MySQLSession *sess = (CMDBM_MySQLSession*)connection;
	if (sess == NULL || sess->conn == NULL) {
		CMLogError("Invalid parameter.");
		return;
	}
	if (mysql_query(sess->conn, "ROLLBACK"))
		MYSQL_LOGERROR(sess, "rollback failed.");
	CMUTIL_UNUSED(initres);
}

CMDBM_STATIC void CMDBM_MySQL_BindLong(
		MYSQL_BIND *bind, CMUTIL_JsonValue *jval,
		CMUTIL_Array *bufarr)
{
    int64_t *pval = CMAlloc(sizeof(int64_t));
    *pval = CMCall(jval, GetLong);
	bind->buffer_type = MYSQL_TYPE_LONGLONG;
	bind->buffer = pval;
    bind->buffer_length = sizeof(int64_t);
    CMCall(bufarr, Add, pval, NULL);
}

CMDBM_STATIC void CMDBM_MySQL_BindDouble(
		MYSQL_BIND *bind, CMUTIL_JsonValue *jval,
		CMUTIL_Array *bufarr)
{
	double *pval = CMAlloc(sizeof(double));
    *pval = CMCall(jval, GetDouble);
	bind->buffer_type = MYSQL_TYPE_DOUBLE;
	bind->buffer = pval;
	bind->buffer_length = sizeof(double);
    CMCall(bufarr, Add, pval, NULL);
}

CMDBM_STATIC void CMDBM_MySQL_BindString(
		MYSQL_BIND *bind, CMUTIL_JsonValue *jval,
		CMUTIL_Array *bufarr)
{
    CMUTIL_String *sval = (CMUTIL_String*)CMCall(jval, GetString);
	bind->buffer_type = MYSQL_TYPE_STRING;
    bind->buffer = (void*)CMCall(sval, GetCString);
    bind->buffer_length = (uint64_t)CMCall(sval, GetSize);
	CMUTIL_UNUSED(bufarr);
}

CMDBM_STATIC void CMDBM_MySQL_BindBoolean(
		MYSQL_BIND *bind, CMUTIL_JsonValue *jval,
		CMUTIL_Array *bufarr)
{
	char *pval = CMAlloc(1);
    *pval = (char)(0+CMCall(jval, GetBoolean));
	bind->buffer_type = MYSQL_TYPE_TINY;
	bind->buffer = pval;
	bind->buffer_length = 1;
    CMCall(bufarr, Add, pval, NULL);
}
CMDBM_STATIC void CMDBM_MySQL_BindNull(
		MYSQL_BIND *bind, CMUTIL_JsonValue *jval,
		CMUTIL_Array *bufarr)
{
	bind->buffer_type = MYSQL_TYPE_NULL;
	CMUTIL_UNUSED(jval, bufarr);
}

typedef void (*CMDBM_MySQL_BindProc)(
		MYSQL_BIND*,CMUTIL_JsonValue*,CMUTIL_Array*);
static CMDBM_MySQL_BindProc g_cmdbm_mysql_bindprocs[]={
	CMDBM_MySQL_BindLong,
	CMDBM_MySQL_BindDouble,
	CMDBM_MySQL_BindString,
	CMDBM_MySQL_BindBoolean,
	CMDBM_MySQL_BindNull
};

CMDBM_STATIC MYSQL_STMT *CMDBM_MySQL_ExecuteBase(
		CMDBM_MySQLSession *sess, CMUTIL_String *query,
		CMUTIL_JsonArray *binds, uint32_t fetchsize)
{
    uint32_t i;
    size_t bsize = 0;
    CMBool succ = CMFalse;
	MYSQL_STMT *stmt = mysql_stmt_init(sess->conn);
	MYSQL_BIND *buffers = NULL;
	CMUTIL_Array *array = NULL;

	if (stmt == NULL) {
		MYSQL_LOGERROR(sess, "mysql_stmt_init() failed.");
		return NULL;
	}

	if (binds) {
		array = CMUTIL_ArrayCreateEx(
                    CMCall(binds, GetSize), NULL, CMFree);
        bsize = CMCall(binds, GetSize);
        buffers = CMAlloc(sizeof(MYSQL_BIND) * (uint64_t)bsize);
        memset(buffers, 0x0, sizeof(MYSQL_BIND) * (uint64_t)bsize);
	}

    if (mysql_stmt_prepare(stmt, CMCall(query, GetCString),
                           (unsigned long)CMCall(query, GetSize))) {
		MYSQL_LOGERROR(sess, "prepare statement failed.");
		goto FAILEDPOINT;
	}

	// a read only cursor keeps the result set on the server and fetches
	// it in blocks of 'fetchsize' rows. must be set before execution.
	if (fetchsize > 0) {
		unsigned long ctype = (unsigned long)CURSOR_TYPE_READ_ONLY;
		unsigned long prefetch = (unsigned long)fetchsize;
		if (mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, &ctype) ||
				mysql_stmt_attr_set(
					stmt, STMT_ATTR_PREFETCH_ROWS, &prefetch)) {
			MYSQL_LOGERROR(sess, "cannot set fetch size of statement.");
			goto FAILEDPOINT;
		}
	}

	// bind variables.
	for (i=0; i<bsize; i++) {
        CMUTIL_Json *json = CMCall(binds, Get, i);
        if (CMCall(json, GetType) == CMJsonTypeValue) {
			CMUTIL_JsonValue *jval = (CMUTIL_JsonValue*)json;
			// type of json value
            g_cmdbm_mysql_bindprocs[CMCall(jval, GetValueType)](
						&buffers[i], jval, array);
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

	if (mysql_stmt_execute(stmt) != 0) {
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
    MYSQL_BIND *bind;
    CMDBM_MySQLSession *sess;
    void (*fassign)(CMDBM_MySQL_FieldInfo*, MYSQL_STMT*, CMUTIL_JsonObject*);
    int index;
    CMJsonValueType jtype;
    unsigned long length;
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
                    stmt, finfo->bind, (uint32_t)finfo->index, 0);
        if (ival != 0) {
            MYSQL_LOGERROR(finfo->sess, "mysql_stmt_fetch_column() failed.");
        } else {
            CMCall(row, PutString, finfo->name, buffer);
        }
        // reset buffer
        CMFree(buffer);
        bind->buffer = NULL;
        bind->buffer_length = 0;
//		CMCall(row, PutString, finfo->name, (char*)bind->buffer);
//		memset(bind->buffer, 0x0, 4096);
	} else {
        CMCall(row, PutNull, finfo->name);
	}
	CMUTIL_UNUSED(stmt);
}

CMDBM_STATIC void CMDBM_MySQL_ResultAssignBoolean(
		CMDBM_MySQL_FieldInfo *finfo, MYSQL_STMT *stmt, CMUTIL_JsonObject *row)
{
    CMBool bval = finfo->longVal? CMTrue:CMFalse;
	CMUTIL_UNUSED(stmt);
    CMCall(row, PutBoolean, finfo->name, bval);
}

// describes every column of the result set the statement currently stands
// on, and binds a buffer to each. the field descriptions are added to
// 'fields' in column order; the returned bind array belongs to the caller,
// which releases it with CMFree once the result set has been read.
CMDBM_STATIC MYSQL_BIND *CMDBM_MySQL_BindResult(
		CMDBM_MySQLSession *sess, MYSQL_STMT *stmt, MYSQL_RES *meta,
		CMUTIL_Array *fields)
{
	int i, fieldcnt = (int)mysql_num_fields(meta);
	MYSQL_FIELD *ofields = mysql_fetch_fields(meta);
	MYSQL_BIND *resbuf = CMAlloc(sizeof(MYSQL_BIND) * (uint64_t)fieldcnt);
	memset(resbuf, 0x0, sizeof(MYSQL_BIND) * (uint64_t)fieldcnt);
	for (i=0; i<fieldcnt; i++) {
		MYSQL_FIELD *f = &ofields[i];
		MYSQL_BIND *b = &(resbuf[i]);
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
            finfo->jtype = CMJsonValueBoolean;
			break;

		case MYSQL_TYPE_TINY:
		case MYSQL_TYPE_SHORT:
		case MYSQL_TYPE_LONG:
		case MYSQL_TYPE_TIMESTAMP:
		case MYSQL_TYPE_LONGLONG:
		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_ENUM:
			// treat as long
			finfo->fassign = CMDBM_MySQL_ResultAssignLong;
			b->buffer_type = MYSQL_TYPE_LONGLONG;
			b->buffer = &finfo->longVal;
            finfo->jtype = CMJsonValueLong;
			break;

		case MYSQL_TYPE_FLOAT:
		case MYSQL_TYPE_DOUBLE:
		case MYSQL_TYPE_DECIMAL:
		case MYSQL_TYPE_NEWDECIMAL:
			// treat as double
			finfo->fassign = CMDBM_MySQL_ResultAssignDouble;
			b->buffer_type = MYSQL_TYPE_DOUBLE;
			b->buffer = &finfo->doubleVal;
            finfo->jtype = CMJsonValueDouble;
			break;

		default:
			// treat as string
			finfo->fassign = CMDBM_MySQL_ResultAssignString;
			b->buffer_type = MYSQL_TYPE_STRING;
            b->buffer = NULL;
            b->buffer_length = 0;
            finfo->jtype = CMJsonValueString;
			break;
		}
		b->length = &(finfo->length);
		b->is_null = &(finfo->isnull);
		b->error = &(finfo->error);
        finfo->bind = b;
		finfo->sess = sess;
        CMCall(fields, Add, finfo, NULL);
	}
	if (mysql_stmt_bind_result(stmt, resbuf) != 0) {
		MYSQL_LOGERROR(sess, "cannot bind result buffers.");
		// the caller still owns 'fields', whose entries point into the
		// array about to go away. no result buffer has been allocated yet
		// at this point, so cutting the link is enough.
		for (i=0; i<fieldcnt; i++) {
			CMDBM_MySQL_FieldInfo *finfo =
					(CMDBM_MySQL_FieldInfo*)CMCall(fields, GetAt, (uint32_t)i);
			finfo->bind = NULL;
		}
		CMFree(resbuf);
		return NULL;
	}
	return resbuf;
}

CMDBM_STATIC MYSQL_STMT *CMDBM_MySQL_SelectBase(
		CMDBM_MySQLSession *sess, CMUTIL_String *query, CMUTIL_JsonArray *binds,
		CMUTIL_JsonObject *outs, CMUTIL_Array *fields, MYSQL_RES **meta,
		MYSQL_BIND **resbuf, uint32_t fetchsize)
{
	MYSQL_STMT *stmt = NULL;

	// the OUT parameters of a procedure arrive as a result set of their
	// own, which a select would take for its rows: only a statement run
	// through Execute reads them back.
	if (outs) {
		CMUTIL_StringArray *keys = CMCall(outs, GetKeys);
		if (CMCall(keys, GetSize) > 0)
			CMLogWarn("a select statement cannot read OUT parameters back "
					  "on MySQL/MariaDB. run the 'CALL procedure(...)' as "
					  "an insert, update or delete statement instead.");
		CMCall(keys, Destroy);
	}

	stmt = CMDBM_MySQL_ExecuteBase(sess, query, binds, fetchsize);
	if (stmt) {
        CMBool succ = CMFalse;

		// buffering the whole result set would defeat the cursor.
		if (fetchsize == 0 && mysql_stmt_store_result(stmt) != 0) {
			MYSQL_LOGERROR(sess, "execute statement failed.");
			goto FAILEDPOINT;
		}

		*meta = mysql_stmt_result_metadata(stmt);
		if (*meta == NULL) {
			MYSQL_LOGERROR(sess, "cannot get result metadata.");
			goto FAILEDPOINT;
		}

		*resbuf = CMDBM_MySQL_BindResult(sess, stmt, *meta, fields);
		if (*resbuf == NULL)
			goto FAILEDPOINT;

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
    uint32_t i;
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
        if (finfo->jtype == CMJsonValueString &&
                finfo->bind && finfo->bind->buffer)
            CMFree(finfo->bind->buffer);
		CMFree(finfo);
	}
}

// the flag the server raises on the result set which carries the OUT
// parameters. defined by every client library since MySQL 5.5.
#ifndef SERVER_PS_OUT_PARAMS
#define SERVER_PS_OUT_PARAMS 4096
#endif

// an OUT parameter of the statement, and the bind index it sits at.
typedef struct CMDBM_MySQL_OutRef {
	CMUTIL_JsonValue	*value;
	int64_t				index;
} CMDBM_MySQL_OutRef;

// collects the OUT parameters ordered by their bind index. the keys of
// 'outs' are decimal spellings, so they cannot simply be sorted as text.
CMDBM_STATIC CMDBM_MySQL_OutRef *CMDBM_MySQL_SortOutRefs(
		CMUTIL_JsonObject *outs, CMUTIL_StringArray *keys, uint32_t count)
{
	uint32_t i;
	CMDBM_MySQL_OutRef *res =
			CMAlloc(sizeof(CMDBM_MySQL_OutRef) * (size_t)count);
	for (i=0; i<count; i++) {
		const char *cidx = CMCall(keys, GetCString, i);
		CMUTIL_Json *item = CMCall(outs, Get, cidx);
		int64_t idx = (int64_t)strtoll(cidx, NULL, 10);
		uint32_t j = i;
		// insertion sort: a statement has a handful of OUT parameters.
		while (j > 0 && res[j-1].index > idx) {
			res[j] = res[j-1];
			j--;
		}
		res[j].index = idx;
		res[j].value = (CMUTIL_JsonValue*)item;
	}
	return res;
}

CMDBM_STATIC void CMDBM_MySQL_CopyValue(
		CMUTIL_JsonValue *dst, CMUTIL_JsonValue *src)
{
	CMJsonValueType vtype = CMCall(src, GetValueType);
	switch (vtype) {
	case CMJsonValueLong: {
		int64_t lval = CMCall(src, GetLong);
		CMCall(dst, SetLong, lval);
		break;
	}
	case CMJsonValueDouble: {
		double dval = CMCall(src, GetDouble);
		CMCall(dst, SetDouble, dval);
		break;
	}
	case CMJsonValueBoolean: {
		CMBool bval = CMCall(src, GetBoolean);
		CMCall(dst, SetBoolean, bval);
		break;
	}
	case CMJsonValueString: {
		const char *sval = CMCall(src, GetCString);
		CMCall(dst, SetString, sval);
		break;
	}
	default:
		CMCall(dst, SetNull);
		break;
	}
}

// reads the single row of the result set the statement stands on into the
// OUT parameters, column by column.
CMDBM_STATIC void CMDBM_MySQL_AssignOutRow(
		CMDBM_MySQLSession *sess, MYSQL_STMT *stmt, MYSQL_RES *meta,
		CMDBM_MySQL_OutRef *refs, uint32_t count)
{
	CMUTIL_Array *fields = CMUTIL_ArrayCreateEx(
				10, NULL, CMDBM_MySQL_FieldDestroy);
	MYSQL_BIND *resb = CMDBM_MySQL_BindResult(sess, stmt, meta, fields);
	if (resb) {
		int frv = mysql_stmt_fetch(stmt);
		if (frv == 0 || frv == MYSQL_DATA_TRUNCATED) {
			uint32_t i, nfields = (uint32_t)CMCall(fields, GetSize);
			CMUTIL_JsonObject *row = CMUTIL_JsonObjectCreate();
			CMUTIL_MySQL_RowSetFields(fields, stmt, row);
			if (nfields != count)
				CMLogWarn("the statement declares %u OUT parameter(s) while "
						  "the procedure returned %u. the first %u are read "
						  "back.", count, nfields,
						  nfields < count? nfields:count);
			if (nfields > count) nfields = count;
			for (i=0; i<nfields; i++) {
				CMDBM_MySQL_FieldInfo *finfo =
						(CMDBM_MySQL_FieldInfo*)CMCall(fields, GetAt, i);
				CMUTIL_Json *item = CMCall(row, Get, finfo->name);
				if (item)
					CMDBM_MySQL_CopyValue(
								refs[i].value, (CMUTIL_JsonValue*)item);
			}
			CMUTIL_JsonDestroy(row);
		} else {
			MYSQL_LOGERROR(sess, "cannot fetch the OUT parameter row.");
		}
	}
	// the field descriptions point into the bind array, so they have to go
	// first: destroying them releases the buffers those binds hold.
	CMCall(fields, Destroy);
	if (resb) CMFree(resb);
}

// MySQL and MariaDB do not write an OUT parameter back into the buffer it
// was bound from. The server sends the values of the OUT and INOUT
// parameters of a procedure as an extra result set instead - flagged
// SERVER_PS_OUT_PARAMS, one column per parameter in declaration order,
// after whatever result sets the body of the procedure produced. The bind
// indices of the OUT parameters ascend in that same order, so the columns
// of that result set map onto them one by one.
CMDBM_STATIC void CMDBM_MySQL_ReadOutParams(
		CMDBM_MySQLSession *sess, MYSQL_STMT *stmt, CMUTIL_JsonObject *outs)
{
	CMUTIL_StringArray *keys = NULL;
	CMDBM_MySQL_OutRef *refs = NULL;
	CMBool found = CMFalse;
	uint32_t count;
	int nrv;

	if (outs == NULL) return;
	keys = CMCall(outs, GetKeys);
	count = (uint32_t)CMCall(keys, GetSize);
	if (count == 0) {
		CMCall(keys, Destroy);
		return;
	}
	refs = CMDBM_MySQL_SortOutRefs(outs, keys, count);

	do {
		MYSQL_RES *meta = mysql_stmt_result_metadata(stmt);
		if (meta) {
			if (!found &&
					(sess->conn->server_status & SERVER_PS_OUT_PARAMS)) {
				CMDBM_MySQL_AssignOutRow(sess, stmt, meta, refs, count);
				found = CMTrue;
			}
			mysql_free_result(meta);
		}
		// every result set must be consumed before the connection is idle
		// again, whether it held the OUT parameters or not.
		mysql_stmt_free_result(stmt);
		nrv = mysql_stmt_next_result(stmt);
	} while (nrv == 0);

	if (nrv > 0)
		MYSQL_LOGERROR(sess, "reading the results of the statement failed.");
	else if (!found)
		CMLogWarn("the statement declares %u OUT parameter(s) but the "
				  "server sent no OUT parameter result set. MySQL and "
				  "MariaDB return them from 'CALL procedure(...)' only.",
				  count);

	CMFree(refs);
	CMCall(keys, Destroy);
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
				sess, query, binds, outs, fields, &meta, &resb, 0);
	CMUTIL_JsonObject *res = NULL;
    CMBool succ = CMFalse;

	if (stmt) {
		// MYSQL_DATA_TRUNCATED is expected for string columns:
		// they are bound with a zero-length buffer and fetched
		// afterwards with mysql_stmt_fetch_column().
		int frv = mysql_stmt_fetch(stmt);
		if (frv != 0 && frv != MYSQL_DATA_TRUNCATED) {
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
				sess, query, binds, outs, fields, &meta, &resb, 0);
	CMUTIL_JsonArray *res = CMUTIL_JsonArrayCreate();
    CMBool succ = CMFalse;

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
	MYSQL_STMT *stmt = CMDBM_MySQL_ExecuteBase(sess, query, binds, 0);
	if (stmt) {
		// a statement whose affected row count is unknown - 'CALL' is the
		// usual one - reports it as ~0. that must not be handed back as
		// the -1 which means the execution failed.
		unsigned long long arows = mysql_stmt_affected_rows(stmt);
		int res = (arows == ~0ULL)? 0:(int)arows;
		CMDBM_MySQL_ReadOutParams(sess, stmt, outs);
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
		CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs,
		uint32_t fetchsize)
{
	CMDBM_MySQLSession *sess = (CMDBM_MySQLSession*)connection;
	CMUTIL_Array *fields = CMUTIL_ArrayCreateEx(
				10, NULL, CMDBM_MySQL_FieldDestroy);
	MYSQL_RES *meta = NULL;
	MYSQL_BIND *resb = NULL;
	MYSQL_STMT *stmt = CMDBM_MySQL_SelectBase(
				sess, query, binds, outs, fields, &meta, &resb, fetchsize);
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
		int frv = mysql_stmt_fetch(csr->stmt);
		if (frv == 0 || frv == MYSQL_DATA_TRUNCATED) {
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
	// mysql_library_end() is deliberately not called: the client library
	// is not meant to be initialized again afterwards, while a datasource
	// may well be created again after the last one has been destroyed.
	// the library releases its global state at process exit.
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


#include "functions.h"

#ifdef CMDBM_ORACLE

CMUTIL_LogDefine("cmdbm.module.oracle")

#include <oci.h>

CMDBM_STATIC void CMDBM_Oracle_LibraryInit()
{
	// oracle does nothing
}

CMDBM_STATIC void CMDBM_Oracle_LibraryClear()
{
	// oracle does nothing
}

typedef struct CMDBM_OracleCtx {
	OCIEnv			*envhp;
	CMUTIL_Mutex	*convmtx;
	CMUTIL_CSConv	*conv;
    CMBool		needconv;
    int             dummy_padder;
} CMDBM_OracleCtx;

typedef struct CMDBM_OracleSession {
	CMDBM_OracleCtx	*ctx;
	OCIEnv			*envhp;
	OCIError		*errhp;
	OCIServer		*srvhp;
	OCISvcCtx		*svchp;
	OCISession		*authp;
    CMBool		autocommit;
    int             dummy_padder;
} CMDBM_OracleSession;

#define CMDBM_OracleCheck(c,s,b,a,...) do {\
	s = (a)(__VA_ARGS__);\
	unsigned char errbuf[2048] = {0,};\
	sb4 errcode = 0;\
	CMUTIL_String *buf = NULL, *out = NULL;\
	switch (s) {\
	case OCI_SUCCESS: break;\
	case OCI_SUCCESS_WITH_INFO:\
		(void) OCIErrorGet((c)->errhp, (ub4) 1, NULL, &errcode,\
						 errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);\
		if ((c)->ctx->needconv) {\
			buf = CMUTIL_StringCreateEx(64, (char*)errbuf);\
            CMCall((c)->ctx->convmtx, Lock);\
            out = CMCall((c)->ctx->conv, Forward, buf);\
            CMCall((c)->ctx->convmtx, Unlock);\
			CMLogInfo("OCI success with information: ORA-%05d > %s",\
                      errcode, CMCall(out, GetCString));\
            CMCall(buf, Destroy); CMCall(out, Destroy);\
		} else {\
			CMLogInfo("OCI success with information: ORA-%05d > %s",\
					  errcode, errbuf);\
		} break;\
	case OCI_NEED_DATA: break;\
	case OCI_NO_DATA: break;\
	case OCI_ERROR:\
		(void) OCIErrorGet(c->errhp, (ub4) 1, NULL, &errcode,\
						 errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);\
		if (c->ctx->needconv) {\
			buf = CMUTIL_StringCreateEx(64, (char*)errbuf);\
            CMCall(c->ctx->convmtx, Lock);\
            out = CMCall(c->ctx->conv, Forward, buf);\
            CMCall(c->ctx->convmtx, Unlock);\
			CMLogError("%s failed. OCI error: ORA-%05d > %s",\
                       #a, errcode, CMCall(out, GetCString));\
            CMCall(buf, Destroy); CMCall(out, Destroy);\
		} else {\
			CMLogError("%s failed. OCI error: ORA-%05d > %s",\
					   #a, errcode, errbuf);\
		} goto b;\
	case OCI_INVALID_HANDLE:\
		CMLogError("OCI returned OCI_INVALID_HANDLE");\
		goto b;\
	case OCI_STILL_EXECUTING:\
		CMLogError("OCI returned OCI_STILL_EXECUTING");\
		goto b;\
	case OCI_CONTINUE:\
		CMLogError("OCI returned OCI_CONTINUE");\
		goto b;\
	default:\
		CMLogError("OCI returned unknown error: %d", status);\
		goto b;\
	}\
} while(0)

CMDBM_STATIC const char *CMDBM_Oracle_GetDBMSKey()
{
	return "ORACLE";
}

CMDBM_STATIC void *CMDBM_Oracle_Initialize(const char *dbcs, const char *prcs)
{
	CMDBM_OracleCtx *res = NULL;
	OCIEnv *env = NULL;
	sword rval = OCIEnvCreate(&env, OCI_DEFAULT | OCI_THREADED,
							  NULL, NULL, NULL, NULL, 0, NULL);
	if (rval != OCI_SUCCESS) {
		CMLogErrorS("OCIEnvCreate() failed with error code: %d", rval);
		env = NULL;
	} else {
		res = CMAlloc(sizeof(CMDBM_OracleCtx));
		memset(res, 0x0, sizeof(CMDBM_OracleCtx));
		res->envhp = env;
		if (strcasecmp(dbcs, prcs) != 0) {
			res->needconv = CMTrue;
			res->conv = CMUTIL_CSConvCreate(dbcs, prcs);
			res->convmtx = CMUTIL_MutexCreate();
		}
	}
	return env;
}

CMDBM_STATIC void CMDBM_Oracle_CleanUp(void *initres)
{
	if (initres) {
		CMDBM_OracleCtx *ctx = (CMDBM_OracleCtx*)initres;
		OCIHandleFree(ctx->envhp, OCI_HTYPE_ENV);
        if (ctx->convmtx) CMCall(ctx->convmtx, Destroy);
        if (ctx->conv) CMCall(ctx->conv, Destroy);
		CMFree(ctx);
	}
}

CMDBM_STATIC char *CMDBM_Oracle_GetBindString(
        void *initres, uint32_t index, char *buffer)
{
	CMUTIL_UNUSED(initres);
    sprintf(buffer, ":%u", (index+1));
	return buffer;
}

CMDBM_STATIC const char *CMDBM_Oracle_GetTestQuery()
{
	return "select 1 from dual";
}

CMDBM_STATIC void CMDBM_Oracle_CloseConnection(
		void *initres, void *connection)
{
	CMDBM_OracleSession *ctx = (CMDBM_OracleSession*)connection;
	if (ctx) {
		if (ctx->authp) {
			 OCISessionEnd(ctx->svchp, ctx->errhp, ctx->authp, OCI_DEFAULT);
			 OCIServerDetach(ctx->srvhp, ctx->errhp, OCI_DEFAULT);
			 OCIHandleFree(ctx->authp, OCI_HTYPE_SESSION);
		 }
		 if (ctx->svchp)
			 OCIHandleFree(ctx->svchp, OCI_HTYPE_SVCCTX);
		 if (ctx->srvhp)
			 OCIHandleFree(ctx->srvhp, OCI_HTYPE_SERVER);
		 if (ctx->errhp)
			 OCIHandleFree(ctx->errhp, OCI_HTYPE_ERROR);
		CMFree(ctx);
	}
	CMUTIL_UNUSED(initres);
}

CMDBM_STATIC void *CMDBM_Oracle_OpenConnection(
		void *initres, CMUTIL_JsonObject *params)
{
    CMBool succ = CMFalse;
	CMUTIL_JsonValue *user =
            (CMUTIL_JsonValue*)CMCall(params, GetString, "user");
	CMUTIL_JsonValue *pass =
            (CMUTIL_JsonValue*)CMCall(params, GetString, "password");
	CMUTIL_JsonValue *tns  =
            (CMUTIL_JsonValue*)CMCall(params, GetString, "tnsname");
	CMDBM_OracleSession *res = CMAlloc(sizeof(CMDBM_OracleSession));
	CMDBM_OracleCtx *ctx = (CMDBM_OracleCtx*)initres;
	const char *suser, *spass, *stns;
	sb4 status;

	if (!user || !pass || !tns) {
		CMLogError("Oracle connection requires 'tnsname', 'user', 'password'");
		goto ENDPOINT;
	}
    suser = CMCall(user, GetCString);
    spass = CMCall(pass, GetCString);
    stns  = CMCall(tns , GetCString);

	memset(res, 0x0, sizeof(CMDBM_OracleSession));
	res->envhp = ctx->envhp;
	res->ctx = ctx;
	if (OCIHandleAlloc(res->envhp, (void**)&(res->errhp),
					   OCI_HTYPE_ERROR, 0, NULL) != OCI_SUCCESS) {
		CMLogError("OCI error context allocation failed.");
		goto ENDPOINT;
	}

	CMDBM_OracleCheck(res, status, ENDPOINT, OCIHandleAlloc, res->envhp,
					  (void**)&(res->srvhp), OCI_HTYPE_SERVER, 0, NULL);
	CMDBM_OracleCheck(res, status, ENDPOINT, OCIHandleAlloc, res->envhp,
					  (void**)&(res->svchp), OCI_HTYPE_SVCCTX, 0, NULL);
	CMDBM_OracleCheck(res, status, ENDPOINT, OCIServerAttach, res->srvhp,
					  res->errhp, (OraText*)stns, (sb4)strlen(stns),
					  OCI_DEFAULT);
	CMDBM_OracleCheck(res, status, ENDPOINT, OCIAttrSet, res->svchp,
					  OCI_HTYPE_SVCCTX, res->srvhp, 0, OCI_ATTR_SERVER,
					  res->errhp);
	CMDBM_OracleCheck(res, status, ENDPOINT, OCIHandleAlloc, res->envhp,
					  (void**)&(res->authp), OCI_HTYPE_SESSION, 0, NULL);
	CMDBM_OracleCheck(res, status, ENDPOINT, OCIAttrSet, res->authp,
					  OCI_HTYPE_SESSION, (void*)suser, (ub4)strlen(suser),
					  OCI_ATTR_USERNAME, res->errhp);
	CMDBM_OracleCheck(res, status, ENDPOINT, OCIAttrSet, res->authp,
					  OCI_HTYPE_SESSION, (void*)spass, (ub4)strlen(spass),
					  OCI_ATTR_PASSWORD, res->errhp);
	CMDBM_OracleCheck(res, status, ENDPOINT, OCISessionBegin, res->svchp,
					  res->errhp, res->authp, OCI_CRED_RDBMS, OCI_DEFAULT);
	CMDBM_OracleCheck(res, status, ENDPOINT, OCIAttrSet, res->svchp,
					  OCI_HTYPE_SVCCTX, res->authp, 0, OCI_ATTR_SESSION,
					  res->errhp);

	succ = CMTrue;
ENDPOINT:
	if (!succ) {
		CMDBM_Oracle_CloseConnection(NULL, res);
		res = NULL;
	}
	return res;
}

CMDBM_STATIC CMBool CMDBM_Oracle_StartTransaction(
		void *initres, void *connection)
{
	CMDBM_OracleSession *conn = (CMDBM_OracleSession*)connection;
	conn->autocommit = CMFalse;
	CMUTIL_UNUSED(initres);
	return CMTrue;
}

CMDBM_STATIC void CMDBM_Oracle_EndTransaction(
		void *initres, void *connection)
{
	CMDBM_OracleSession *conn = (CMDBM_OracleSession*)connection;
	conn->autocommit = CMTrue;
	CMUTIL_UNUSED(initres);
}

CMDBM_STATIC CMBool CMDBM_Oracle_CommitTransaction(
		void *initres, void *connection)
{
    CMBool res = CMFalse;
	sb4 status;
	CMDBM_OracleSession *conn = (CMDBM_OracleSession*)connection;
	CMDBM_OracleCheck(conn, status, ENDPOINT, OCITransCommit,
					  conn->svchp, conn->errhp, 0);
	res = CMTrue;
ENDPOINT:
	CMUTIL_UNUSED(initres);
	return res;
}

CMDBM_STATIC void CMDBM_Oracle_RollbackTransaction(
		void *initres, void *connection)
{
	CMDBM_OracleSession *conn = (CMDBM_OracleSession*)connection;
	sb4 status;
	CMDBM_OracleCheck(conn, status, ENDPOINT, OCITransRollback,
					  conn->svchp, conn->errhp, 0);
ENDPOINT:
	CMUTIL_UNUSED(initres);
}

typedef struct CMDBM_OracleColumn {
    char        *name;
    void        *buffer;
    OCIDefine   *define;
    uint32_t    index;
    uint32_t    bufsz;
    int         indicator;
    ub2         typecd;
    short       dummy_padder;
} CMDBM_OracleColumn;

CMDBM_STATIC CMBool CMDBM_Oracle_BindLong(
		CMDBM_OracleSession *conn, OCIBind **bind,
		OCIStmt *stmt, CMUTIL_JsonValue *jval,
        uint32_t pos, CMUTIL_Array *bufarr, CMUTIL_Json *out,
		CMUTIL_Array *outarr)
{
    int64_t *val = CMAlloc(sizeof(int64_t));
	sb4 status;
    *val = CMCall(jval, GetLong);
    CMCall(bufarr, Add, val);
	if (out) {
		CMDBM_OracleColumn *item = CMAlloc(sizeof(CMDBM_OracleColumn));
		memset(item, 0x0, sizeof(sizeof(CMDBM_OracleColumn)));
		item->typecd = CMUTIL_JsonValueLong;
		item->index = pos;
		item->buffer = val;
        CMCall(outarr, Add, item);
		CMDBM_OracleCheck(conn, status, ENDPOINT, OCIBindByPos,
                          stmt, bind, conn->errhp, pos+1, val, sizeof(int64_t),
						  SQLT_INT, &(item->indicator),0,0,0,0,OCI_DEFAULT);
	} else {
		CMDBM_OracleCheck(conn, status, ENDPOINT, OCIBindByPos,
                          stmt, bind, conn->errhp, pos+1, val, sizeof(int64_t),
						  SQLT_INT, 0,0,0,0,0,OCI_DEFAULT);
	}
	return CMTrue;
ENDPOINT:
	return CMFalse;
}

CMDBM_STATIC CMBool CMDBM_Oracle_BindDouble(
		CMDBM_OracleSession *conn, OCIBind **bind,
		OCIStmt *stmt, CMUTIL_JsonValue *jval,
        uint32_t pos, CMUTIL_Array *bufarr, CMUTIL_Json *out,
		CMUTIL_Array *outarr)
{
	double *val = CMAlloc(sizeof(double));
	sb4 status;
    *val = CMCall(jval, GetDouble);
    CMCall(bufarr, Add, val);
	if (out) {
		CMDBM_OracleColumn *item = CMAlloc(sizeof(CMDBM_OracleColumn));
		memset(item, 0x0, sizeof(sizeof(CMDBM_OracleColumn)));
		item->typecd = CMUTIL_JsonValueDouble;
		item->index = pos;
		item->buffer = val;
        CMCall(outarr, Add, item);
		CMDBM_OracleCheck(conn, status, ENDPOINT, OCIBindByPos,
						  stmt, bind, conn->errhp, pos+1, val, sizeof(double),
						  SQLT_FLT, &(item->indicator),0,0,0,0,OCI_DEFAULT);
	} else {
		CMDBM_OracleCheck(conn, status, ENDPOINT, OCIBindByPos,
						  stmt, bind, conn->errhp, pos+1, val, sizeof(double),
						  SQLT_FLT, 0,0,0,0,0,OCI_DEFAULT);
	}
	return CMTrue;
ENDPOINT:
	return CMFalse;
}

CMDBM_STATIC CMBool CMDBM_Oracle_BindString(
		CMDBM_OracleSession *conn, OCIBind **bind,
		OCIStmt *stmt, CMUTIL_JsonValue *jval,
        uint32_t pos, CMUTIL_Array *bufarr, CMUTIL_Json *out,
		CMUTIL_Array *outarr)
{
	char buf[4096] = {0,};
	sb4 status;
    CMUTIL_String *val = CMCall(jval, GetString);
	if (out) {
		CMDBM_OracleColumn *item = CMAlloc(sizeof(CMDBM_OracleColumn));
		memset(item, 0x0, sizeof(sizeof(CMDBM_OracleColumn)));
		item->typecd = CMUTIL_JsonValueString;
		item->index = pos;
        CMCall(val, AddNString, buf, 4096);
        CMCall(outarr, Add, item);
		CMDBM_OracleCheck(conn, status, ENDPOINT, OCIBindByPos,
						  stmt, bind, conn->errhp, pos+1,
                          (void*)CMCall(val, GetCString),
                          (int)CMCall(val, GetSize),
						  SQLT_STR, &(item->indicator),0,0,0,0,OCI_DEFAULT);
	} else {
		CMDBM_OracleCheck(conn, status, ENDPOINT, OCIBindByPos,
						  stmt, bind, conn->errhp, pos+1,
                          (void*)CMCall(val, GetCString),
                          (int)CMCall(val, GetSize),
						  SQLT_STR, 0,0,0,0,0,OCI_DEFAULT);
	}
	return CMTrue;
ENDPOINT:
	CMUTIL_UNUSED(bufarr);
	return CMFalse;
}

CMDBM_STATIC CMBool CMDBM_Oracle_BindBoolean(
		CMDBM_OracleSession *conn, OCIBind **bind,
		OCIStmt *stmt, CMUTIL_JsonValue *jval,
        uint32_t pos, CMUTIL_Array *bufarr, CMUTIL_Json *out,
		CMUTIL_Array *outarr)
{
	int *val = CMAlloc(sizeof(int));
	sb4 status;
    *val = CMCall(jval, GetBoolean);
    CMCall(bufarr, Add, val);
	if (out) {
		CMDBM_OracleColumn *item = CMAlloc(sizeof(CMDBM_OracleColumn));
		memset(item, 0x0, sizeof(sizeof(CMDBM_OracleColumn)));
		item->typecd = CMUTIL_JsonValueBoolean;
		item->index = pos;
		item->buffer = val;
        CMCall(outarr, Add, item);
		CMDBM_OracleCheck(conn, status, ENDPOINT, OCIBindByPos,
						  stmt, bind, conn->errhp, pos+1, val, sizeof(int),
						  SQLT_INT, &(item->indicator),0,0,0,0,OCI_DEFAULT);
	} else {
		CMDBM_OracleCheck(conn, status, ENDPOINT, OCIBindByPos,
						  stmt, bind, conn->errhp, pos+1, val, sizeof(int),
						  SQLT_INT, 0,0,0,0,0,OCI_DEFAULT);
	}
	return CMTrue;
ENDPOINT:
	return CMFalse;
}

CMDBM_STATIC CMBool CMDBM_Oracle_BindNull(
		CMDBM_OracleSession *conn, OCIBind **bind,
		OCIStmt *stmt, CMUTIL_JsonValue *jval,
        uint32_t pos, CMUTIL_Array *bufarr, CMUTIL_Json *out,
		CMUTIL_Array *outarr)
{
	sb4 status, ind = -1;
	CMDBM_OracleCheck(conn, status, ENDPOINT, OCIBindByPos,
					  stmt, bind, conn->errhp, pos+1, NULL, 0,
					  SQLT_STR, &ind,0,0,0,0,OCI_DEFAULT);
	return CMTrue;
ENDPOINT:
	CMUTIL_UNUSED(jval, bufarr, out, outarr);
	return CMFalse;
}

typedef CMBool (*CMDBM_Oracle_BindProc)(
		CMDBM_OracleSession *conn, OCIBind **bind,
		OCIStmt *stmt, CMUTIL_JsonValue *jval,
        uint32_t pos, CMUTIL_Array *bufarr, CMUTIL_Json *out,
		CMUTIL_Array *outarr);
static CMDBM_Oracle_BindProc g_cmdbm_oracle_bindprocs[]={
	CMDBM_Oracle_BindLong,
	CMDBM_Oracle_BindDouble,
	CMDBM_Oracle_BindString,
	CMDBM_Oracle_BindBoolean,
	CMDBM_Oracle_BindNull
};

CMDBM_STATIC void CMDBM_Oracle_AllocBuffer(
		CMDBM_OracleColumn *col)
{
	switch (col->typecd) {
	case SQLT_INT:
	case SQLT_UIN:
		// treat as long
        col->bufsz = (int)sizeof(int64_t);
		col->typecd = SQLT_INT;
		break;
	case SQLT_FLT:
		// treat as double
		col->bufsz = (int)sizeof(double);
		col->typecd = SQLT_FLT;
		break;
	case SQLT_CLOB:
	case SQLT_BLOB:
		// treat as string
		col->bufsz = 9999;
		col->typecd = SQLT_STR;
		break;
	default:
		// treat as string
		col->bufsz = 4000;
		col->typecd = SQLT_STR;
	}
	col->buffer = CMAlloc(col->bufsz);
}

CMDBM_STATIC void CMDBM_OracleColumnDestroy(void *data)
{
	CMDBM_OracleColumn *col = (CMDBM_OracleColumn*)data;
	if (col) {
		if (col->define) OCIHandleFree(col->define, OCI_HTYPE_DEFINE);
		if (col->buffer) CMFree(col->buffer);
		if (col->name  ) CMFree(col->name);
		CMFree(col);
	}
}

CMDBM_STATIC void CMDBM_Oracle_SetOutValue(
		CMDBM_OracleColumn *col, CMUTIL_JsonValue *jval)
{
	if (col->indicator == -1) {
        CMCall(jval, SetNull);
	} else {
        size_t len;
		CMUTIL_String *temp;
		switch (col->typecd) {
		case CMUTIL_JsonValueBoolean:
            CMCall(jval, SetBoolean, (CMBool)*((int*)col->buffer));
			break;
		case CMUTIL_JsonValueLong:
            CMCall(jval, SetLong, (int64_t)*((int64_t*)col->buffer));
			break;
		case CMUTIL_JsonValueDouble:
            CMCall(jval, SetDouble, (double)*((double*)col->buffer));
			break;
		case CMUTIL_JsonValueString:
            temp = CMCall(jval, GetString);
            len = strlen(CMCall(temp, GetCString));
            CMCall(temp, CutTailOff, len);
			break;
		}
	}
}

CMDBM_STATIC OCIStmt *CMDBM_Oracle_ExecuteBase(
		CMDBM_OracleSession *conn, CMUTIL_String *query,
		CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    uint32_t i;
    size_t bsize;
	sb4 status;
    CMBool succ = CMFalse;
	OCIStmt *stmt = NULL;
	OCIBind **buffers = NULL;
	CMUTIL_Array *array = CMUTIL_ArrayCreateEx(
                CMCall(binds, GetSize), NULL, CMFree);
	CMUTIL_Array *outarr = CMUTIL_ArrayCreateEx(
                CMCall(binds, GetSize), NULL, CMFree);

	CMDBM_OracleCheck(conn, status, FAILEDPOINT, OCIHandleAlloc, conn->envhp,
					  (void**)&stmt, OCI_HTYPE_STMT, 0, NULL);
	CMDBM_OracleCheck(conn, status, FAILEDPOINT, OCIStmtPrepare,
					  stmt, conn->errhp,
                      (text*)CMCall(query, GetCString),
                      (ub4)CMCall(query, GetSize),
					  OCI_NTV_SYNTAX, OCI_DEFAULT);

    bsize = CMCall(binds, GetSize);
	buffers = CMAlloc(sizeof(OCIBind*) * bsize);
	memset(buffers, 0x0, sizeof(OCIBind*) * bsize);
	for (i=0; i<bsize; i++) {
		char ibuf[20];
        CMUTIL_Json *json = CMCall(binds, Get, i);
		sprintf(ibuf, "%d", i);
        if (CMCall(json, GetType) == CMUTIL_JsonTypeValue) {
			CMUTIL_JsonValue *jval = (CMUTIL_JsonValue*)json;
            CMUTIL_Json *out = CMCall(outs, Get, ibuf);
			// type of json value
			succ = g_cmdbm_oracle_bindprocs[
                    CMCall(jval, GetValueType)](
						conn, &buffers[i], stmt, jval, i, array, out, outarr);
			if (!succ) {
				goto FAILEDPOINT;
			}
		} else {
			CMLogError("binding variable is not value type JSON.");
			goto FAILEDPOINT;
		}
	}

	// execute statement
	CMDBM_OracleCheck(conn, status, FAILEDPOINT, OCIStmtExecute,
					  conn->svchp, stmt, conn->errhp, 1, 0,0,0, OCI_DEFAULT);

	// retreive out variables
	{
        CMUTIL_StringArray *keys = CMCall(outs, GetKeys);
        for (i=0; i<CMCall(keys, GetSize); i++) {
            const char *cidx = CMCall(keys, GetCString, i);
			CMUTIL_JsonValue *jval =
                    (CMUTIL_JsonValue*)CMCall(outs, Get, cidx);
            uint32_t idx = (uint32_t)atoi(cidx);
			CMDBM_OracleColumn *col =
                    (CMDBM_OracleColumn*)CMCall(outarr, GetAt, idx);
			CMDBM_Oracle_SetOutValue(col, jval);
		}
	}

	succ = CMTrue;
FAILEDPOINT:
	if (!succ) {
		if (stmt) {
			OCIHandleFree(stmt, OCI_HTYPE_STMT);
			stmt = NULL;
		}
	}

	if (buffers) {
		for (i=0; i<bsize; i++)
			if (buffers[i]) OCIHandleFree(buffers[i], OCI_HTYPE_BIND);
		CMFree(buffers);
	}

	if (outarr)
        CMCall(outarr, Destroy);
	if (array)
        CMCall(array, Destroy);
	return stmt;
}

CMDBM_STATIC OCIStmt *CMDBM_Oracle_SelectBase(
		CMDBM_OracleSession *conn, CMUTIL_String *query,
		CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs,
		CMUTIL_Array *outcols)
{
    CMBool succ = CMFalse;
	ub4 j, colcnt;
	sb4 status;
	OCIStmt *stmt = CMDBM_Oracle_ExecuteBase(conn, query, binds, outs);

	if (!stmt) goto FAILEDPOINT;

	// how many columns to be retreived.
	CMDBM_OracleCheck(conn, status, FAILEDPOINT, OCIAttrGet,
					  stmt, OCI_HTYPE_STMT, &colcnt, 0,
					  OCI_ATTR_PARAM_COUNT, conn->errhp);

	for (j=0; j<colcnt; j++) {
		OCIParam *param = NULL;
		text *colname = NULL;
		ub4 namelen = 0;
		ub2 typecd = 0;
		CMDBM_OracleColumn *column;

		// get column parameter reference.
		CMDBM_OracleCheck(conn, status, FAILEDPOINT, OCIParamGet,
						  stmt, OCI_HTYPE_STMT, conn->errhp,
						  (void**)&param, j+1);

		// get column name
		CMDBM_OracleCheck(conn, status, FAILEDPOINT, OCIAttrGet,
						  param, OCI_DTYPE_PARAM, &colname, &namelen,
						  OCI_ATTR_NAME, conn->errhp);

		// get column type
		CMDBM_OracleCheck(conn, status, FAILEDPOINT, OCIAttrGet,
						  param, OCI_DTYPE_PARAM, &typecd, 0,
						  OCI_ATTR_DATA_TYPE, conn->errhp);

		column = CMAlloc(sizeof(CMDBM_OracleColumn));
		memset(column, 0x0, sizeof(CMDBM_OracleColumn));
		column->index = j;
		column->name = CMAlloc(namelen+1);
		memcpy(column->name, colname, namelen);
		column->name[namelen] = 0x0;
		column->typecd = typecd;

		// allocate buffer for result define.
		CMDBM_Oracle_AllocBuffer(column);

		// set define.
		CMDBM_OracleCheck(conn, status, FAILEDPOINT, OCIDefineByPos,
						  stmt, &(column->define), conn->errhp, j+1,
						  column->buffer, (sb4)column->bufsz,
						  column->typecd, &(column->indicator),
						  0, 0, OCI_DEFAULT);

        CMCall(outcols, Add, column);
	}
	succ = CMTrue;
FAILEDPOINT:
	if (!succ && stmt) {
		OCIHandleFree(stmt, OCI_HTYPE_STMT);
		stmt = NULL;
	}
	return stmt;
}

CMDBM_STATIC CMUTIL_JsonObject *CMDBM_Oracle_FetchRow(
		CMDBM_OracleSession *conn, OCIStmt *stmt, CMUTIL_Array *cols)
{
    uint32_t i;
	sb4 status;
	CMUTIL_JsonObject *res = NULL;
	CMDBM_OracleCheck(conn, status, FAILEDPOINT, OCIStmtFetch,
					  stmt, conn->errhp, 1, OCI_FETCH_NEXT, OCI_DEFAULT);
	if (status != OCI_NO_DATA && status != OCI_NEED_DATA) {
		res = CMUTIL_JsonObjectCreate();
        for (i=0; i<CMCall(cols, GetSize); i++) {
			CMDBM_OracleColumn *col =
                    (CMDBM_OracleColumn*)CMCall(cols, GetAt, i);
			switch(col->typecd) {
			case SQLT_INT:
                CMCall(res, PutLong, col->name,
                            (int64_t)*((int64_t*)col->buffer));
				break;
			case SQLT_FLT:
                CMCall(res, PutDouble, col->name,
							(double)*((double*)col->buffer));
				break;
			default:
                CMCall(res, PutString, col->name, col->buffer);
				// must be reset for next call
				memset(col->buffer, 0x0, col->bufsz);
			}
		}
	}
FAILEDPOINT:
	return res;
}

CMDBM_STATIC CMUTIL_JsonObject *CMDBM_Oracle_GetRow(
		void *initres, void *connection,
		CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
	CMDBM_OracleSession *conn = (CMDBM_OracleSession*)connection;
	CMUTIL_Array *outcols = CMUTIL_ArrayCreateEx(
				10, NULL, CMDBM_OracleColumnDestroy);
	OCIStmt *stmt = CMDBM_Oracle_SelectBase(conn, query, binds, outs, outcols);
	CMUTIL_JsonObject *res = CMDBM_Oracle_FetchRow(conn, stmt, outcols);
	if (res == NULL)
        CMLogError("cannot fetch row.\n%s", CMCall(query, GetCString));

    CMCall(outcols, Destroy);
	if (stmt) OCIHandleFree(stmt, OCI_HTYPE_STMT);
	CMUTIL_UNUSED(initres);
	return res;
}

CMDBM_STATIC CMUTIL_JsonValue *CMDBM_Oracle_GetOneValue(
		void *initres, void *connection,
		CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
	CMUTIL_JsonValue *res = NULL;
	CMUTIL_JsonObject *row =
			CMDBM_Oracle_GetRow(initres, connection, query, binds, outs);
	if (row) {
        CMUTIL_StringArray *keys = CMCall(row, GetKeys);
        if (CMCall(keys, GetSize) > 0) {
            const char *key = CMCall(keys, GetCString, 0);
            res = (CMUTIL_JsonValue*)CMCall(row, Remove, key);
		} else {
			CMLogError("row does not contain any fields.\n%s",
                       CMCall(query, GetCString));
		}
        CMCall(keys, Destroy);
		CMUTIL_JsonDestroy(row);
	}
	return res;
}

CMDBM_STATIC CMUTIL_JsonArray *CMDBM_Oracle_GetList(
		void *initres, void *connection,
		CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
	CMDBM_OracleSession *conn = (CMDBM_OracleSession*)connection;
	CMUTIL_Array *outcols = CMUTIL_ArrayCreateEx(
				10, NULL, CMDBM_OracleColumnDestroy);
	OCIStmt *stmt = CMDBM_Oracle_SelectBase(conn, query, binds, outs, outcols);
	CMUTIL_JsonObject *row = NULL;
	CMUTIL_JsonArray *res = CMUTIL_JsonArrayCreate();

	while ((row = CMDBM_Oracle_FetchRow(conn, stmt, outcols)) != NULL)
        CMCall(res, Add, (CMUTIL_Json*)row);

    CMCall(outcols, Destroy);
	if (stmt) OCIHandleFree(stmt, OCI_HTYPE_STMT);
	CMUTIL_UNUSED(initres);
	return res;
}

CMDBM_STATIC int CMDBM_Oracle_Execute(
		void *initres, void *connection,
		CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
	int res = -1;
	sb4 status;
	CMDBM_OracleSession *conn = (CMDBM_OracleSession*)connection;
	OCIStmt *stmt = CMDBM_Oracle_ExecuteBase(conn, query, binds, outs);

	if (!stmt) goto FAILEDPOINT;
	CMDBM_OracleCheck(conn, status, FAILEDPOINT, OCIAttrGet,
					  stmt, OCI_HTYPE_STMT, &res, 0, OCI_ATTR_ROW_COUNT,
					  conn->errhp);

FAILEDPOINT:
	if (stmt) OCIHandleFree(stmt, OCI_HTYPE_STMT);
	CMUTIL_UNUSED(initres);
	return res;
}

typedef struct CMDBM_Oracle_Cursor {
	CMDBM_OracleSession	*conn;
	OCIStmt				*stmt;
	CMUTIL_Array		*outcols;
    CMBool			isend;
    int                 dummy_padder;
} CMDBM_Oracle_Cursor;

CMDBM_STATIC void *CMDBM_Oracle_OpenCursor(
		void *initres, void *connection,
		CMUTIL_String *query, CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
	CMDBM_Oracle_Cursor *res = NULL;
	CMDBM_OracleSession *conn = (CMDBM_OracleSession*)connection;
	CMUTIL_Array *outcols = CMUTIL_ArrayCreateEx(
				10, NULL, CMDBM_OracleColumnDestroy);
	OCIStmt *stmt = CMDBM_Oracle_SelectBase(conn, query, binds, outs, outcols);

	if (stmt) {
		res = CMAlloc(sizeof(CMDBM_Oracle_Cursor));
		memset(res, 0x0, sizeof(CMDBM_Oracle_Cursor));
		res->conn = conn;
		res->outcols = outcols;
		res->stmt = stmt;
	} else {
        CMCall(outcols, Destroy);
	}
	CMUTIL_UNUSED(initres);
	return res;
}

CMDBM_STATIC void CMDBM_Oracle_CloseCursor(void *cursor)
{
	CMDBM_Oracle_Cursor *csr = (CMDBM_Oracle_Cursor*)cursor;
	if (csr) {
        if (csr->outcols) CMCall(csr->outcols, Destroy);
		if (csr->stmt) OCIHandleFree(csr->stmt, OCI_HTYPE_STMT);
		CMFree(csr);
	}
}

CMDBM_STATIC CMUTIL_JsonObject *CMDBM_Oracle_CursorNextRow(void *cursor)
{
	CMDBM_Oracle_Cursor *csr = (CMDBM_Oracle_Cursor*)cursor;
	if (!csr->isend) {
		CMUTIL_JsonObject *res =
				CMDBM_Oracle_FetchRow(csr->conn, csr->stmt, csr->outcols);
		if (res == NULL)
			csr->isend = CMTrue;
	}
	return NULL;
}

CMDBM_ModuleInterface g_cmdbm_oracle_interface = {
	CMDBM_Oracle_LibraryInit,
	CMDBM_Oracle_LibraryClear,
	CMDBM_Oracle_GetDBMSKey,
	CMDBM_Oracle_Initialize,
	CMDBM_Oracle_CleanUp,
	CMDBM_Oracle_GetBindString,
	CMDBM_Oracle_GetTestQuery,
	CMDBM_Oracle_OpenConnection,
	CMDBM_Oracle_CloseConnection,
	CMDBM_Oracle_StartTransaction,
	CMDBM_Oracle_EndTransaction,
	CMDBM_Oracle_CommitTransaction,
	CMDBM_Oracle_RollbackTransaction,
	CMDBM_Oracle_GetOneValue,
	CMDBM_Oracle_GetRow,
	CMDBM_Oracle_GetList,
	CMDBM_Oracle_Execute,
	CMDBM_Oracle_OpenCursor,
	CMDBM_Oracle_CloseCursor,
	CMDBM_Oracle_CursorNextRow
};

#endif

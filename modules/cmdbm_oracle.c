
#ifdef CMDBM_ORACLE

#if defined(DEBUG)
# include "functions.h"
#else
# include "libcmdbm.h"
# define CMDBM_STATIC	static
#endif

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
	CMUTIL_Bool		needconv;
} CMDBM_OracleCtx;

typedef struct CMDBM_OracleSession {
	CMDBM_OracleCtx	*ctx;
	OCIEnv			*envhp;
	OCIError		*errhp;
	OCIServer		*srvhp;
	OCISvcCtx		*svchp;
	OCISession		*authp;
	CMUTIL_Bool		autocommit;
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
			CMUTIL_CALL((c)->ctx->convmtx, Lock);\
			out = CMUTIL_CALL((c)->ctx->conv, Forward, buf);\
			CMUTIL_CALL((c)->ctx->convmtx, Unlock);\
			CMLogInfo("OCI success with information: ORA-%05d > %s",\
					  errcode, CMUTIL_CALL(out, GetCString));\
			CMUTIL_CALL(buf, Destroy); CMUTIL_CALL(out, Destroy);\
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
			CMUTIL_CALL(c->ctx->convmtx, Lock);\
			out = CMUTIL_CALL(c->ctx->conv, Forward, buf);\
			CMUTIL_CALL(c->ctx->convmtx, Unlock);\
			CMLogError("%s failed. OCI error: ORA-%05d > %s",\
					   #a, errcode, CMUTIL_CALL(out, GetCString));\
			CMUTIL_CALL(buf, Destroy); CMUTIL_CALL(out, Destroy);\
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
			res->needconv = CMUTIL_True;
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
		if (ctx->convmtx) CMUTIL_CALL(ctx->convmtx, Destroy);
		if (ctx->conv) CMUTIL_CALL(ctx->conv, Destroy);
		CMFree(ctx);
	}
}

CMDBM_STATIC char *CMDBM_Oracle_GetBindString(
		void *initres, int index, char *buffer)
{
	CMUTIL_UNUSED(initres);
	sprintf(buffer, ":%d", (index+1));
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
	CMUTIL_Bool succ = CMUTIL_False;
	CMUTIL_JsonValue *user =
			(CMUTIL_JsonValue*)CMUTIL_CALL(params, GetString, "user");
	CMUTIL_JsonValue *pass =
			(CMUTIL_JsonValue*)CMUTIL_CALL(params, GetString, "password");
	CMUTIL_JsonValue *tns  =
			(CMUTIL_JsonValue*)CMUTIL_CALL(params, GetString, "tnsname");
	CMDBM_OracleSession *res = CMAlloc(sizeof(CMDBM_OracleSession));
	CMDBM_OracleCtx *ctx = (CMDBM_OracleCtx*)initres;
	const char *suser, *spass, *stns;
	sb4 status;

	if (!user || !pass || !tns) {
		CMLogError("Oracle connection requires 'tnsname', 'user', 'password'");
		goto ENDPOINT;
	}
	suser = CMUTIL_CALL(user, GetCString);
	spass = CMUTIL_CALL(pass, GetCString);
	stns  = CMUTIL_CALL(tns , GetCString);

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

	succ = CMUTIL_True;
ENDPOINT:
	if (!succ) {
		CMDBM_Oracle_CloseConnection(NULL, res);
		res = NULL;
	}
	return res;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_Oracle_StartTransaction(
		void *initres, void *connection)
{
	CMDBM_OracleSession *conn = (CMDBM_OracleSession*)connection;
	conn->autocommit = CMUTIL_False;
	CMUTIL_UNUSED(initres);
	return CMUTIL_True;
}

CMDBM_STATIC void CMDBM_Oracle_EndTransaction(
		void *initres, void *connection)
{
	CMDBM_OracleSession *conn = (CMDBM_OracleSession*)connection;
	conn->autocommit = CMUTIL_True;
	CMUTIL_UNUSED(initres);
}

CMDBM_STATIC CMUTIL_Bool CMDBM_Oracle_CommitTransaction(
		void *initres, void *connection)
{
	CMUTIL_Bool res = CMUTIL_False;
	sb4 status;
	CMDBM_OracleSession *conn = (CMDBM_OracleSession*)connection;
	CMDBM_OracleCheck(conn, status, ENDPOINT, OCITransCommit,
					  conn->svchp, conn->errhp, 0);
	res = CMUTIL_True;
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
	char		*name;
	int			index;
	ub2			typecd;
	int			bufsz;
	void		*buffer;
	int			indicator;
	OCIDefine	*define;
} CMDBM_OracleColumn;

CMDBM_STATIC CMUTIL_Bool CMDBM_Oracle_BindLong(
		CMDBM_OracleSession *conn, OCIBind **bind,
		OCIStmt *stmt, CMUTIL_JsonValue *jval,
		int pos, CMUTIL_Array *bufarr, CMUTIL_Json *out,
		CMUTIL_Array *outarr)
{
	int64 *val = CMAlloc(sizeof(int64));
	sb4 status;
	*val = CMUTIL_CALL(jval, GetLong);
	CMUTIL_CALL(bufarr, Add, val);
	if (out) {
		CMDBM_OracleColumn *item = CMAlloc(sizeof(CMDBM_OracleColumn));
		memset(item, 0x0, sizeof(sizeof(CMDBM_OracleColumn)));
		item->typecd = CMUTIL_JsonValueLong;
		item->index = pos;
		item->buffer = val;
		CMUTIL_CALL(outarr, Add, item);
		CMDBM_OracleCheck(conn, status, ENDPOINT, OCIBindByPos,
						  stmt, bind, conn->errhp, pos+1, val, sizeof(int64),
						  SQLT_INT, &(item->indicator),0,0,0,0,OCI_DEFAULT);
	} else {
		CMDBM_OracleCheck(conn, status, ENDPOINT, OCIBindByPos,
						  stmt, bind, conn->errhp, pos+1, val, sizeof(int64),
						  SQLT_INT, 0,0,0,0,0,OCI_DEFAULT);
	}
	return CMUTIL_True;
ENDPOINT:
	return CMUTIL_False;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_Oracle_BindDouble(
		CMDBM_OracleSession *conn, OCIBind **bind,
		OCIStmt *stmt, CMUTIL_JsonValue *jval,
		int pos, CMUTIL_Array *bufarr, CMUTIL_Json *out,
		CMUTIL_Array *outarr)
{
	double *val = CMAlloc(sizeof(double));
	sb4 status;
	*val = CMUTIL_CALL(jval, GetDouble);
	CMUTIL_CALL(bufarr, Add, val);
	if (out) {
		CMDBM_OracleColumn *item = CMAlloc(sizeof(CMDBM_OracleColumn));
		memset(item, 0x0, sizeof(sizeof(CMDBM_OracleColumn)));
		item->typecd = CMUTIL_JsonValueDouble;
		item->index = pos;
		item->buffer = val;
		CMUTIL_CALL(outarr, Add, item);
		CMDBM_OracleCheck(conn, status, ENDPOINT, OCIBindByPos,
						  stmt, bind, conn->errhp, pos+1, val, sizeof(double),
						  SQLT_FLT, &(item->indicator),0,0,0,0,OCI_DEFAULT);
	} else {
		CMDBM_OracleCheck(conn, status, ENDPOINT, OCIBindByPos,
						  stmt, bind, conn->errhp, pos+1, val, sizeof(double),
						  SQLT_FLT, 0,0,0,0,0,OCI_DEFAULT);
	}
	return CMUTIL_True;
ENDPOINT:
	return CMUTIL_False;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_Oracle_BindString(
		CMDBM_OracleSession *conn, OCIBind **bind,
		OCIStmt *stmt, CMUTIL_JsonValue *jval,
		int pos, CMUTIL_Array *bufarr, CMUTIL_Json *out,
		CMUTIL_Array *outarr)
{
	char buf[4096] = {0,};
	sb4 status;
	CMUTIL_String *val = CMUTIL_CALL(jval, GetString);
	if (out) {
		CMDBM_OracleColumn *item = CMAlloc(sizeof(CMDBM_OracleColumn));
		memset(item, 0x0, sizeof(sizeof(CMDBM_OracleColumn)));
		item->typecd = CMUTIL_JsonValueString;
		item->index = pos;
		CMUTIL_CALL(val, AddNString, buf, 4096);
		CMUTIL_CALL(outarr, Add, item);
		CMDBM_OracleCheck(conn, status, ENDPOINT, OCIBindByPos,
						  stmt, bind, conn->errhp, pos+1,
						  (void*)CMUTIL_CALL(val, GetCString),
						  CMUTIL_CALL(val, GetSize),
						  SQLT_STR, &(item->indicator),0,0,0,0,OCI_DEFAULT);
	} else {
		CMDBM_OracleCheck(conn, status, ENDPOINT, OCIBindByPos,
						  stmt, bind, conn->errhp, pos+1,
						  (void*)CMUTIL_CALL(val, GetCString),
						  CMUTIL_CALL(val, GetSize),
						  SQLT_STR, 0,0,0,0,0,OCI_DEFAULT);
	}
	return CMUTIL_True;
ENDPOINT:
	CMUTIL_UNUSED(bufarr);
	return CMUTIL_False;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_Oracle_BindBoolean(
		CMDBM_OracleSession *conn, OCIBind **bind,
		OCIStmt *stmt, CMUTIL_JsonValue *jval,
		int pos, CMUTIL_Array *bufarr, CMUTIL_Json *out,
		CMUTIL_Array *outarr)
{
	int *val = CMAlloc(sizeof(int));
	sb4 status;
	*val = CMUTIL_CALL(jval, GetBoolean);
	CMUTIL_CALL(bufarr, Add, val);
	if (out) {
		CMDBM_OracleColumn *item = CMAlloc(sizeof(CMDBM_OracleColumn));
		memset(item, 0x0, sizeof(sizeof(CMDBM_OracleColumn)));
		item->typecd = CMUTIL_JsonValueBoolean;
		item->index = pos;
		item->buffer = val;
		CMUTIL_CALL(outarr, Add, item);
		CMDBM_OracleCheck(conn, status, ENDPOINT, OCIBindByPos,
						  stmt, bind, conn->errhp, pos+1, val, sizeof(int),
						  SQLT_INT, &(item->indicator),0,0,0,0,OCI_DEFAULT);
	} else {
		CMDBM_OracleCheck(conn, status, ENDPOINT, OCIBindByPos,
						  stmt, bind, conn->errhp, pos+1, val, sizeof(int),
						  SQLT_INT, 0,0,0,0,0,OCI_DEFAULT);
	}
	return CMUTIL_True;
ENDPOINT:
	return CMUTIL_False;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_Oracle_BindNull(
		CMDBM_OracleSession *conn, OCIBind **bind,
		OCIStmt *stmt, CMUTIL_JsonValue *jval,
		int pos, CMUTIL_Array *bufarr, CMUTIL_Json *out,
		CMUTIL_Array *outarr)
{
	sb4 status, ind = -1;
	CMDBM_OracleCheck(conn, status, ENDPOINT, OCIBindByPos,
					  stmt, bind, conn->errhp, pos+1, NULL, 0,
					  SQLT_STR, &ind,0,0,0,0,OCI_DEFAULT);
	return CMUTIL_True;
ENDPOINT:
	CMUTIL_UNUSED(jval, bufarr, out, outarr);
	return CMUTIL_False;
}

typedef CMUTIL_Bool (*CMDBM_Oracle_BindProc)(
		CMDBM_OracleSession *conn, OCIBind **bind,
		OCIStmt *stmt, CMUTIL_JsonValue *jval,
		int pos, CMUTIL_Array *bufarr, CMUTIL_Json *out,
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
		col->bufsz = (int)sizeof(int64);
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
		CMUTIL_CALL(jval, SetNull);
	} else {
		int len;
		CMUTIL_String *temp;
		switch (col->typecd) {
		case CMUTIL_JsonValueBoolean:
			CMUTIL_CALL(jval, SetBoolean, (CMUTIL_Bool)*((int*)col->buffer));
			break;
		case CMUTIL_JsonValueLong:
			CMUTIL_CALL(jval, SetLong, (int64)*((int64*)col->buffer));
			break;
		case CMUTIL_JsonValueDouble:
			CMUTIL_CALL(jval, SetDouble, (double)*((double*)col->buffer));
			break;
		case CMUTIL_JsonValueString:
			temp = CMUTIL_CALL(jval, GetString);
			len = strlen(CMUTIL_CALL(temp, GetCString));
			CMUTIL_CALL(temp, CutTailOff, len);
			break;
		}
	}
}

CMDBM_STATIC OCIStmt *CMDBM_Oracle_ExecuteBase(
		CMDBM_OracleSession *conn, CMUTIL_String *query,
		CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
	int i, bsize;
	sb4 status;
	CMUTIL_Bool succ = CMUTIL_False;
	OCIStmt *stmt = NULL;
	OCIBind **buffers = NULL;
	CMUTIL_Array *array = CMUTIL_ArrayCreateEx(
				CMUTIL_CALL(binds, GetSize), NULL, CMFree);
	CMUTIL_Array *outarr = CMUTIL_ArrayCreateEx(
				CMUTIL_CALL(binds, GetSize), NULL, CMFree);

	CMDBM_OracleCheck(conn, status, FAILEDPOINT, OCIHandleAlloc, conn->envhp,
					  (void**)&stmt, OCI_HTYPE_STMT, 0, NULL);
	CMDBM_OracleCheck(conn, status, FAILEDPOINT, OCIStmtPrepare,
					  stmt, conn->errhp,
					  (text*)CMUTIL_CALL(query, GetCString),
					  (ub4)CMUTIL_CALL(query, GetSize),
					  OCI_NTV_SYNTAX, OCI_DEFAULT);

	bsize = CMUTIL_CALL(binds, GetSize);
	buffers = CMAlloc(sizeof(OCIBind*) * bsize);
	memset(buffers, 0x0, sizeof(OCIBind*) * bsize);
	for (i=0; i<bsize; i++) {
		char ibuf[20];
		CMUTIL_Json *json = CMUTIL_CALL(binds, Get, i);
		sprintf(ibuf, "%d", i);
		if (CMUTIL_CALL(json, GetType) == CMUTIL_JsonTypeValue) {
			CMUTIL_JsonValue *jval = (CMUTIL_JsonValue*)json;
			CMUTIL_Json *out = CMUTIL_CALL(outs, Get, ibuf);
			// type of json value
			succ = g_cmdbm_oracle_bindprocs[
					CMUTIL_CALL(jval, GetValueType)](
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
		CMUTIL_StringArray *keys = CMUTIL_CALL(outs, GetKeys);
		for (i=0; i<CMUTIL_CALL(keys, GetSize); i++) {
			const char *cidx = CMUTIL_CALL(keys, GetCString, i);
			CMUTIL_JsonValue *jval =
					(CMUTIL_JsonValue*)CMUTIL_CALL(outs, Get, cidx);
			int idx = atoi(cidx);
			CMDBM_OracleColumn *col =
					(CMDBM_OracleColumn*)CMUTIL_CALL(outarr, GetAt, idx);
			CMDBM_Oracle_SetOutValue(col, jval);
		}
	}

	succ = CMUTIL_True;
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
		CMUTIL_CALL(outarr, Destroy);
	if (array)
		CMUTIL_CALL(array, Destroy);
	return stmt;
}

CMDBM_STATIC OCIStmt *CMDBM_Oracle_SelectBase(
		CMDBM_OracleSession *conn, CMUTIL_String *query,
		CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs,
		CMUTIL_Array *outcols)
{
	CMUTIL_Bool succ = CMUTIL_False;
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

		CMUTIL_CALL(outcols, Add, column);
	}
	succ = CMUTIL_True;
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
	int i;
	sb4 status;
	CMUTIL_JsonObject *res = NULL;
	CMDBM_OracleCheck(conn, status, FAILEDPOINT, OCIStmtFetch,
					  stmt, conn->errhp, 1, OCI_FETCH_NEXT, OCI_DEFAULT);
	if (status != OCI_NO_DATA && status != OCI_NEED_DATA) {
		res = CMUTIL_JsonObjectCreate();
		for (i=0; i<CMUTIL_CALL(cols, GetSize); i++) {
			CMDBM_OracleColumn *col =
					(CMDBM_OracleColumn*)CMUTIL_CALL(cols, GetAt, i);
			switch(col->typecd) {
			case SQLT_INT:
				CMUTIL_CALL(res, PutLong, col->name,
							(int64)*((int64*)col->buffer));
				break;
			case SQLT_FLT:
				CMUTIL_CALL(res, PutDouble, col->name,
							(double)*((double*)col->buffer));
				break;
			default:
				CMUTIL_CALL(res, PutString, col->name, col->buffer);
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
		CMLogError("cannot fetch row.\n%s", CMUTIL_CALL(query, GetCString));

	CMUTIL_CALL(outcols, Destroy);
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
		CMUTIL_StringArray *keys = CMUTIL_CALL(row, GetKeys);
		if (CMUTIL_CALL(keys, GetSize) > 0) {
			const char *key = CMUTIL_CALL(keys, GetCString, 0);
			res = (CMUTIL_JsonValue*)CMUTIL_CALL(row, Remove, key);
		} else {
			CMLogError("row does not contain any fields.\n%s",
					   CMUTIL_CALL(query, GetCString));
		}
		CMUTIL_CALL(keys, Destroy);
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
		CMUTIL_CALL(res, Add, (CMUTIL_Json*)row);

	CMUTIL_CALL(outcols, Destroy);
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
	CMUTIL_Bool			isend;
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
		CMUTIL_CALL(outcols, Destroy);
	}
	CMUTIL_UNUSED(initres);
	return res;
}

CMDBM_STATIC void CMDBM_Oracle_CloseCursor(void *cursor)
{
	CMDBM_Oracle_Cursor *csr = (CMDBM_Oracle_Cursor*)cursor;
	if (csr) {
		if (csr->outcols) CMUTIL_CALL(csr->outcols, Destroy);
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
			csr->isend = CMUTIL_True;
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

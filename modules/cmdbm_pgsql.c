
#include "functions.h"

#ifdef CMDBM_PGSQL

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

CMDBM_STATIC void *CMDBM_PgSQL_OpenConnection(
		void *initres, CMUTIL_JsonObject *params)
{
	int i;
    char *key[128], * value[128];
    CMUTIL_StringArray *keys = CMCall(params, GetKeys);
    for (i=0; i<CMCall(keys, GetSize); i++) {
        key[i] = (char*)CMCall(keys, GetCString, i);
        value[i] = (char*)CMCall(params, GetCString, key[i]);
	}
	key[i] = value[i] = NULL;

    CMCall(keys, Destroy);
    CMUTIL_UNUSED(initres);
}

CMDBM_STATIC void CMDBM_PgSQL_CloseConnection(
		void *initres, void *conneciton)
{

}

#endif

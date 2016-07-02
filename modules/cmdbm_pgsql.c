#ifdef CMDBM_PGSQL

#if defined(DEBUG)
# include "functions.h"
#else
# include "libcmdbm.h"
# define CMDBM_STATIC	static
#endif

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
	char *key[128], *value[128];
	CMUTIL_StringArray *keys = CMUTIL_CALL(params, GetKeys);
	for (i=0; i<CMUTIL_CALL(keys, GetSize); i++) {
		key[i] = (char*)CMUTIL_CALL(keys, GetCString, i);
		value[i] = (char*)CMUTIL_CALL(params, GetCString, key[i]);
	}
	key[i] = value[i] = NULL;
	CMUTIL_CALL(keys, Destroy);
}

CMDBM_STATIC void CMDBM_PgSQL_CloseConnection(
		void *initres, void *conneciton)
{

}

#endif

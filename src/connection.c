
#include "functions.h"

CMUTIL_LogDefine("cmdbm.connection")

typedef struct CMDBM_Connection_Internal {
	CMDBM_Connection		base;
	CMDBM_ModuleInterface	*modif;
	CMDBM_DatabaseEx		*db;
	void					*connection;
	void					*initres;
} CMDBM_Connection_Internal;

typedef struct CMDBM_Cursor_Internal {
	CMDBM_Cursor				base;
	CMDBM_Connection_Internal	*connref;
	void						*cursor;
} CMDBM_Cursor_Internal;

CMDBM_STATIC char *CMDBM_ConnectionGetBindString(
		CMDBM_Connection *conn,
        uint32_t index,
		char *buffer)
{
	CMDBM_Connection_Internal *iconn = (CMDBM_Connection_Internal*)conn;
	return iconn->modif->GetBindString(iconn->initres, index, buffer);
}

#define CMDBM_DEFAULT_EXEC(a)	do {\
	CMDBM_Connection_Internal *iconn = (CMDBM_Connection_Internal*)conn;\
	return iconn->modif->a(iconn->initres,iconn->connection,query,binds,outs);\
} while(0)


CMDBM_STATIC CMUTIL_JsonValue *CMDBM_ConnectionGetObject(
		CMDBM_Connection *conn,
		CMUTIL_String *query,
		CMUTIL_JsonArray *binds,
		CMUTIL_JsonObject *outs)
{
	CMDBM_DEFAULT_EXEC(GetOneValue);
}

CMDBM_STATIC CMUTIL_JsonObject *CMDBM_ConnectionGetRow(
		CMDBM_Connection *conn,
		CMUTIL_String *query,
		CMUTIL_JsonArray *binds,
		CMUTIL_JsonObject *outs)
{
	CMDBM_DEFAULT_EXEC(GetRow);
}

CMDBM_STATIC CMUTIL_JsonArray *CMDBM_ConnectionGetList(
		CMDBM_Connection *conn,
		CMUTIL_String *query,
		CMUTIL_JsonArray *binds,
		CMUTIL_JsonObject *outs)
{
	CMDBM_DEFAULT_EXEC(GetList);
}

CMDBM_STATIC int CMDBM_ConnectionExecute(
		CMDBM_Connection *conn,
		CMUTIL_String *query,
		CMUTIL_JsonArray *binds,
		CMUTIL_JsonObject *outs)
{
	CMDBM_DEFAULT_EXEC(Execute);
}

CMDBM_STATIC CMUTIL_JsonObject *CMDBM_CursorGetNext(
		CMDBM_Cursor *cursor)
{
	CMDBM_Cursor_Internal *icsr = (CMDBM_Cursor_Internal*)cursor;
	return icsr->connref->modif->CursorNextRow(icsr->cursor);
}

CMDBM_STATIC void CMDBM_CursorClose(
		CMDBM_Cursor *cursor)
{
	CMDBM_Cursor_Internal *icsr = (CMDBM_Cursor_Internal*)cursor;
	icsr->connref->modif->CloseCursor(icsr->cursor);
	CMFree(icsr);
}

CMDBM_STATIC CMDBM_Cursor *CMDBM_ConnectionOpenCursor(
		CMDBM_Connection *conn,
		CMUTIL_String *query,
		CMUTIL_JsonArray *binds,
		CMUTIL_JsonObject *outs)
{
	CMDBM_Connection_Internal *iconn = (CMDBM_Connection_Internal*)conn;
	void *csr = iconn->modif->OpenCursor(
				iconn->initres, iconn->connection, query, binds, outs);
	CMDBM_Cursor_Internal *res = CMAlloc(sizeof(CMDBM_Cursor_Internal));
	memset(res, 0x0, sizeof(CMDBM_Cursor_Internal));
	res->base.GetNext = CMDBM_CursorGetNext;
	res->base.Close = CMDBM_CursorClose;
	res->connref = iconn;
	res->cursor = csr;
	return (CMDBM_Cursor*)res;
}

CMDBM_STATIC CMUTIL_XmlNode *CMDBM_ConnectionGetQuery(
		CMDBM_Connection *conn,
		const char *id)
{
	CMDBM_Connection_Internal *iconn = (CMDBM_Connection_Internal*)conn;
	return CMCall(iconn->db, GetQuery, id);
}

CMDBM_STATIC void CMDBM_ConnectionClose(
		CMDBM_Connection *conn)
{
	CMDBM_Connection_Internal *iconn = (CMDBM_Connection_Internal*)conn;
	CMCall(iconn->db, ReleaseConnection, conn);
    CMLogTrace("connection closed");
}

CMDBM_STATIC void CMDBM_ConnectionCloseReal(
		CMDBM_Connection *conn)
{
	CMDBM_Connection_Internal *iconn = (CMDBM_Connection_Internal*)conn;
	if (iconn) {
		if (iconn->connection)
			iconn->modif->CloseConnection(iconn->initres, iconn->connection);
		CMFree(iconn);
	}
}

CMDBM_STATIC CMUTIL_Bool CMDBM_ConnectionBeginTransaction(
		CMDBM_Connection *conn)
{
	CMDBM_Connection_Internal *iconn = (CMDBM_Connection_Internal*)conn;
	return iconn->modif->StartTransaction(iconn->initres, iconn->connection);
}

CMDBM_STATIC void CMDBM_ConnectionEndTransaction(CMDBM_Connection *conn)
{
	CMDBM_Connection_Internal *iconn = (CMDBM_Connection_Internal*)conn;
	iconn->modif->EndTransaction(iconn->initres, iconn->connection);
}

CMDBM_STATIC CMUTIL_Bool CMDBM_ConnectionCommit(CMDBM_Connection *conn)
{
	CMDBM_Connection_Internal *iconn = (CMDBM_Connection_Internal*)conn;
	return iconn->modif->CommitTransaction(iconn->initres, iconn->connection);
}

CMDBM_STATIC void CMDBM_ConnectionRollback(CMDBM_Connection *conn)
{
	CMDBM_Connection_Internal *iconn = (CMDBM_Connection_Internal*)conn;
	iconn->modif->RollbackTransaction(iconn->initres, iconn->connection);
}

static CMDBM_Connection g_cmdbm_connection={
	CMDBM_ConnectionGetBindString,
	CMDBM_ConnectionGetQuery,
	CMDBM_ConnectionGetObject,
	CMDBM_ConnectionGetRow,
	CMDBM_ConnectionGetList,
	CMDBM_ConnectionExecute,
	CMDBM_ConnectionOpenCursor,
	CMDBM_ConnectionClose,
	CMDBM_ConnectionCloseReal,
	CMDBM_ConnectionBeginTransaction,
	CMDBM_ConnectionEndTransaction,
	CMDBM_ConnectionCommit,
	CMDBM_ConnectionRollback
};

CMDBM_Connection *CMDBM_ConnectionCreate(CMDBM_DatabaseEx *db, void *rawconn)
{
	CMDBM_Connection_Internal *res = CMAlloc(sizeof(CMDBM_Connection_Internal));
	memset(res, 0x0, sizeof(CMDBM_Connection_Internal));
	memcpy(res, &g_cmdbm_connection, sizeof(CMDBM_Connection));
	res->db = db;
	res->modif = CMCall(db, GetModuleIF);
	res->initres = CMCall(db, GetInitResult);
	res->connection = rawconn;
    CMLogTrace("connection created");
	return (CMDBM_Connection*)res;
}

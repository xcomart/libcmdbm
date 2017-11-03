#ifndef TYPES_H__
#define TYPES_H__

#include "libcmdbm.h"

typedef struct CMDBM_DatabaseEx CMDBM_DatabaseEx;

typedef struct CMDBM_Cursor CMDBM_Cursor;
struct CMDBM_Cursor {
	CMUTIL_JsonObject *(*GetNext)(CMDBM_Cursor *cursor);
	void (*Close)(CMDBM_Cursor *cursor);
};

typedef struct CMDBM_Connection CMDBM_Connection;
struct CMDBM_Connection {
	char *(*GetBindString)(
			CMDBM_Connection *conn,
            uint32_t index,
			char *buffer);
	CMUTIL_XmlNode *(*GetQuery)(
			CMDBM_Connection *conn,
			const char *id);
	CMUTIL_JsonValue *(*GetObject)(
			CMDBM_Connection *conn,
			CMUTIL_String *query,
			CMUTIL_JsonArray *binds,
			CMUTIL_JsonObject *outs);
	CMUTIL_JsonObject *(*GetRow)(
			CMDBM_Connection *conn,
			CMUTIL_String *query,
			CMUTIL_JsonArray *binds,
			CMUTIL_JsonObject *outs);
	CMUTIL_JsonArray *(*GetList)(
			CMDBM_Connection *conn,
			CMUTIL_String *query,
			CMUTIL_JsonArray *binds,
			CMUTIL_JsonObject *outs);
	int (*Execute)(
			CMDBM_Connection *conn,
			CMUTIL_String *query,
			CMUTIL_JsonArray *binds,
			CMUTIL_JsonObject *outs);
	CMDBM_Cursor *(*OpenCursor)(
			CMDBM_Connection *conn,
			CMUTIL_String *query,
			CMUTIL_JsonArray *binds,
			CMUTIL_JsonObject *outs);
	void (*Close)(
			CMDBM_Connection *conn);
	void (*CloseReal)(
			CMDBM_Connection *conn);
	CMUTIL_Bool (*BeginTransaction)(
			CMDBM_Connection *conn);
	void (*EndTransaction)(
			CMDBM_Connection *conn);
	CMUTIL_Bool (*Commit)(
			CMDBM_Connection *conn);
	void (*Rollback)(
			CMDBM_Connection *conn);
};

CMDBM_Connection *CMDBM_ConnectionCreate(
		CMDBM_DatabaseEx *db,
		void *rawconn);

struct CMDBM_DatabaseEx {
	CMDBM_Database		base;
	CMUTIL_Bool (*Initialize)(
			CMDBM_DatabaseEx *db,
			CMUTIL_Timer *timer,
			const char *pgcs);
	CMUTIL_JsonObject *(*GetParams)(
			CMDBM_DatabaseEx *db);
	CMDBM_ModuleInterface *(*GetModuleIF)(
			CMDBM_DatabaseEx *db);
	void *(*GetInitResult)(
			CMDBM_DatabaseEx *db);
	const char *(*GetId)(
			CMDBM_DatabaseEx *db);
	CMUTIL_XmlNode *(*GetQuery)(
			CMDBM_DatabaseEx *db,
			const char *id);
	CMDBM_Connection *(*GetConnection)(
			CMDBM_DatabaseEx *db);
	void (*ReleaseConnection)(
			CMDBM_DatabaseEx *db,
			CMDBM_Connection *conn);
	void (*LockQueryItem)(
			CMDBM_DatabaseEx *db);
	void (*UnlockQueryItem)(
			CMDBM_DatabaseEx *db);
};

typedef struct CMDBM_ContextEx CMDBM_ContextEx;
struct CMDBM_ContextEx {
	CMDBM_Context		base;
	CMDBM_DatabaseEx *(*GetDatabase)(
			CMDBM_ContextEx *ctx,
			const char *dbid);
};

#if defined(CMDBM_ODBC)
extern CMDBM_ModuleInterface g_cmdbm_odbc_interface;
#endif
#if defined(CMDBM_ORACLE)
extern CMDBM_ModuleInterface g_cmdbm_oracle_interface;
#endif
#if defined(CMDBM_MYSQL)
extern CMDBM_ModuleInterface g_cmdbm_mysql_interface;
#endif
#if defined(CMDBM_SQLITE)
extern CMDBM_ModuleInterface g_cmdbm_sqlite_interface;
#endif
#if defined(CMDBM_PGSQL)
extern CMDBM_ModuleInterface g_cmdbm_pgsql_interface;
#endif

#endif // TYPES_H__


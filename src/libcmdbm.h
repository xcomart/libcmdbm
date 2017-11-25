#ifndef LIBCMDBM_H__
#define LIBCMDBM_H__

#ifdef __cplusplus
extern "C" {
#endif

#include <libcmutils.h>

#ifndef CMDBM_API
# if defined(MSWIN)
#  if defined(CMDBM_EXPORT)
#   define CMDBM_API  __declspec(dllexport)
#  else
#   define CMDBM_API  __declspec(dllimport)
#  endif
# else
#  define CMDBM_API
# endif
#endif

CMDBM_API void CMDBM_Init(void);
CMDBM_API void CMDBM_Clear(void);

typedef struct CMDBM_ModuleInterface CMDBM_ModuleInterface;
struct CMDBM_ModuleInterface {
    void (*LibraryInit)(void);
    void (*LibraryClear)(void);
    const char *(*GetDBMSKey)(void);
	void *(*Initialize)(const char *dbcs, const char *prcs);
	void (*CleanUp)(
			void *initres);
	char *(*GetBindString)(
			void *initres,
            uint32_t index,
			char *buffer);
    const char *(*GetTestQuery)(void);
	void *(*OpenConnection)(
			void *initres,
			CMUTIL_JsonObject *params);
	void (*CloseConnection)(
			void *initres,
			void *connection);
	CMBool (*StartTransaction)(
			void *initres,
			void *connection);
	void (*EndTransaction)(
			void *initres,
			void *connection);
	CMBool (*CommitTransaction)(
			void *initres,
			void *connection);
	void (*RollbackTransaction)(
			void *initres,
			void *connection);
	CMUTIL_JsonValue *(*GetOneValue)(
			void *initres,
			void *connection,
			CMUTIL_String *query,
			CMUTIL_JsonArray *binds,
			CMUTIL_JsonObject *outs);
	CMUTIL_JsonObject *(*GetRow)(
			void *initres,
			void *connection,
			CMUTIL_String *query,
			CMUTIL_JsonArray *binds,
			CMUTIL_JsonObject *outs);
	CMUTIL_JsonArray *(*GetList)(
			void *initres,
			void *connection,
			CMUTIL_String *query,
			CMUTIL_JsonArray *binds,
			CMUTIL_JsonObject *outs);
	int (*Execute)(
			void *initres,
			void *connection,
			CMUTIL_String *query,
			CMUTIL_JsonArray *binds,
			CMUTIL_JsonObject *outs);
	void *(*OpenCursor)(
			void *initres,
			void *connection,
			CMUTIL_String *query,
			CMUTIL_JsonArray *binds,
			CMUTIL_JsonObject *outs);
	void (*CloseCursor)(
			void *cursor);
	CMUTIL_JsonObject *(*CursorNextRow)(
			void *cursor);
};

typedef struct CMDBM_PoolConfig {
    uint32_t pingterm;
	CMBool pingtest;
	CMBool testonborrow;
    uint32_t initcnt;
    uint32_t maxcnt;
	char *testsql;
} CMDBM_PoolConfig;


typedef enum CMDBM_DBType {
	CMDBM_DBType_ODBC		= 0,
	CMDBM_DBType_Oracle,
	CMDBM_DBType_MySQL,
	CMDBM_DBType_SQLite,
	CMDBM_DBType_PgSQL,
	CMDBM_DBType_Unknown,
	CMDBM_DBType_Custom		= 0xF00000
} CMDBM_DBType;

typedef struct CMDBM_Database CMDBM_Database;
struct CMDBM_Database {
	CMBool (*AddMapper)(
			CMDBM_Database *db,
			const char *mapperfile);
	CMBool (*AddMapperSet)(
			CMDBM_Database *db,
			const char *basepath,
			const char *filepattern,
			CMBool recursive);
	CMBool (*SetMonitor)(
			CMDBM_Database *db,
			int interval);
	void (*Destroy)(
			CMDBM_Database *db);
};

CMDBM_Database *CMDBM_DatabaseCreate(
		const char *sourceid,
		CMDBM_DBType dbtype,
		const char *dbcharset,
		CMDBM_PoolConfig *poolconf,
		CMUTIL_JsonObject *params);

CMDBM_Database *CMDBM_DatabaseCreateCustom(
		const char *sourceid,
		CMDBM_ModuleInterface *modif,
		const char *dbcharset,
		CMDBM_PoolConfig *poolconf,
		CMUTIL_JsonObject *params);

typedef struct CMDBM_Session CMDBM_Session;
struct CMDBM_Session {
	CMBool (*BeginTransaction)(
			CMDBM_Session		*session);
	void (*EndTransaction)(
			CMDBM_Session		*session);
	int (*Execute)(
			CMDBM_Session		*session,
			const char			*dbid,
			const char			*sqlid,
			CMUTIL_JsonObject	*params);
	CMUTIL_JsonValue *(*GetObject)(
			CMDBM_Session		*session,
			const char			*dbid,
			const char			*sqlid,
			CMUTIL_JsonObject	*params);
	CMUTIL_JsonObject *(*GetRow)(
			CMDBM_Session		*session,
			const char			*dbid,
			const char			*sqlid,
			CMUTIL_JsonObject	*params);
	CMUTIL_JsonArray *(*GetRowSet)(
			CMDBM_Session		*session,
			const char			*dbid,
			const char			*sqlid,
			CMUTIL_JsonObject	*params);
	CMBool (*ForEachRow)(
			CMDBM_Session		*session,
			const char			*dbid,
			const char			*sqlid,
			CMUTIL_JsonObject	*params,
			void				*udata,
			CMBool			(*rowcb)(
				CMUTIL_JsonObject	*row,
                uint32_t			rownum,
				void				*udata));
	CMBool (*Commit)(
			CMDBM_Session		*session);
	void (*Rollback)(
			CMDBM_Session		*session);
	void (*Close)(
			CMDBM_Session		*session);
};

typedef struct CMDBM_Context CMDBM_Context;
struct CMDBM_Context {
	CMBool (*AddDatabase)(
			CMDBM_Context		*context,
			CMDBM_Database		*database);
	CMDBM_Session *(*GetSession)(
			CMDBM_Context		*context);
	void (*Destroy)(
			CMDBM_Context		*context);
};

CMDBM_API CMDBM_Context *CMDBM_ContextCreate(
		const char			*confjson,		/* optional */
		const char			*progcharset,	/* optional: utf-8 default. */
		CMUTIL_Timer        *timer);		/* optional */

#ifdef __cplusplus
}
#endif

#endif // LIBCMDBM_H__


/*
MIT License

Copyright (c) 2020 Dennis Soungjin Park<xcomart@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

/**
 * @file libcmdbm.h
 * @brief The entire public API of libcmdbm.
 *
 * This is the only header to include. The context, the datasource, the
 * session and the interface a DBMS module implements are all declared here
 * and grouped by subject in the module list.
 *
 * @author Dennis Soungjin Park <xcomart@gmail.com>
 */

/**
 * @mainpage libcmdbm
 *
 * A C99 SQL mapping library in the shape of MyBatis: statements live in XML
 * mapper files, parameters and results are JSON objects, and the application
 * never writes SQL text or binding code.
 *
 * @section mp_object Objects are structs of function pointers
 *
 * libcmdbm follows the convention of
 * <a href="https://xcomart.github.io/libcmutils/api/">libcmutils</a>, on which
 * it is built. An object is a struct whose members are its methods, and every
 * method takes the object as its first argument. The @c CMCall macro fills
 * that argument in:
 *
 * @code
 * CMDBM_Session *sess = CMCall(ctx, GetSession);   // ctx->GetSession(ctx)
 * CMCall(sess, Close);                             // sess->Close(sess)
 * @endcode
 *
 * @section mp_objects The three objects of an application
 *
 * @li #CMDBM_Context - one per application. Reads the JSON configuration,
 *   owns the datasources declared in it and hands out sessions.
 * @li #CMDBM_Database - a datasource: one DBMS module, one connection pool
 *   and the mapper files whose statements it can run. Usually built from the
 *   configuration, but #CMDBM_DatabaseCreate builds one in code.
 * @li #CMDBM_Session - the object statements are run through. One session
 *   borrows at most one connection per datasource and holds them until it is
 *   closed, which is what makes a transaction across several datasources
 *   possible.
 *
 * @section mp_lifecycle Lifecycle
 *
 * @code
 * CMDBM_Init();                        // first library call in the process
 * ctx = CMDBM_ContextCreate("cmdbm_config.json", "UTF-8", NULL);
 * sess = CMCall(ctx, GetSession);
 * row = CMCall(sess, GetRow, "sales", "user.selectUser", params);
 * CMUTIL_JsonDestroy(row);             // results are owned by the caller
 * CMCall(sess, Close);                 // returns the connections to the pool
 * CMCall(ctx, Destroy);
 * CMDBM_Clear();                       // also clears libcmutils
 * @endcode
 *
 * Results handed out by a session are owned by the caller and are released
 * with @c CMUTIL_JsonDestroy. The parameter object stays the caller's.
 *
 * @section mp_where Where to look
 *
 * <b>Topics</b>, in the bar above, groups the API by subject. <b>Data
 * Structures</b> lists the object types, and the search box finds anything by
 * name.
 *
 * Elsewhere:
 *
 * @li <a href="https://github.com/xcomart/libcmdbm">Repository</a> - source,
 *   issues and releases. Its README documents the configuration file and the
 *   mapper syntax, neither of which is C API and so neither of which appears
 *   here.
 * @li <a href="https://github.com/xcomart/libcmdbm/tree/master/test">Tests</a>
 *   - runnable code, including a complete DBMS module written for the tests.
 */

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


/**
 * @defgroup CMDBM_Base Library initialization and version.
 * @{
 */

/**
 * @brief Version of the library which is being run.
 *
 * The value is the contents of the VERSION file the library was built from,
 * as a "major.minor.patch" string.
 *
 * @return A static string, never NULL. It must not be freed.
 */
CMDBM_API const char *CMDBM_GetLibVersion(void);

/**
 * @brief Initialize the library.
 *
 * Must be called before anything else in this header, from a single thread.
 * It initializes libcmutils with the @c CMMemSystem allocator, prepares the
 * mapper parser and builds the registry of the DBMS modules compiled in.
 *
 * Call it exactly once per process: unlike @c CMUTIL_Init it is not
 * reference counted, and a second call leaks the registry the first one
 * built. An application which wants its logging configured before libcmdbm
 * logs anything can take the libcmutils reference itself first - see the
 * logging section of the project README.
 *
 * @see CMDBM_Clear
 */
CMDBM_API void CMDBM_Init(void);

/**
 * @brief Release everything the library holds.
 *
 * Destroy every context first: a datasource which outlives this call keeps a
 * DBMS client library alive that nothing will clean up afterwards, which is
 * reported as a warning. The call ends with @c CMUTIL_Clear(), so libcmutils
 * is torn down as well - and its leak report covers this library too.
 *
 * @see CMDBM_Init
 */
CMDBM_API void CMDBM_Clear(void);

/**
 * @}
 */


/**
 * @defgroup CMDBM_Modules DBMS modules and the module interface.
 * @{
 *
 * A module is what turns the generic query machinery into calls of a
 * particular client library. The modules for MySQL/MariaDB, PostgreSQL,
 * SQLite, Oracle and ODBC ship with the library and are registered by
 * @c CMDBM_Init when they are compiled in; an application can add its own
 * with #CMDBM_RegisterDBMS.
 *
 * Only #CMDBM_ModuleInterface::Initialize, #CMDBM_ModuleInterface::CleanUp
 * and #CMDBM_ModuleInterface::OpenConnection know anything about connection
 * parameters. Everything else works on the opaque pointers those return, so
 * a module never has to know how the library is configured.
 */

/**
 * @brief The set of callbacks a DBMS module implements.
 *
 * Every member is called by the library only; an application fills the
 * struct in and hands it to #CMDBM_RegisterDBMS. The struct is copied into
 * each datasource which uses it, so it may live on the stack of the
 * registering function - but the registry keeps the pointer as well, so a
 * static or heap instance is the safer choice.
 *
 * Three opaque pointers travel through the interface:
 *
 * @li @em initres - whatever #Initialize returned. One per datasource.
 * @li @em connection - whatever #OpenConnection returned. One per pooled
 *   connection.
 * @li @em cursor - whatever #OpenCursor returned. One per open iteration.
 *
 * The statement callbacks share the same four arguments: the final SQL text,
 * the values to bind in placeholder order, and the map of the OUT parameters
 * to be written back. None of the JSON objects handed in is owned by the
 * module - it must neither destroy them nor keep references to them beyond
 * the call.
 */
typedef struct CMDBM_ModuleInterface CMDBM_ModuleInterface;
struct CMDBM_ModuleInterface {
    /**
     * @brief Prepare the client library, once per process.
     *
     * Called before the first datasource which uses this client library is
     * created, and never again while any datasource using it exists - the
     * library is reference counted by the key #GetDBMSKey reports, so a
     * module registered under several names, or used by several
     * datasources, still initializes once.
     *
     * May be NULL when the client library needs no global setup.
     */
    void (*LibraryInit)(void);

    /**
     * @brief Release what #LibraryInit prepared.
     *
     * Called when the last datasource using this client library is
     * destroyed. May be NULL - and should be, for client libraries whose
     * global teardown cannot be undone by a later #LibraryInit.
     */
    void (*LibraryClear)(void);

    /**
     * @brief Identify the client library this module drives.
     *
     * The key is what the reference counting of #LibraryInit and
     * #LibraryClear is keyed by, so modules which share a client library -
     * the MySQL and MariaDB registrations are the same module - must report
     * the same key. It is unrelated to the name the module is registered
     * under.
     *
     * @return A static string. If this member is NULL, or it returns NULL,
     *         the library callbacks are skipped altogether.
     */
    const char *(*GetDBMSKey)(void);

    /**
     * @brief Create the context of one datasource.
     *
     * Called once per datasource, before any connection of it is opened.
     * Nothing here talks to a database yet; this is where a module keeps
     * what it needs for the lifetime of the datasource, character set
     * conversion above all.
     *
     * @param dbcs The character set the database stores text in, as
     *             configured for this datasource.
     * @param prcs The character set the application works in - the one
     *             results are to be converted to.
     * @return The module context, handed back as @em initres to every other
     *         callback. NULL is allowed for a module which needs none, and
     *         then #CleanUp is not called either.
     */
    void *(*Initialize)(const char *dbcs, const char *prcs);

    /**
     * @brief Release the context #Initialize created.
     *
     * Called when the datasource is destroyed, after its connections are
     * closed.
     *
     * @param initres The module context.
     */
    void (*CleanUp)(
            void *initres);

    /**
     * @brief Write the placeholder of one bind variable.
     *
     * Called while the SQL text is being built, once for every @c \#{...}
     * expression, in the order the values will be bound in.
     *
     * @param initres The module context.
     * @param index Zero based position of the value among the bind
     *              variables of this statement. Client libraries which
     *              number their placeholders from one must add one, as the
     *              SQLite module does with its <code>?1</code> form.
     * @param buffer Where to write the placeholder, at least 50 bytes.
     * @param vtype JSON type of the value about to be bound, for the client
     *              libraries whose placeholder syntax depends on it.
     * @return @a buffer.
     */
    char *(*GetBindString)(
            void *initres,
            uint32_t index,
            char *buffer,
            CMJsonValueType vtype);

    /**
     * @brief The statement which proves a pooled connection still works.
     *
     * Read when the datasource is created, and run through #GetOneValue to
     * validate a pooled connection, so it must return exactly one value -
     * "select 1" for most databases, "select 1 from dual" for Oracle. It is
     * what a datasource validates with unless its pool configuration names
     * a statement of its own in CMDBM_PoolConfig::testsql.
     *
     * @return A static string. It must not be NULL.
     */
    const char *(*GetTestQuery)(void);

    /**
     * @brief Open one connection to the database.
     *
     * @param initres The module context.
     * @param params The connection parameters of the datasource: the keys of
     *               its configuration object which are not meta keys of
     *               libcmdbm itself, with the values as configured. Owned by
     *               the datasource - read it, do not keep it.
     * @return The connection, handed back as @em connection to every
     *         statement callback, or NULL when the connection cannot be
     *         opened. Returning NULL is not fatal: the pool retries on the
     *         next checkout.
     */
    void *(*OpenConnection)(
            void *initres,
            CMUTIL_JsonObject *params);

    /**
     * @brief Close a connection #OpenConnection opened.
     *
     * @param initres The module context.
     * @param connection The connection to close.
     */
    void (*CloseConnection)(
            void *initres,
            void *connection);

    /**
     * @brief Begin a transaction on one connection.
     *
     * Called when a session which is in a transaction takes this connection,
     * which for most client libraries means turning autocommit off.
     *
     * @param initres The module context.
     * @param connection The connection.
     * @return CMTrue when the connection is in a transaction afterwards.
     */
    CMBool (*StartTransaction)(
            void *initres,
            void *connection);

    /**
     * @brief End the transaction #StartTransaction began.
     *
     * Called after the session committed or rolled back, and must leave the
     * connection back in autocommit: it goes to the pool right afterwards
     * and the next session must not inherit a transaction.
     *
     * @param initres The module context.
     * @param connection The connection.
     */
    void (*EndTransaction)(
            void *initres,
            void *connection);

    /**
     * @brief Commit the running transaction.
     *
     * @param initres The module context.
     * @param connection The connection.
     * @return CMTrue when the commit succeeded.
     */
    CMBool (*CommitTransaction)(
            void *initres,
            void *connection);

    /**
     * @brief Roll the running transaction back.
     *
     * @param initres The module context.
     * @param connection The connection.
     */
    void (*RollbackTransaction)(
            void *initres,
            void *connection);

    /**
     * @brief Run a statement and return its first column of its first row.
     *
     * @param initres The module context.
     * @param connection The connection.
     * @param query The final SQL text.
     * @param binds The values to bind, in placeholder order. References into
     *              the caller's parameter object: bind them, do not destroy
     *              or keep them.
     * @param outs The OUT parameters, keyed by the decimal spelling of the
     *             zero based bind index of each. A module which can bind OUT
     *             parameters writes the returned values into these objects;
     *             one which cannot ignores the map.
     * @return The value, owned by the caller, or NULL when the statement
     *         failed or returned no row.
     */
    CMUTIL_JsonValue *(*GetOneValue)(
            void *initres,
            void *connection,
            CMUTIL_String *query,
            CMUTIL_JsonArray *binds,
            CMUTIL_JsonObject *outs);

    /**
     * @brief Run a statement and return its first row.
     *
     * Column names become the keys of the object, in the case the database
     * reports them.
     *
     * @param initres The module context.
     * @param connection The connection.
     * @param query The final SQL text.
     * @param binds The values to bind, in placeholder order.
     * @param outs The OUT parameters - see #GetOneValue.
     * @return The row, owned by the caller, or NULL when the statement
     *         failed or returned no row.
     */
    CMUTIL_JsonObject *(*GetRow)(
            void *initres,
            void *connection,
            CMUTIL_String *query,
            CMUTIL_JsonArray *binds,
            CMUTIL_JsonObject *outs);

    /**
     * @brief Run a statement and return all of its rows.
     *
     * @param initres The module context.
     * @param connection The connection.
     * @param query The final SQL text.
     * @param binds The values to bind, in placeholder order.
     * @param outs The OUT parameters - see #GetOneValue.
     * @return An array of row objects, owned by the caller - empty when the
     *         statement returned no row - or NULL when it failed. The
     *         difference matters: NULL is what the session reports as an
     *         error.
     */
    CMUTIL_JsonArray *(*GetList)(
            void *initres,
            void *connection,
            CMUTIL_String *query,
            CMUTIL_JsonArray *binds,
            CMUTIL_JsonObject *outs);

    /**
     * @brief Run a statement for its effect.
     *
     * A statement executed this way may still produce rows - an
     * <code>insert ... returning</code> does - and the module is expected to
     * consume them.
     *
     * @param initres The module context.
     * @param connection The connection.
     * @param query The final SQL text.
     * @param binds The values to bind, in placeholder order.
     * @param outs The OUT parameters - see #GetOneValue.
     * @return The number of rows affected, or -1 when the statement failed.
     */
    int (*Execute)(
            void *initres,
            void *connection,
            CMUTIL_String *query,
            CMUTIL_JsonArray *binds,
            CMUTIL_JsonObject *outs);

    /**
     * @brief Run a statement to be read row by row.
     *
     * The cursor keeps the connection busy until #CloseCursor, so a module
     * must not need it for anything else in between.
     *
     * @param initres The module context.
     * @param connection The connection.
     * @param query The final SQL text.
     * @param binds The values to bind, in placeholder order.
     * @param outs The OUT parameters - see #GetOneValue.
     * @param fetchsize The @c fetchSize attribute of the executed select,
     *                  0 when it is not given. It is a hint: modules which
     *                  cannot control the fetch size of their client library
     *                  may ignore it, and those which can read that many
     *                  rows per round trip instead of buffering the whole
     *                  result.
     * @return The cursor, handed back to #CursorNextRow and #CloseCursor, or
     *         NULL when the statement failed.
     */
    void *(*OpenCursor)(
            void *initres,
            void *connection,
            CMUTIL_String *query,
            CMUTIL_JsonArray *binds,
            CMUTIL_JsonObject *outs,
            uint32_t fetchsize);

    /**
     * @brief Close a cursor and release what it holds.
     *
     * @param cursor The cursor #OpenCursor returned.
     */
    void (*CloseCursor)(
            void *cursor);

    /**
     * @brief Read the next row of a cursor.
     *
     * @param cursor The cursor #OpenCursor returned.
     * @return The row, owned by the caller, or NULL when the result is
     *         exhausted or the fetch failed.
     */
    CMUTIL_JsonObject *(*CursorNextRow)(
            void *cursor);
};

/**
 * @brief Make a DBMS module available under a name.
 *
 * The name is what a datasource asks for - the @c type of the configuration,
 * or the @a dbmskey of #CMDBM_DatabaseCreate - and is matched case
 * insensitively. Registering a name which is taken replaces it, which is how
 * a module of an application can override one that ships with the library.
 *
 * The registry keeps @a modif by pointer, so it must live until
 * #CMDBM_Clear; each datasource created from it takes its own copy.
 *
 * @param dbmskey The name to register the module under.
 * @param modif The module interface.
 * @return CMTrue.
 */
CMDBM_API CMBool CMDBM_RegisterDBMS(
        const char *dbmskey,
        CMDBM_ModuleInterface *modif);

/**
 * @}
 */


/**
 * @defgroup CMDBM_Datasources Datasources and their connection pool.
 * @{
 *
 * A datasource is one database: the module which talks to it, the pool of
 * connections to it, and the mapper files whose statements may be run
 * against it. Applications which keep their configuration in the JSON file
 * never build one by hand - #CMDBM_ContextCreate does it - but the objects
 * are public so that a datasource can be assembled in code.
 */

/**
 * @brief How a datasource pools its connections.
 *
 * The struct is copied by #CMDBM_DatabaseCreate, so a caller may fill one in
 * on the stack, and @c testsql is copied too.
 */
typedef struct CMDBM_PoolConfig {
    /**
     * @brief Seconds between two rounds of the idle connection check.
     *
     * 0 is taken as the default of 30 - the check is a repeating task and
     * cannot run with no period at all.
     */
    uint32_t pingterm;
    /**
     * @brief Whether idle connections are checked every @c pingterm
     *        seconds.
     *
     * The check runs @c testsql on each connection currently sitting in the
     * pool, and replaces the ones which do not answer - which is what keeps
     * a pool usable across a database restart or an idle timeout on the
     * server.
     */
    CMBool pingtest;
    /**
     * @brief Whether a connection is validated when it is borrowed.
     *
     * The safe setting, and the default: a session never gets a connection
     * the database has dropped meanwhile. It costs one round trip per
     * checkout, which is what @c pingtest alone avoids.
     *
     * With this and @c pingtest both off, connections are handed out
     * without ever being tested.
     */
    CMBool testonborrow;
    /** @brief Connections opened when the datasource starts up. */
    uint32_t initcnt;
    /** @brief Upper limit of open connections. A session which asks for one
     *         beyond it waits, and gives up after five seconds. */
    uint32_t maxcnt;
    /**
     * @brief The statement which validates a connection.
     *
     * NULL takes the one of the module,
     * CMDBM_ModuleInterface::GetTestQuery - which is how an Oracle
     * datasource ends up validating with "select 1 from dual" without
     * anything having to say so.
     */
    char *testsql;
} CMDBM_PoolConfig;

/**
 * @brief A datasource: one database, its pool and its statements.
 */
typedef struct CMDBM_Database CMDBM_Database;
struct CMDBM_Database {
    /**
     * @brief Load one mapper file into this datasource.
     *
     * The statements of the file become available under
     * "namespace.statement". Loading a path which is already loaded
     * replaces it, statement by statement - which is what the reload monitor
     * uses when a file changes on disk.
     *
     * @param db The datasource.
     * @param mapperfile Path of the XML mapper file.
     * @return CMFalse when the file cannot be read or is not a valid mapper,
     *         and then nothing of it is loaded.
     */
    CMBool (*AddMapper)(
            CMDBM_Database *db,
            const char *mapperfile);

    /**
     * @brief Load every mapper file matching a pattern.
     *
     * The set is remembered as a whole, so the reload monitor notices files
     * which appear, change or disappear under @a basepath afterwards.
     *
     * @param db The datasource.
     * @param basepath Directory to look in.
     * @param filepattern Glob pattern the file names must match, "*.xml"
     *                    being the usual one.
     * @param recursive Whether subdirectories are searched too.
     * @return CMTrue. Files which fail to parse are logged and skipped, and
     *         do not make the call fail.
     */
    CMBool (*AddMapperSet)(
            CMDBM_Database *db,
            const char *basepath,
            const char *filepattern,
            CMBool recursive);

    /**
     * @brief How often the mapper files are checked for changes.
     *
     * Must be called before the datasource is handed to
     * CMDBM_Context::AddDatabase, which is where the task is scheduled.
     * Without it the interval is 30 seconds.
     *
     * @param db The datasource.
     * @param interval Seconds between two checks.
     * @return CMTrue.
     */
    CMBool (*SetMonitor)(
            CMDBM_Database *db,
            int interval);

    /**
     * @brief Destroy the datasource, closing every connection it pooled.
     *
     * Only for a datasource which was never added to a context, or whose
     * CMDBM_Context::AddDatabase failed: a context destroys the datasources
     * it took over.
     *
     * @param db The datasource.
     */
    void (*Destroy)(
            CMDBM_Database *db);
};

/**
 * @brief Build a datasource in code.
 *
 * Nothing is opened here. The module is initialized, the pool filled and the
 * mappers loaded when the datasource is handed to
 * CMDBM_Context::AddDatabase, so mappers and the monitor interval are set
 * between the two calls.
 *
 * Both @a poolconf and @a params are copied, and the caller keeps ownership
 * of what it passed in.
 *
 * @param sourceid The id sessions will name this datasource by.
 * @param dbmskey The registered name of the DBMS module - "PGSQL", "MARIA",
 *                "SQLITE", "ORACLE", "ODBC" or one of an application's own.
 *                Matched case insensitively.
 * @param dbcharset The character set the database stores text in.
 * @param poolconf How the connections are pooled.
 * @param params The connection parameters, handed to
 *               CMDBM_ModuleInterface::OpenConnection as they are - which
 *               keys are expected is up to the module.
 * @return The datasource, or NULL when no module is registered under
 *         @a dbmskey.
 */
CMDBM_API CMDBM_Database *CMDBM_DatabaseCreate(
        const char *sourceid,
        const char *dbmskey,
        const char *dbcharset,
        CMDBM_PoolConfig *poolconf,
        CMUTIL_JsonObject *params);

/**
 * @}
 */


/**
 * @defgroup CMDBM_Sessions Sessions: running mapped statements.
 * @{
 *
 * Every statement runs through a session. A session borrows the connection
 * of a datasource the first time it is asked for one and keeps it until it
 * is closed, so several calls - and a transaction spanning them - see the
 * same connection.
 *
 * A session belongs to the thread which created it: it is not synchronized,
 * and threads take one session each.
 *
 * All five statement methods take the same three arguments: which datasource
 * to run against, which statement of its mappers to run, and the parameters.
 * The parameter object is read, and written when a statement has a
 * @c selectKey or an OUT parameter, but never taken over - the caller
 * destroys it. Everything a session returns is the caller's, to be released
 * with @c CMUTIL_JsonDestroy.
 */

/**
 * @brief A session: the object statements are run through.
 */
typedef struct CMDBM_Session CMDBM_Session;
struct CMDBM_Session {
    /**
     * @brief Start a transaction across every datasource this session uses.
     *
     * Connections already borrowed leave autocommit at once, and connections
     * borrowed later join the transaction as they are taken - so a
     * transaction can be started before the first statement runs.
     *
     * Every transaction is ended by #Commit or #Rollback followed by
     * #EndTransaction. A session closed while one is running rolls it back.
     *
     * @param session The session.
     * @return CMTrue while a transaction is running, including when this
     *         call found one already started - which is logged as a warning.
     */
    CMBool (*BeginTransaction)(
            CMDBM_Session       *session);

    /**
     * @brief Put the connections of this session back into autocommit.
     *
     * Neither commits nor rolls back: the work is decided by #Commit or
     * #Rollback before this call.
     *
     * @param session The session.
     */
    void (*EndTransaction)(
            CMDBM_Session       *session);

    /**
     * @brief Run a statement for its effect.
     *
     * The statement is an insert, update, delete or DDL. A @c selectKey of
     * the statement runs as its @c order says, and writes the key it read
     * into @a params.
     *
     * @param session The session.
     * @param dbid Id of the datasource to run against.
     * @param sqlid Id of the statement, "namespace.statement".
     * @param params The parameters of the statement, and where a
     *               @c selectKey or an OUT parameter writes back.
     * @return The number of rows affected, or -1 when the statement could
     *         not be built or failed.
     */
    int (*Execute)(
            CMDBM_Session       *session,
            const char          *dbid,
            const char          *sqlid,
            CMUTIL_JsonObject   *params);

    /**
     * @brief Run a select and return one single value.
     *
     * The first column of the first row - a count, a sum, one column of one
     * row looked up by key.
     *
     * @param session The session.
     * @param dbid Id of the datasource to run against.
     * @param sqlid Id of the statement, "namespace.statement".
     * @param params The parameters of the statement.
     * @return The value, owned by the caller, or NULL when the statement
     *         failed or matched no row.
     */
    CMUTIL_JsonValue *(*GetObject)(
            CMDBM_Session       *session,
            const char          *dbid,
            const char          *sqlid,
            CMUTIL_JsonObject   *params);

    /**
     * @brief Run a select and return its first row.
     *
     * @param session The session.
     * @param dbid Id of the datasource to run against.
     * @param sqlid Id of the statement, "namespace.statement".
     * @param params The parameters of the statement.
     * @return The row as an object keyed by column name, owned by the
     *         caller, or NULL when the statement failed or matched no row.
     */
    CMUTIL_JsonObject *(*GetRow)(
            CMDBM_Session       *session,
            const char          *dbid,
            const char          *sqlid,
            CMUTIL_JsonObject   *params);

    /**
     * @brief Run a select and return all of its rows.
     *
     * The whole result is read into memory before the call returns;
     * #ForEachRow is the way to walk a result too large for that.
     *
     * @param session The session.
     * @param dbid Id of the datasource to run against.
     * @param sqlid Id of the statement, "namespace.statement".
     * @param params The parameters of the statement.
     * @return An array owned by the caller - of row objects, or of plain
     *         values when the statement carries @c resultType="value" -
     *         empty when nothing matched, or NULL when the statement failed.
     */
    CMUTIL_JsonArray *(*GetRowSet)(
            CMDBM_Session       *session,
            const char          *dbid,
            const char          *sqlid,
            CMUTIL_JsonObject   *params);

    /**
     * @brief Run a select and hand its rows to a callback one by one.
     *
     * Only one row is in memory at a time, which is what makes a result of
     * any size iterable. A statement which carries @c fetchSize also keeps
     * the client library from buffering the whole result - unless it has a
     * @c selectKey to run afterwards, in which case streaming is dropped and
     * a warning is logged, since the cursor would block that statement.
     *
     * @param session The session.
     * @param dbid Id of the datasource to run against.
     * @param sqlid Id of the statement, "namespace.statement".
     * @param params The parameters of the statement.
     * @param udata Passed to @a rowcb untouched.
     * @param rowcb Called for every row, with the row, its zero based
     *              number and @a udata. The row is destroyed by the library
     *              as soon as the callback returns, so anything to be kept
     *              is to be copied out of it. Returning CMFalse stops the
     *              iteration.
     * @return CMTrue when the statement ran, whether or not it matched any
     *         row and whether or not the callback stopped early; CMFalse
     *         when it could not be built or failed.
     */
    CMBool (*ForEachRow)(
            CMDBM_Session       *session,
            const char          *dbid,
            const char          *sqlid,
            CMUTIL_JsonObject   *params,
            void                *udata,
            CMBool             (*rowcb)(
                CMUTIL_JsonObject   *row,
                uint32_t            rownum,
                void                *udata));

    /**
     * @brief Commit the running transaction on every connection of this
     *        session.
     *
     * @param session The session.
     * @return CMFalse when no transaction is running.
     */
    CMBool (*Commit)(
            CMDBM_Session       *session);

    /**
     * @brief Roll the running transaction back on every connection of this
     *        session.
     *
     * @param session The session.
     */
    void (*Rollback)(
            CMDBM_Session       *session);

    /**
     * @brief Return the borrowed connections and destroy the session.
     *
     * A transaction still running is rolled back first, with a warning: a
     * connection must never go back to the pool with uncommitted work on it.
     *
     * @param session The session.
     */
    void (*Close)(
            CMDBM_Session       *session);
};

/**
 * @}
 */


/**
 * @defgroup CMDBM_Contexts The context: configuration, datasources and
 *           sessions.
 * @{
 */

/**
 * @brief The root object: the datasources of an application and the
 *        sessions which use them.
 */
typedef struct CMDBM_Context CMDBM_Context;
struct CMDBM_Context {
    /**
     * @brief Start a datasource and take it over.
     *
     * This is where a datasource comes to life: the module is initialized,
     * the initial connections are opened, the mapper files are read and the
     * reload task is scheduled.
     *
     * @param context The context.
     * @param database The datasource, built by #CMDBM_DatabaseCreate. On
     *                 success the context owns it and destroys it with
     *                 itself; on failure it stays the caller's, to be
     *                 destroyed through CMDBM_Database::Destroy.
     * @return CMFalse when the datasource cannot be initialized - a client
     *         library which will not load, or connection parameters no
     *         connection can be opened with.
     */
    CMBool (*AddDatabase)(
            CMDBM_Context       *context,
            CMDBM_Database      *database);

    /**
     * @brief Open a session on this context.
     *
     * Cheap: no connection is borrowed until a statement runs. Sessions are
     * not shared between threads, and each is closed through
     * CMDBM_Session::Close.
     *
     * @param context The context.
     * @return The session, never NULL in practice.
     */
    CMDBM_Session *(*GetSession)(
            CMDBM_Context       *context);

    /**
     * @brief Destroy the context and every datasource in it.
     *
     * Close the sessions first: a session outliving its context holds
     * connections of pools which no longer exist.
     *
     * @param context The context.
     */
    void (*Destroy)(
            CMDBM_Context       *context);
};

/**
 * @brief Create a context, optionally from a configuration file.
 *
 * With a configuration file this is the only call an application needs: the
 * datasources declared in it are created, started and ready to be used
 * through a session when it returns. Without one the context comes up empty,
 * to be filled with #CMDBM_DatabaseCreate and CMDBM_Context::AddDatabase.
 *
 * @param confjson Path of the JSON configuration file, or NULL for an empty
 *                 context. A file which cannot be read leaves the context
 *                 empty as well; one which parses but is not valid
 *                 configuration fails the call.
 * @param progcharset The character set the application works in, "UTF-8"
 *                    when NULL. Results are converted to it.
 * @param timer The timer to schedule the mapper reload tasks and the pool
 *              maintenance on, or NULL to have the context keep one of its
 *              own. A timer passed in is not destroyed with the context, and
 *              must outlive it.
 * @return The context, or NULL when the configuration could not be applied.
 */
CMDBM_API CMDBM_Context *CMDBM_ContextCreate(
        const char              *confjson,
        const char              *progcharset,
        CMUTIL_Timer            *timer);

/**
 * @}
 */

#ifdef __cplusplus
}
#endif

#endif // LIBCMDBM_H__

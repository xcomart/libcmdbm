
#include "mockdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static MockDbStat g_stats[2];

MockDbStat *MockDbStatOf(int index)
{
    return &g_stats[index];
}

void MockDbReset(void)
{
    int i;
    for (i=0; i<2; i++) {
        if (g_stats[i].params)
            CMUTIL_JsonDestroy(g_stats[i].params);
        memset(&g_stats[i], 0x0, sizeof(MockDbStat));
    }
}

/* ------------------------------------------------------------------ */

static void MockLibInit(void) { g_stats[0].libinit++; }
static void MockLibClear(void) { g_stats[0].libclear++; }
static void Mock2LibInit(void) { g_stats[1].libinit++; }
static void Mock2LibClear(void) { g_stats[1].libclear++; }

static const char *MockKey(void) { return "MOCK"; }
static const char *Mock2Key(void) { return "MOCK2"; }

static void *MockInitialize(const char *dbcs, const char *prcs)
{
    CMUTIL_UNUSED(dbcs, prcs);
    return &g_stats[0];
}
static void *Mock2Initialize(const char *dbcs, const char *prcs)
{
    CMUTIL_UNUSED(dbcs, prcs);
    return &g_stats[1];
}
static void MockCleanUp(void *initres) { CMUTIL_UNUSED(initres); }

static char *MockBindString(
        void *initres, uint32_t index, char *buffer, CMJsonValueType vtype)
{
    CMUTIL_UNUSED(initres, vtype);
    sprintf(buffer, "$%u", index+1);
    return buffer;
}

static const char *MockTestQuery(void) { return "select 1"; }

static void *MockOpenConnection(void *initres, CMUTIL_JsonObject *params)
{
    MockDbStat *stat = (MockDbStat*)initres;
    CMUTIL_Json *json = (CMUTIL_Json*)params;
    if (stat->params)
        CMUTIL_JsonDestroy(stat->params);
    stat->params = (CMUTIL_JsonObject*)CMCall(json, Clone);
    stat->opened++;
    return stat;
}

static void MockCloseConnection(void *initres, void *connection)
{
    MockDbStat *stat = (MockDbStat*)initres;
    CMUTIL_UNUSED(connection);
    stat->closed++;
}

static CMBool MockStartTransaction(void *initres, void *connection)
{
    CMUTIL_UNUSED(initres, connection);
    return CMTrue;
}
static void MockEndTransaction(void *initres, void *connection)
{
    CMUTIL_UNUSED(initres, connection);
}
static CMBool MockCommitTransaction(void *initres, void *connection)
{
    CMUTIL_UNUSED(initres, connection);
    return CMTrue;
}
static void MockRollbackTransaction(void *initres, void *connection)
{
    CMUTIL_UNUSED(initres, connection);
}

/* rows are {"id":<n>,"name":"row<n>"}, n starting at 1 */
static CMUTIL_JsonObject *MockRow(int n)
{
    char buf[32];
    CMUTIL_JsonObject *row = CMUTIL_JsonObjectCreate();
    sprintf(buf, "row%d", n);
    CMCall(row, PutLong, "id", n);
    CMCall(row, PutString, "name", buf);
    return row;
}

static CMUTIL_JsonValue *MockGetOneValue(
        void *initres, void *connection, CMUTIL_String *query,
        CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    CMUTIL_JsonValue *res = CMUTIL_JsonValueCreate();
    CMUTIL_UNUSED(initres, connection, query, binds, outs);
    CMCall(res, SetLong, 1);
    return res;
}

static CMUTIL_JsonObject *MockGetRow(
        void *initres, void *connection, CMUTIL_String *query,
        CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    CMUTIL_UNUSED(initres, connection, query, binds, outs);
    return MockRow(1);
}

static CMUTIL_JsonArray *MockGetList(
        void *initres, void *connection, CMUTIL_String *query,
        CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    int i;
    CMUTIL_JsonArray *res = CMUTIL_JsonArrayCreate();
    CMUTIL_UNUSED(initres, connection, query, binds, outs);
    for (i=1; i<=MOCKDB_ROWCNT; i++) {
        CMUTIL_JsonObject *row = MockRow(i);
        CMCall(res, Add, (CMUTIL_Json*)row);
    }
    return res;
}

static int MockExecute(
        void *initres, void *connection, CMUTIL_String *query,
        CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs)
{
    CMUTIL_UNUSED(initres, connection, query, binds, outs);
    return 1;
}

typedef struct MockCursor {
    MockDbStat  *stat;
    int         current;
} MockCursor;

static void *MockOpenCursor(
        void *initres, void *connection, CMUTIL_String *query,
        CMUTIL_JsonArray *binds, CMUTIL_JsonObject *outs, uint32_t fetchsize)
{
    MockDbStat *stat = (MockDbStat*)initres;
    MockCursor *res = (MockCursor*)malloc(sizeof(MockCursor));
    CMUTIL_UNUSED(connection, query, binds, outs);
    stat->fetchsize = fetchsize;
    res->stat = stat;
    res->current = 0;
    return res;
}

static void MockCloseCursor(void *cursor)
{
    free(cursor);
}

static CMUTIL_JsonObject *MockCursorNextRow(void *cursor)
{
    MockCursor *csr = (MockCursor*)cursor;
    if (csr->current >= MOCKDB_ROWCNT)
        return NULL;
    csr->current++;
    return MockRow(csr->current);
}

#define MOCKDB_INTERFACE(libinit, libclear, key, initialize)    \
    libinit, libclear, key, initialize, MockCleanUp,            \
    MockBindString, MockTestQuery,                              \
    MockOpenConnection, MockCloseConnection,                    \
    MockStartTransaction, MockEndTransaction,                   \
    MockCommitTransaction, MockRollbackTransaction,             \
    MockGetOneValue, MockGetRow, MockGetList, MockExecute,      \
    MockOpenCursor, MockCloseCursor, MockCursorNextRow

CMDBM_ModuleInterface g_mockdb_interface = {
    MOCKDB_INTERFACE(MockLibInit, MockLibClear, MockKey, MockInitialize) };

CMDBM_ModuleInterface g_mockdb2_interface = {
    MOCKDB_INTERFACE(Mock2LibInit, Mock2LibClear, Mock2Key, Mock2Initialize) };

void MockDbRegister(void)
{
    CMDBM_RegisterDBMS("MOCK", &g_mockdb_interface);
    /* the same module under a second key, like MARIA and MYSQL */
    CMDBM_RegisterDBMS("MOCKALIAS", &g_mockdb_interface);
    CMDBM_RegisterDBMS("MOCK2", &g_mockdb2_interface);
}

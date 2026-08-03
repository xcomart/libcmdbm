/*
 * Connection pool settings: which statement validates a connection, and
 * when it is run.
 *
 * The mock module records every GetOneValue it is asked for, which is the
 * callback the pool validates through - so counting those calls is how the
 * checks below see what the pool did.
 */
#include "test.h"
#include "mockdb.h"

#include <string.h>

/* borrows a connection of 'dbid' by running a statement on it */
static CMBool TouchDatasource(CMDBM_Context *ctx, const char *dbid)
{
    CMUTIL_JsonObject *params = CMUTIL_JsonObjectCreate();
    CMDBM_Session *sess = CMCall(ctx, GetSession);
    CMUTIL_JsonObject *row = CMCall(sess, GetRow, dbid, "t.selMap", params);
    CMBool res = row != NULL? CMTrue:CMFalse;
    if (row) CMUTIL_JsonDestroy(row);
    CMCall(sess, Close);
    CMUTIL_JsonDestroy(params);
    return res;
}

int main(void)
{
    CMDBM_Context *ctx;
    MockDbStat *mock, *mock2;
    CMDBM_PoolConfig pconf;
    CMDBM_Database *db;
    CMUTIL_JsonObject *cparams;
    int pings, waited;

    CMDBM_Init();
    MockDbRegister();
    mock = MockDbStatOf(0);
    mock2 = MockDbStatOf(1);

    TESTCASE("the test statement of the pool configuration");
    ctx = CMDBM_ContextCreate("data/pool_conf.json", "UTF-8", NULL);
    CHECK(ctx != NULL, "context created");
    if (ctx == NULL) {
        MockDbReset();
        CMDBM_Clear();
        return TESTRESULT();
    }
    CHECK(mock->onevalue == 0, "opening a connection does not validate it");
    CHECK(TouchDatasource(ctx, "withsql"), "statement executed");
    CHECK(mock->onevalue == 1, "borrowing validates the connection once");
    CHECK(strcmp(mock->lastqry, "select mock_ping") == 0,
          "testSql of the configuration is what the pool runs");

    TESTCASE("no testSql leaves the statement of the module");
    CHECK(TouchDatasource(ctx, "modulesql"), "statement executed");
    CHECK(mock2->onevalue == 1, "borrowing validates the connection once");
    CHECK(strcmp(mock2->lastqry, MOCKDB2_TESTQUERY) == 0,
          "the module decides when the pool configuration does not");
    CMCall(ctx, Destroy);
    MockDbReset();

    TESTCASE("testOnBorrow and pingTest both off");
    ctx = CMDBM_ContextCreate("data/pool_notest_conf.json", "UTF-8", NULL);
    CHECK(ctx != NULL, "context created");
    if (ctx == NULL) {
        MockDbReset();
        CMDBM_Clear();
        return TESTRESULT();
    }
    CHECK(TouchDatasource(ctx, "notest"), "statement executed");
    CHECK(mock->onevalue == 0, "the connection is handed out untested");

    TESTCASE("pingTest without testOnBorrow");
    CHECK(mock2->onevalue == 0, "borrowing is not what triggers the ping");
    /* pingInterval of this datasource is 3 seconds. a period taken as
     * milliseconds - which is what the pool used to be created with - would
     * have pinged well within the first wait. */
    usleep(1200 * 1000);
    CHECK(mock2->onevalue == 0, "nothing is pinged before the period is up");
    /* waiting for the first ping rather than for a fixed span: a debug or
     * sanitizer build takes its time, and a slow machine must not turn this
     * into a failure. */
    for (waited = 1200; waited < 30000 && mock2->onevalue == 0; waited += 200)
        usleep(200 * 1000);
    pings = mock2->onevalue;
    printf("  (idle pings after %d ms: %d)\n", waited, pings);
    CHECK(pings >= 1, "the idle connection is pinged when the period elapses");
    CHECK(strcmp(mock2->lastqry, MOCKDB2_TESTQUERY) == 0,
          "the ping runs the test statement");

    CMCall(ctx, Destroy);
    MockDbReset();

    TESTCASE("a datasource built in code");
    memset(&pconf, 0x0, sizeof(pconf));
    pconf.initcnt = 1;
    pconf.maxcnt = 2;
    pconf.pingterm = 60;
    pconf.testonborrow = CMTrue;
    /* no testsql: the statement of the module is used */
    ctx = CMDBM_ContextCreate(NULL, "UTF-8", NULL);
    cparams = CMUTIL_JsonObjectCreate();
    db = CMDBM_DatabaseCreate("coded", "MOCK", "utf-8", &pconf, cparams);
    CHECK(db != NULL, "datasource created");
    if (db) {
        CHECK(CMCall(db, AddMapper, "data/mapper.xml"), "mapper loaded");
        CHECK(CMCall(ctx, AddDatabase, db), "datasource started");
        CHECK(TouchDatasource(ctx, "coded"), "statement executed");
        CHECK(mock->onevalue == 1, "borrowing validates the connection");
        CHECK(strcmp(mock->lastqry, MOCKDB_TESTQUERY) == 0,
              "a pool config without testsql takes the module statement");
    }
    CMUTIL_JsonDestroy(cparams);
    CMCall(ctx, Destroy);
    MockDbReset();

    CMDBM_Clear();
    return TESTRESULT();
}

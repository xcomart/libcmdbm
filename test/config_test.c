/*
 * JSON configuration loading, against the mock module.
 */
#include "test.h"
#include "mockdb.h"

int main(void)
{
    CMDBM_Context *ctx;
    CMDBM_Session *sess;
    CMUTIL_JsonObject *params, *row, *cparams;
    MockDbStat *stat;

    CMDBM_Init();
    MockDbRegister();
    stat = MockDbStatOf(0);

    TESTCASE("loading");
    ctx = CMDBM_ContextCreate("data/mock_conf.json", "UTF-8", NULL);
    CHECK(ctx != NULL, "context created from the configuration file");
    if (ctx == NULL) {
        MockDbReset();
        CMDBM_Clear();
        return TESTRESULT();
    }
    CHECK(stat->opened > 0, "the pool opened a connection");

    TESTCASE("connection parameters");
    cparams = stat->params;
    CHECK(cparams != NULL, "the module received parameters");
    if (cparams) {
        CHECK(CMCall(cparams, Get, "host") != NULL,
              "a scalar attribute of the datasource is passed through");
        CHECK(CMCall(cparams, Get, "user") != NULL,
              "an entry of the params object is passed through");
        CHECK(CMCall(cparams, Get, "type") == NULL, "'type' is not passed");
        CHECK(CMCall(cparams, Get, "id") == NULL, "'id' is not passed");
        CHECK(CMCall(cparams, Get, "charset") == NULL,
              "'charset' is not passed");
        CHECK(CMCall(cparams, Get, "pool") == NULL, "'pool' is not passed");
    }

    TESTCASE("the mapper of the configuration is loaded");
    sess = CMCall(ctx, GetSession);
    params = CMUTIL_JsonObjectCreate();
    row = CMCall(sess, GetRow, "local", "t.selMap", params);
    CHECK(row != NULL, "a query of the mapper file can be executed");
    if (row) CMUTIL_JsonDestroy(row);

    CMUTIL_JsonDestroy(params);
    CMCall(sess, Close);
    CMCall(ctx, Destroy);
    MockDbReset();
    CMDBM_Clear();

    return TESTRESULT();
}

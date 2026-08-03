/*
 * Configuration loading, against the mock module.
 * Both accepted forms are covered: the JSON one and the XML one, which is
 * converted into the very same JSON object before it is parsed.
 */
#include "test.h"
#include "mockdb.h"
// the logging flags are read through the internal context interface, the
// XML conversion is an internal function of the library.
#include "types.h"
#include "functions.h"

/* the serialized form of a JSON value, for comparing two configurations. */
static void json_string(CMUTIL_Json *json, char *buf, size_t bufsz)
{
    CMUTIL_String *str = CMUTIL_StringCreate();
    const char *sstr;
    *buf = 0x0;
    if (json == NULL) return;
    CMCall(json, ToString, str, CMFalse);
    sstr = CMCall(str, GetCString);
    snprintf(buf, bufsz, "%s", sstr);
    CMCall(str, Destroy);
}

/* every key of 'a' is in 'b' with the same value, and both have the same
 * number of keys. */
static CMBool same_object(CMUTIL_JsonObject *a, CMUTIL_JsonObject *b)
{
    CMUTIL_StringArray *keys, *bkeys;
    CMBool res = CMTrue;
    uint32_t i;
    size_t asize, bsize;

    if (a == NULL || b == NULL) return CMFalse;
    keys = CMCall(a, GetKeys);
    bkeys = CMCall(b, GetKeys);
    asize = CMCall(keys, GetSize);
    bsize = CMCall(bkeys, GetSize);
    CMCall(bkeys, Destroy);
    if (asize != bsize) {
        printf("       key count %u != %u\n",
               (unsigned)asize, (unsigned)bsize);
        res = CMFalse;
    }
    for (i=0; res && i<CMCall(keys, GetSize); i++) {
        const char *key = CMCall(keys, GetCString, i);
        CMUTIL_Json *av = CMCall(a, Get, key);
        CMUTIL_Json *bv = CMCall(b, Get, key);
        char abuf[512], bbuf[512];
        json_string(av, abuf, sizeof(abuf));
        json_string(bv, bbuf, sizeof(bbuf));
        if (bv == NULL || strcmp(abuf, bbuf) != 0) {
            printf("       key '%s': '%s' != '%s'\n", key, abuf, bbuf);
            res = CMFalse;
        }
    }
    CMCall(keys, Destroy);
    return res;
}

/* the datasource object of a converted configuration. */
static CMUTIL_JsonObject *first_database(CMUTIL_Json *conf)
{
    CMUTIL_Json *dbs;
    if (conf == NULL || CMCall(conf, GetType) != CMJsonTypeObject)
        return NULL;
    dbs = CMCall((CMUTIL_JsonObject*)conf, Get, "databases");
    if (dbs == NULL || CMCall(dbs, GetType) != CMJsonTypeArray)
        return NULL;
    if (CMCall((CMUTIL_JsonArray*)dbs, GetSize) == 0)
        return NULL;
    return (CMUTIL_JsonObject*)CMCall((CMUTIL_JsonArray*)dbs, Get, 0);
}

int main(void)
{
    CMDBM_Context *ctx, *dctx, *xctx;
    CMDBM_Session *sess;
    CMUTIL_JsonObject *params, *row, *cparams, *jparams = NULL;
    CMUTIL_XmlNode *xroot;
    CMUTIL_Json *xconf;
    MockDbStat *stat;
    CMBool lqid, lqry, lres;
    CMBool xqid, xqry, xres;

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
        CHECK(CMCall(cparams, Get, "monitorinterval") == NULL,
              "'monitorInterval' is not passed");
        /* kept for the comparison with the XML configuration */
        jparams = (CMUTIL_JsonObject*)CMCall((CMUTIL_Json*)cparams, Clone);
    }

    TESTCASE("the logging section is applied");
    lqid = lqry = lres = CMTrue;
    CMCall((CMDBM_ContextEx*)ctx, GetLogFlags, &lqid, &lqry, &lres);
    // "queryId": false, "query": "true", "result": true
    CHECK(lqid == CMFalse, "a boolean false of the logging section is read");
    CHECK(lqry == CMTrue, "the string \"true\" is read as a boolean");
    CHECK(lres == CMTrue, "a boolean true of the logging section is read");

    TESTCASE("the logging defaults apply without the section");
    dctx = CMDBM_ContextCreate(NULL, "UTF-8", NULL);
    CHECK(dctx != NULL, "a context without a configuration is created");
    if (dctx) {
        lqid = lqry = lres = CMFalse;
        CMCall((CMDBM_ContextEx*)dctx, GetLogFlags, &lqid, &lqry, &lres);
        CHECK(lqid == CMTrue, "queryId defaults to true");
        CHECK(lqry == CMTrue, "query defaults to true");
        CHECK(lres == CMFalse, "result defaults to false");
        CMCall(dctx, Destroy);
    }

    TESTCASE("the mapper of the configuration is loaded");
    sess = CMCall(ctx, GetSession);
    params = CMUTIL_JsonObjectCreate();
    row = CMCall(sess, GetRow, "local", "t.selMap", params);
    CHECK(row != NULL, "a query of the mapper file can be executed");
    if (row) CMUTIL_JsonDestroy(row);
    CMUTIL_JsonDestroy(params);
    CMCall(sess, Close);

    TESTCASE("the XML configuration is converted");
    xroot = CMUTIL_XmlParseFile("data/mock_conf.xml");
    CHECK(xroot != NULL, "the XML configuration file is parsed");
    xconf = xroot? CMDBM_ConfigFromXml(xroot):NULL;
    CHECK(xconf != NULL, "the XML tree is converted to a configuration");
    if (xconf) {
        CMUTIL_JsonObject *dcfg = first_database(xconf);
        CHECK(dcfg != NULL, "the 'Databases' section became an array");
        if (dcfg) {
            const char *stype = CMCall(dcfg, GetCString, "type");
            CMUTIL_Json *mi = CMCall(dcfg, Get, "monitorinterval");
            CMUTIL_Json *pool = CMCall(dcfg, Get, "pool");
            CMUTIL_Json *mappers = CMCall(dcfg, Get, "mappers");
            CMUTIL_Json *dparams = CMCall(dcfg, Get, "params");
            CHECK(stype != NULL && strcmp(stype, "MoCk") == 0,
                  "the tag name of a datasource became its type");
            CHECK(CMCall(dcfg, GetCString, "host") != NULL,
                  "a scalar child tag became a key");
            CHECK(mi != NULL && CMCall(mi, GetType) == CMJsonTypeValue &&
                  CMCall((CMUTIL_JsonValue*)mi, GetValueType) ==
                        CMJsonValueLong &&
                  CMCall(dcfg, GetLong, "monitorinterval") == 60,
                  "'Mappers/@monitorInterval' became a number of the "
                  "datasource");
            CHECK(pool != NULL && CMCall(pool, GetType) == CMJsonTypeObject &&
                  CMCall((CMUTIL_JsonObject*)pool, GetCString, "confref")
                        != NULL,
                  "'Pool' became an object");
            CHECK(mappers != NULL &&
                  CMCall(mappers, GetType) == CMJsonTypeArray &&
                  CMCall((CMUTIL_JsonArray*)mappers, GetSize) == 1,
                  "'Mappers' became an array");
            CHECK(dparams != NULL &&
                  CMCall(dparams, GetType) == CMJsonTypeObject &&
                  CMCall((CMUTIL_JsonObject*)dparams, GetCString, "user")
                        != NULL,
                  "'Param' became an entry of the params object");
        }
        {
            CMUTIL_Json *lcfg = CMCall((CMUTIL_JsonObject*)xconf,
                                       Get, "logging");
            CMUTIL_Json *pcs = CMCall((CMUTIL_JsonObject*)xconf,
                                      Get, "poolconfigurations");
            CHECK(lcfg != NULL && CMCall(lcfg, GetType) == CMJsonTypeObject &&
                  CMCall((CMUTIL_JsonObject*)lcfg, GetBoolean, "query")
                        == CMTrue,
                  "'Logging' became an object of booleans");
            CHECK(pcs != NULL && CMCall(pcs, GetType) == CMJsonTypeArray &&
                  CMCall((CMUTIL_JsonArray*)pcs, GetSize) == 1,
                  "'PoolConfigurations' became an array");
        }
        CMUTIL_JsonDestroy(xconf);
    }
    if (xroot) CMCall(xroot, Destroy);

    TESTCASE("the XML configuration builds the same context");
    xctx = CMDBM_ContextCreate("data/mock_conf.xml", "UTF-8", NULL);
    CHECK(xctx != NULL, "a context is created from the XML configuration");
    if (xctx) {
        cparams = stat->params;
        CHECK(cparams != NULL, "the module received parameters");
        if (cparams) {
            CHECK(CMCall(cparams, Get, "type") == NULL,
                  "'type' is not passed");
            CHECK(CMCall(cparams, Get, "id") == NULL, "'id' is not passed");
            CHECK(CMCall(cparams, Get, "charset") == NULL,
                  "'charset' is not passed");
            CHECK(CMCall(cparams, Get, "pool") == NULL,
                  "'pool' is not passed");
            CHECK(CMCall(cparams, Get, "mappers") == NULL,
                  "'mappers' is not passed");
            CHECK(CMCall(cparams, Get, "monitorinterval") == NULL,
                  "'monitorInterval' is not passed");
            CHECK(same_object(jparams, cparams) == CMTrue,
                  "the module got the same parameters as with JSON");
        }

        xqid = xqry = xres = CMFalse;
        CMCall((CMDBM_ContextEx*)xctx, GetLogFlags, &xqid, &xqry, &xres);
        CHECK(xqid == CMFalse && xqry == CMTrue && xres == CMTrue,
              "the logging flags are the same as with JSON");

        sess = CMCall(xctx, GetSession);
        params = CMUTIL_JsonObjectCreate();
        row = CMCall(sess, GetRow, "local", "t.selMap", params);
        CHECK(row != NULL, "a query of the mapper file can be executed");
        if (row) CMUTIL_JsonDestroy(row);
        CMUTIL_JsonDestroy(params);
        CMCall(sess, Close);
        CMCall(xctx, Destroy);
    }

    TESTCASE("an unreadable configuration is an error");
    CHECK(CMDBM_ContextCreate("data/no_such_conf.json", "UTF-8", NULL) == NULL,
          "a missing configuration file fails the context creation");

    if (jparams) CMUTIL_JsonDestroy(jparams);
    CMCall(ctx, Destroy);
    MockDbReset();
    CMDBM_Clear();

    return TESTRESULT();
}

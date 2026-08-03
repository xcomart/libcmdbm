/*
 * resultType and fetchSize of the select tag, against the mock module.
 */
#include "test.h"
#include "mockdb.h"

static CMBool RowCb(CMUTIL_JsonObject *row, uint32_t rownum, void *udata)
{
    int *cnt = (int*)udata;
    CMUTIL_UNUSED(row, rownum);
    (*cnt)++;
    return CMTrue;
}

static uint32_t IterateFetchSize(
        CMDBM_Session *sess, CMUTIL_JsonObject *params, const char *sqlid)
{
    int cnt = 0;
    MockDbStat *stat = MockDbStatOf(0);
    stat->fetchsize = (uint32_t)-1;
    CMCall(sess, ForEachRow, "mock", sqlid, params, &cnt, RowCb);
    return stat->fetchsize;
}

int main(void)
{
    CMDBM_Context *ctx;
    CMDBM_Session *sess;
    CMDBM_Database *db;
    CMDBM_PoolConfig pool = { 30, CMTrue, CMTrue, 1, 2, (char*)"select 1" };
    CMUTIL_JsonObject *cparm, *params;
    CMUTIL_JsonArray *rows;
    CMUTIL_Json *item;
    int cnt = 0;

    CMDBM_Init();
    MockDbRegister();

    ctx = CMDBM_ContextCreate(NULL, "UTF-8", NULL);
    cparm = CMUTIL_JsonObjectCreate();
    db = CMDBM_DatabaseCreate("mock", "MOCK", "utf-8", &pool, cparm);
    CMUTIL_JsonDestroy(cparm);
    if (db == NULL) {
        printf("cannot create mock datasource\n");
        return 1;
    }
    CMCall(db, AddMapper, "data/mapper.xml");
    CMCall(ctx, AddDatabase, db);

    sess = CMCall(ctx, GetSession);
    params = CMUTIL_JsonObjectCreate();

    TESTCASE("resultType=\"value\" on GetRowSet");
    rows = CMCall(sess, GetRowSet, "mock", "t.selValue", params);
    CHECK(rows != NULL, "rowset returned");
    if (rows) {
        CHECK(CMCall(rows, GetSize) == MOCKDB_ROWCNT, "every row is there");
        item = CMCall(rows, Get, 0);
        CHECK(CMCall(item, GetType) == CMJsonTypeValue, "item is a value");
        if (CMCall(item, GetType) == CMJsonTypeValue) {
            CMUTIL_JsonValue *val = (CMUTIL_JsonValue*)item;
            CHECK(CMCall(val, GetLong) == 1, "value is the first column");
        }
        CMUTIL_JsonDestroy(rows);
    }

    TESTCASE("default resultType on GetRowSet");
    rows = CMCall(sess, GetRowSet, "mock", "t.selMap", params);
    CHECK(rows != NULL, "rowset returned");
    if (rows) {
        item = CMCall(rows, Get, 0);
        CHECK(CMCall(item, GetType) == CMJsonTypeObject, "item is an object");
        CMUTIL_JsonDestroy(rows);
    }

    TESTCASE("fetchSize");
    CHECK(IterateFetchSize(sess, params, "t.selFetch") == 50,
          "reaches the module");
    CHECK(IterateFetchSize(sess, params, "t.selMap") == 0,
          "0 when the attribute is missing");
    CHECK(IterateFetchSize(sess, params, "t.selBadFetch") == 0,
          "0 when the attribute is not a number");
    CHECK(IterateFetchSize(sess, params, "t.selKeyFetch") == 0,
          "dropped when a selectKey runs after the statement");

    TESTCASE("cursor iteration");
    CMCall(sess, ForEachRow, "mock", "t.selMap", params, &cnt, RowCb);
    CHECK(cnt == MOCKDB_ROWCNT, "every row is iterated");

    CMUTIL_JsonDestroy(params);
    CMCall(sess, Close);
    CMCall(ctx, Destroy);
    MockDbReset();
    CMDBM_Clear();

    return TESTRESULT();
}

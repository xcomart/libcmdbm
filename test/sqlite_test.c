/*
 * End to end test of the SQLite module against a real database file.
 */
#include "test.h"

#include <stdlib.h>
#include <string.h>

static CMBool RowCb(CMUTIL_JsonObject *row, uint32_t rownum, void *udata)
{
    int *cnt = (int*)udata;
    const char *name = CMCall(row, GetCString, "name");
    (void)rownum;
    if (name) (*cnt)++;
    return CMTrue;
}

static int64_t CountUsers(CMDBM_Session *sess, CMUTIL_JsonObject *params)
{
    int64_t res = -1;
    CMUTIL_JsonValue *v = CMCall(sess, GetObject, "local", "u.countUsers",
                                 params);
    if (v) {
        res = CMCall(v, GetLong);
        CMUTIL_JsonDestroy(v);
    }
    return res;
}

int main(void)
{
    CMDBM_Context *ctx;
    CMDBM_Session *sess;
    CMUTIL_JsonObject *params, *row;
    CMUTIL_JsonArray *rows, *ids;
    CMUTIL_Json *item;
    int cnt = 0, affected;

    remove("sqlite_test.db");
    CMDBM_Init();

    ctx = CMDBM_ContextCreate("data/sqlite_conf.json", "UTF-8", NULL);
    if (ctx == NULL) { printf("context create failed\n"); return 1; }
    sess = CMCall(ctx, GetSession);
    params = CMUTIL_JsonObjectCreate();

    TESTCASE("DDL and insert with bind variables");
    affected = CMCall(sess, Execute, "local", "u.createTable", params);
    CHECK(affected >= 0, "create table");

    CMCall(params, PutString, "name", "alice");
    CMCall(params, PutLong, "age", 30);
    CMCall(params, PutDouble, "score", 91.5);
    affected = CMCall(sess, Execute, "local", "u.insertUser", params);
    CHECK(affected == 1, "insert returns 1 affected row");
    CHECK(CMCall(params, Get, "userId") != NULL,
          "selectKey(AFTER) stored the generated key");

    CMCall(params, PutString, "name", "bob");
    CMCall(params, PutLong, "age", 42);
    CMCall(params, PutDouble, "score", 78.25);
    CMCall(sess, Execute, "local", "u.insertUser", params);
    CMCall(params, PutString, "name", "carol");
    CMCall(params, PutLong, "age", 25);
    CMCall(params, PutNull, "score");
    CMCall(sess, Execute, "local", "u.insertUser", params);
    CHECK(CountUsers(sess, params) == 3, "three rows inserted");

    TESTCASE("GetRow and column types");
    CMCall(params, PutString, "name", "alice");
    row = CMCall(sess, GetRow, "local", "u.selectByName", params);
    CHECK(row != NULL, "row returned");
    if (row) {
        CMUTIL_Json *jage = CMCall(row, Get, "age");
        CMUTIL_Json *jscore = CMCall(row, Get, "score");
        CMUTIL_JsonValue *vage = (CMUTIL_JsonValue*)jage;
        CMUTIL_JsonValue *vscore = (CMUTIL_JsonValue*)jscore;
        CHECK(CMCall(vage, GetValueType) == CMJsonValueLong, "integer column");
        CHECK(CMCall(vage, GetLong) == 30, "integer value");
        CHECK(CMCall(vscore, GetValueType) == CMJsonValueDouble,
              "real column");
        CHECK(CMCall(vscore, GetDouble) > 91.4 &&
              CMCall(vscore, GetDouble) < 91.6, "real value");
        CHECK(strcmp(CMCall(row, GetCString, "name"), "alice") == 0,
              "text value");
        CMUTIL_JsonDestroy(row);
    }

    TESTCASE("null column");
    CMCall(params, PutString, "name", "carol");
    row = CMCall(sess, GetRow, "local", "u.selectByName", params);
    if (row) {
        CMUTIL_Json *jscore = CMCall(row, Get, "score");
        CMUTIL_JsonValue *vscore = (CMUTIL_JsonValue*)jscore;
        CHECK(CMCall(vscore, GetValueType) == CMJsonValueNull, "null column");
        CMUTIL_JsonDestroy(row);
    } else {
        CHECK(CMFalse, "row returned");
    }

    TESTCASE("dynamic sql: <where>/<if> and <foreach>");
    CMUTIL_JsonDestroy(CMCall(params, Remove, "name"));
    CMCall(params, PutLong, "minAge", 28);
    rows = CMCall(sess, GetRowSet, "local", "u.searchUsers", params);
    CHECK(rows != NULL && CMCall(rows, GetSize) == 2, "two rows above 28");
    if (rows) CMUTIL_JsonDestroy(rows);
    CMUTIL_JsonDestroy(CMCall(params, Remove, "minAge"));
    {
        CMUTIL_JsonArray *names = CMUTIL_JsonArrayCreate();
        CMCall(names, AddString, "alice");
        CMCall(names, AddString, "carol");
        CMCall(params, Put, "names", (CMUTIL_Json*)names);
    }
    rows = CMCall(sess, GetRowSet, "local", "u.selectInNames", params);
    CHECK(rows != NULL && CMCall(rows, GetSize) == 2, "foreach in list");
    if (rows) CMUTIL_JsonDestroy(rows);

    TESTCASE("resultType=\"value\"");
    ids = CMCall(sess, GetRowSet, "local", "u.selectNames", params);
    CHECK(ids != NULL && CMCall(ids, GetSize) == 3, "three values");
    if (ids) {
        item = CMCall(ids, Get, 0);
        CHECK(CMCall(item, GetType) == CMJsonTypeValue, "item is a value");
        if (CMCall(item, GetType) == CMJsonTypeValue) {
            CMUTIL_JsonValue *v = (CMUTIL_JsonValue*)item;
            CHECK(strcmp(CMCall(v, GetCString), "alice") == 0,
                  "first value is 'alice'");
        }
        CMUTIL_JsonDestroy(ids);
    }

    TESTCASE("cursor iteration with fetchSize");
    cnt = 0;
    CHECK(CMCall(sess, ForEachRow, "local", "u.selectCursor", params,
                 &cnt, RowCb), "ForEachRow succeeded");
    CHECK(cnt == 3, "three rows iterated");

    TESTCASE("transaction commit");
    CHECK(CMCall(sess, BeginTransaction), "transaction started");
    CMCall(params, PutString, "name", "dave");
    CMCall(params, PutLong, "age", 51);
    CMCall(params, PutDouble, "score", 60.0);
    CMCall(sess, Execute, "local", "u.insertUser", params);
    CHECK(CMCall(sess, Commit), "commit");
    CMCall(sess, EndTransaction);
    CHECK(CountUsers(sess, params) == 4, "committed row is there");

    TESTCASE("transaction rollback");
    CMCall(sess, BeginTransaction);
    CMCall(params, PutString, "name", "erin");
    CMCall(sess, Execute, "local", "u.insertUser", params);
    CHECK(CountUsers(sess, params) == 5, "row visible inside transaction");
    CMCall(sess, Rollback);
    CMCall(sess, EndTransaction);
    CHECK(CountUsers(sess, params) == 4, "rolled back row is gone");

    TESTCASE("transaction started before any statement");
    CMCall(sess, Close);
    sess = CMCall(ctx, GetSession);
    CHECK(CMCall(sess, BeginTransaction), "transaction started on new session");
    CMCall(params, PutString, "name", "frank");
    CMCall(sess, Execute, "local", "u.insertUser", params);
    CMCall(sess, Rollback);
    CMCall(sess, EndTransaction);
    CHECK(CountUsers(sess, params) == 4,
          "lazily borrowed connection joined the transaction");

    TESTCASE("session closed inside a transaction rolls back");
    CMCall(sess, BeginTransaction);
    CMCall(params, PutString, "name", "grace");
    CMCall(sess, Execute, "local", "u.insertUser", params);
    CMCall(sess, Close);
    sess = CMCall(ctx, GetSession);
    CHECK(CountUsers(sess, params) == 4, "uncommitted row is gone");

    CMUTIL_JsonDestroy(params);
    CMCall(sess, Close);
    CMCall(ctx, Destroy);
    CMDBM_Clear();

    return TESTRESULT();
}

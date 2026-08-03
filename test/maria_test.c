/*
 * End to end test of the MySQL/MariaDB module against a real server.
 *
 * Skips itself - exit code 77 - when the CMDBM_TEST_MARIA_* variables do
 * not describe a server to talk to. test/docker/env.sh sets them for the
 * server of test/docker/compose.yml.
 */
#include "dbtest.h"

#define CONFFILE    "maria_test_conf.json"

// MariaDB and MySQL are one module registered under two keys.
#ifndef CMDBM_TEST_DBTYPE
#define CMDBM_TEST_DBTYPE   "MARIA"
#endif

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
    CMUTIL_JsonValue *v = CMCall(sess, GetObject, TESTDBID, "u.countUsers",
                                 params);
    if (v) {
        res = CMCall(v, GetLong);
        CMUTIL_JsonDestroy(v);
    }
    return res;
}

static int InsertUser(CMDBM_Session *sess, CMUTIL_JsonObject *params,
                      const char *name, int64_t age, CMBool active)
{
    CMCall(params, PutString, "name", name);
    CMCall(params, PutLong, "age", age);
    CMCall(params, PutBoolean, "active", active);
    return CMCall(sess, Execute, TESTDBID, "u.insertUser", params);
}

static void DropSchema(CMDBM_Session *sess, CMUTIL_JsonObject *params)
{
    CMCall(sess, Execute, TESTDBID, "u.dropProcedure", params);
    CMCall(sess, Execute, TESTDBID, "u.dropTable", params);
}

int main(void)
{
    CMDBM_Context *ctx;
    CMDBM_Session *sess;
    CMUTIL_JsonObject *params, *row;
    CMUTIL_JsonArray *rows, *names;
    CMUTIL_Json *item;
    int cnt = 0, affected;

    if (!DbTestWriteConf("MARIA", CMDBM_TEST_DBTYPE,
                         "data/maria_mapper.xml", CONFFILE))
        return TESTSKIP;

    CMDBM_Init();
    ctx = CMDBM_ContextCreate(CONFFILE, "UTF-8", NULL);
    if (ctx == NULL) { printf("context create failed\n"); return 1; }
    sess = CMCall(ctx, GetSession);
    params = CMUTIL_JsonObjectCreate();

    TESTCASE("schema setup");
    DropSchema(sess, params);
    CHECK(CMCall(sess, Execute, TESTDBID, "u.createTable", params) >= 0,
          "create table");
    CHECK(CMCall(sess, Execute, TESTDBID, "u.createProcedure", params) >= 0,
          "create procedure");

    TESTCASE("insert with bind variables and selectKey(AFTER)");
    CMCall(params, PutDouble, "score", 91.5);
    affected = InsertUser(sess, params, "alice", 30, CMTrue);
    CHECK(affected == 1, "insert returns 1 affected row");
    CHECK(CMCall(params, Get, "userId") != NULL,
          "selectKey(AFTER) stored the generated key");

    CMCall(params, PutDouble, "score", 78.25);
    InsertUser(sess, params, "bob", 42, CMFalse);
    CMCall(params, PutNull, "score");
    InsertUser(sess, params, "carol", 25, CMTrue);
    CHECK(CountUsers(sess, params) == 3, "three rows inserted");

    TESTCASE("GetRow and column types");
    CMCall(params, PutString, "name", "alice");
    row = CMCall(sess, GetRow, TESTDBID, "u.selectByName", params);
    CHECK(row != NULL, "row returned");
    if (row) {
        CMUTIL_JsonValue *vage = (CMUTIL_JsonValue*)CMCall(row, Get, "age");
        CMUTIL_JsonValue *vscr = (CMUTIL_JsonValue*)CMCall(row, Get, "score");
        CMUTIL_JsonValue *vact = (CMUTIL_JsonValue*)CMCall(row, Get, "active");
        const char *name = CMCall(row, GetCString, "name");
        double score = CMCall(vscr, GetDouble);
        CHECK(CMCall(vage, GetValueType) == CMJsonValueLong, "bigint column");
        CHECK(CMCall(vage, GetLong) == 30, "bigint value");
        CHECK(CMCall(vscr, GetValueType) == CMJsonValueDouble,
              "double column");
        CHECK(score > 91.4 && score < 91.6, "double value");
        // 'boolean' is an alias of tinyint(1), which comes back as a number.
        CHECK(CMCall(vact, GetLong) == 1, "boolean column");
        CHECK(name && strcmp(name, "alice") == 0, "varchar value");
        CMUTIL_JsonDestroy(row);
    }

    TESTCASE("null column");
    CMCall(params, PutString, "name", "carol");
    row = CMCall(sess, GetRow, TESTDBID, "u.selectByName", params);
    if (row) {
        CMUTIL_JsonValue *vscr = (CMUTIL_JsonValue*)CMCall(row, Get, "score");
        CHECK(CMCall(vscr, GetValueType) == CMJsonValueNull, "null column");
        CMUTIL_JsonDestroy(row);
    } else {
        CHECK(CMFalse, "row returned");
    }

    TESTCASE("dynamic sql: <where>/<if> and <foreach>");
    CMUTIL_JsonDestroy(CMCall(params, Remove, "name"));
    CMCall(params, PutLong, "minAge", 28);
    rows = CMCall(sess, GetRowSet, TESTDBID, "u.searchUsers", params);
    CHECK(rows != NULL && CMCall(rows, GetSize) == 2, "two rows above 28");
    if (rows) CMUTIL_JsonDestroy(rows);
    CMUTIL_JsonDestroy(CMCall(params, Remove, "minAge"));
    {
        CMUTIL_JsonArray *innames = CMUTIL_JsonArrayCreate();
        CMCall(innames, AddString, "alice");
        CMCall(innames, AddString, "carol");
        CMCall(params, Put, "names", (CMUTIL_Json*)innames);
    }
    rows = CMCall(sess, GetRowSet, TESTDBID, "u.selectInNames", params);
    CHECK(rows != NULL && CMCall(rows, GetSize) == 2, "foreach in list");
    if (rows) CMUTIL_JsonDestroy(rows);

    TESTCASE("resultType=\"value\"");
    names = CMCall(sess, GetRowSet, TESTDBID, "u.selectNames", params);
    CHECK(names != NULL && CMCall(names, GetSize) == 3, "three values");
    if (names) {
        item = CMCall(names, Get, 0);
        CHECK(CMCall(item, GetType) == CMJsonTypeValue, "item is a value");
        if (CMCall(item, GetType) == CMJsonTypeValue) {
            CMUTIL_JsonValue *v = (CMUTIL_JsonValue*)item;
            const char *sv = CMCall(v, GetCString);
            CHECK(strcmp(sv, "alice") == 0, "first value is 'alice'");
        }
        CMUTIL_JsonDestroy(names);
    }

    TESTCASE("cursor iteration with fetchSize");
    cnt = 0;
    CHECK(CMCall(sess, ForEachRow, TESTDBID, "u.selectCursor", params,
                 &cnt, RowCb), "ForEachRow succeeded");
    CHECK(cnt == 3, "three rows iterated");

    TESTCASE("transaction commit");
    CHECK(CMCall(sess, BeginTransaction), "transaction started");
    CMCall(params, PutDouble, "score", 60.0);
    InsertUser(sess, params, "dave", 51, CMTrue);
    CHECK(CMCall(sess, Commit), "commit");
    CMCall(sess, EndTransaction);
    CHECK(CountUsers(sess, params) == 4, "committed row is there");

    TESTCASE("transaction rollback");
    CMCall(sess, BeginTransaction);
    InsertUser(sess, params, "erin", 33, CMFalse);
    CHECK(CountUsers(sess, params) == 5, "row visible inside transaction");
    CMCall(sess, Rollback);
    CMCall(sess, EndTransaction);
    CHECK(CountUsers(sess, params) == 4, "rolled back row is gone");

    TESTCASE("OUT parameters of a procedure");
    CMCall(params, PutLong, "a", 3);
    CMCall(params, PutLong, "b", 4);
    CMCall(params, PutLong, "total", 0);
    CMCall(params, PutString, "label", "");
    affected = CMCall(sess, Execute, TESTDBID, "u.callSum", params);
    CHECK(affected >= 0, "call succeeded");
    {
        CMUTIL_JsonValue *vtot =
                (CMUTIL_JsonValue*)CMCall(params, Get, "total");
        const char *label = CMCall(params, GetCString, "label");
        CHECK(vtot && CMCall(vtot, GetLong) == 7,
              "OUT parameter 'total' came back as 7");
        CHECK(label && strcmp(label, "sum of 3 and 4") == 0,
              "OUT parameter 'label' came back as text");
    }

    DropSchema(sess, params);
    CMUTIL_JsonDestroy(params);
    CMCall(sess, Close);
    CMCall(ctx, Destroy);
    CMDBM_Clear();
    remove(CONFFILE);

    return TESTRESULT();
}

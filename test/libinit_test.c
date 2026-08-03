/*
 * LibraryInit and LibraryClear of a module must run once per client
 * library, however many datasources of that type exist.
 */
#include "test.h"
#include "mockdb.h"

static CMDBM_Database *MakeDb(const char *id, const char *type)
{
    CMDBM_PoolConfig pool = { 30, CMTrue, CMTrue, 1, 2, (char*)"select 1" };
    CMUTIL_JsonObject *cparm = CMUTIL_JsonObjectCreate();
    CMDBM_Database *db = CMDBM_DatabaseCreate(id, type, "utf-8", &pool, cparm);
    CMUTIL_JsonDestroy(cparm);
    if (db) CMCall(db, AddMapper, "data/mapper.xml");
    return db;
}

static void AddDb(CMDBM_Context *ctx, const char *id, const char *type)
{
    CMDBM_Database *db = MakeDb(id, type);
    if (db) CMCall(ctx, AddDatabase, db);
}

int main(void)
{
    CMDBM_Context *ctx, *ctx2;
    MockDbStat *one, *two;

    CMDBM_Init();
    MockDbRegister();
    one = MockDbStatOf(0);
    two = MockDbStatOf(1);

    TESTCASE("several datasources of one library");
    ctx = CMDBM_ContextCreate(NULL, "UTF-8", NULL);
    AddDb(ctx, "a", "MOCK");
    CHECK(one->libinit == 1, "initialized by the first datasource");
    AddDb(ctx, "b", "MOCK");
    CHECK(one->libinit == 1, "not again for a second datasource");
    AddDb(ctx, "c", "MOCKALIAS");
    CHECK(one->libinit == 1, "not again for the same module under a "
                             "second key");
    CHECK(one->libclear == 0, "not cleaned up while in use");

    TESTCASE("an independent library");
    AddDb(ctx, "d", "MOCK2");
    CHECK(two->libinit == 1, "initialized once");
    CHECK(one->libinit == 1, "the other library is untouched");

    TESTCASE("another context");
    ctx2 = CMDBM_ContextCreate(NULL, "UTF-8", NULL);
    AddDb(ctx2, "e", "MOCK");
    CHECK(one->libinit == 1, "shares the library of the first context");

    TESTCASE("cleanup");
    CMCall(ctx, Destroy);
    CHECK(one->libclear == 0, "still used by the other context");
    CHECK(two->libclear == 1, "the unused library is cleaned up once");
    CMCall(ctx2, Destroy);
    CHECK(one->libclear == 1, "cleaned up when the last datasource is gone");

    TESTCASE("use after everything was released");
    ctx = CMDBM_ContextCreate(NULL, "UTF-8", NULL);
    AddDb(ctx, "f", "MOCK");
    CHECK(one->libinit == 2, "initialized again");
    CMCall(ctx, Destroy);
    CHECK(one->libclear == 2, "and cleaned up again");

    MockDbReset();
    CMDBM_Clear();

    return TESTRESULT();
}

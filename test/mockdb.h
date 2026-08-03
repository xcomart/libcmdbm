/*
 * A DBMS module which answers from memory, so the core can be tested
 * without a database server.
 */
#ifndef CMDBM_TEST_MOCKDB_H__
#define CMDBM_TEST_MOCKDB_H__

#include <libcmdbm.h>

/* two independent modules: 'MOCK' and 'MOCK2' */
extern CMDBM_ModuleInterface g_mockdb_interface;
extern CMDBM_ModuleInterface g_mockdb2_interface;

#define MOCKDB_ROWCNT   3

typedef struct MockDbStat {
    int                 libinit;        /* LibraryInit call count       */
    int                 libclear;       /* LibraryClear call count      */
    int                 opened;         /* opened connections           */
    int                 closed;         /* closed connections           */
    int                 onevalue;       /* GetOneValue call count       */
    uint32_t            fetchsize;      /* fetchSize of the last cursor */
    char                lastqry[256];   /* SQL of the last GetOneValue  */
    CMUTIL_JsonObject   *params;        /* last connection parameters   */
} MockDbStat;

/* distinct per module, so that a pool test can tell which one it ran */
#define MOCKDB_TESTQUERY    "select 1 from mock"
#define MOCKDB2_TESTQUERY   "select 1 from mock2"

/* index 0 is 'MOCK', index 1 is 'MOCK2' */
MockDbStat *MockDbStatOf(int index);

/* registers both modules, must be called after CMDBM_Init */
void MockDbRegister(void);

/* clears the counters and releases the recorded parameters */
void MockDbReset(void);

#endif // CMDBM_TEST_MOCKDB_H__

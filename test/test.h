#ifndef CMDBM_TEST_H__
#define CMDBM_TEST_H__

#include <libcmdbm.h>
#include <stdio.h>

static int g_test_failed = 0;

#define TESTCASE(name)  printf("[ %s ]\n", name)

#define CHECK(cond, msg) do {                                   \
    if (cond) {                                                 \
        printf("  ok   - %s\n", msg);                           \
    } else {                                                    \
        printf("  FAIL - %s (%s:%d)\n", msg, __FILE__, __LINE__);\
        g_test_failed++;                                        \
    }                                                           \
} while(0)

#define TESTRESULT() (                                          \
    printf(g_test_failed?                                       \
           "\nFAILED(%d)\n":"\nALL PASSED(%d)\n", g_test_failed),\
    g_test_failed? 1:0)

#endif // CMDBM_TEST_H__

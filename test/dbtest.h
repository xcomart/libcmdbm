#ifndef CMDBM_DBTEST_H__
#define CMDBM_DBTEST_H__

/*
 * Shared scaffolding of the integration tests which need a database server.
 */
#include "test.h"

#include <stdlib.h>
#include <string.h>

// the exit code ctest reads as "this test skipped itself". declared with
// SKIP_RETURN_CODE in test/CMakeLists.txt.
#define TESTSKIP    77

// the id every integration test gives its datasource.
#define TESTDBID    "testdb"

/*
 * Writes the configuration of the datasource under test, built from the
 * CMDBM_TEST_<prefix>_HOST/PORT/DB/USER/PASSWORD environment variables -
 * test/docker/env.sh exports them for the servers of the compose file.
 *
 * Returns CMFalse when they are not set, which is how a test without a
 * server to talk to skips itself.
 */
static CMBool DbTestWriteConf(
        const char *prefix, const char *dbtype,
        const char *mapper, const char *confpath)
{
    static const char *suffixes[] = {
        "HOST", "PORT", "DB", "USER", "PASSWORD", NULL };
    const char *values[5];
    char name[64];
    FILE *fp;
    int i;

    for (i=0; suffixes[i]; i++) {
        sprintf(name, "CMDBM_TEST_%s_%s", prefix, suffixes[i]);
        values[i] = getenv(name);
        if (values[i] == NULL || *values[i] == 0x0) {
            printf("%s is not set: no %s server to test against.\n",
                   name, dbtype);
            return CMFalse;
        }
    }

    fp = fopen(confpath, "w");
    if (fp == NULL) {
        printf("cannot write the configuration file '%s'.\n", confpath);
        return CMFalse;
    }
    fprintf(fp,
            "{\n"
            "    \"databases\": [\n"
            "        {\n"
            "            \"type\": \"%s\",\n"
            "            \"id\": \"" TESTDBID "\",\n"
            "            \"charset\": \"UTF-8\",\n"
            "            \"host\": \"%s\",\n"
            "            \"port\": \"%s\",\n"
            "            \"database\": \"%s\",\n"
            "            \"user\": \"%s\",\n"
            "            \"password\": \"%s\",\n"
            "            \"pool\": { \"initCount\": 1, \"maxCount\": 3 },\n"
            "            \"mappers\": [\n"
            "                { \"type\": \"mapper\", \"filePath\": \"%s\" }\n"
            "            ]\n"
            "        }\n"
            "    ]\n"
            "}\n",
            dbtype, values[0], values[1], values[2], values[3], values[4],
            mapper);
    fclose(fp);
    return CMTrue;
}

#endif // CMDBM_DBTEST_H__

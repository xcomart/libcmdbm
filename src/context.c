
#include "functions.h"

CMUTIL_LogDefine("cmdbm.context")

typedef struct CMDBM_Context_Internal {
    CMDBM_ContextEx base;
    CMUTIL_Map      *databases;
    CMUTIL_Timer    *timer;
    char            *progcs;        // program character set
    CMUTIL_Map      *poolconfs;
    CMBool          istimerinternal;
    CMBool          logqueryid;
    CMBool          logquery;
    CMBool          logresult;
} CMDBM_Context_Internal;

CMDBM_STATIC void CMDBM_ContextDatabaseDestroyer(void *data)
{
    CMDBM_Database *db = (CMDBM_Database*)data;
    if (db)
        CMCall(db, Destroy);
}

CMDBM_STATIC void CMDBM_ContextPoolConfDestroyer(void *data)
{
    CMDBM_PoolConfig *pconf = (CMDBM_PoolConfig*)data;
    if (pconf) {
        if (pconf->testsql) CMFree(pconf->testsql);
        CMFree(pconf);
    }
}

CMDBM_STATIC void CMDBM_ContextConfigClean(CMUTIL_Json *json)
{
    uint32_t i;
    switch (CMCall(json, GetType)) {
    case CMJsonTypeObject: {
        CMUTIL_JsonObject *jobj = (CMUTIL_JsonObject*)json;
        CMUTIL_StringArray *keys = CMCall(jobj, GetKeys);
        for (i=0; i<CMCall(keys, GetSize); i++) {
            const CMUTIL_String *str = CMCall(keys, GetAt, i);
            const char *key = CMCall(str, GetCString);
            CMUTIL_Json *item = CMCall(jobj, Remove, key);
            // change internal buffer to lowercase
            CMCall((CMUTIL_String*)str, SelfToLower);
            // change item recursively.
            CMDBM_ContextConfigClean(item);
            // 'key' is changed to lowercase already by calling SelfToLower.
            // so just put with it.
            CMCall(jobj, Put, key, item);
        }
        CMCall(keys, Destroy);
        break;
    }
    case CMJsonTypeArray: {
        CMUTIL_JsonArray *jarr = (CMUTIL_JsonArray*)json;
        for (i=0; i<CMCall(jarr, GetSize); i++)
            // change item recursively.
            CMDBM_ContextConfigClean(CMCall(jarr, Get, i));
        break;
    }
    default: return;
    }
}

CMDBM_STATIC CMBool CMDBM_ContextParsePoolConfig(
        CMDBM_Context *context,
        CMUTIL_JsonObject *pcfg)
{
    CMBool res = CMFalse;
    CMDBM_Context_Internal *ictx = (CMDBM_Context_Internal*)context;
    CMUTIL_JsonValue *id =
            (CMUTIL_JsonValue*)CMCall(pcfg, Get, "id");
    CMUTIL_JsonValue *testsql =
            (CMUTIL_JsonValue*)CMCall(pcfg, Get, "testsql");
    if (id) {
        const char *sid = CMCall(id, GetCString);
        CMDBM_PoolConfig *poolconf = CMAlloc(sizeof(CMDBM_PoolConfig));

        memset(poolconf, 0x0, sizeof(CMDBM_PoolConfig));
        if (CMCall(pcfg, Get, "initcount"))
            poolconf->initcnt = (uint32_t)CMCall(pcfg, GetLong, "initcount");
        else
            poolconf->initcnt = 5;
        if (CMCall(pcfg, Get, "maxcount"))
            poolconf->maxcnt = (uint32_t)CMCall(pcfg, GetLong, "maxcount");
        else
            poolconf->maxcnt = 20;
        if (CMCall(pcfg, Get, "pinginterval"))
            poolconf->pingterm =
                    (uint32_t)CMCall(pcfg, GetLong, "pinginterval");
        else
            poolconf->pingterm = 30;
        if (CMCall(pcfg, Get, "pingtest"))
            poolconf->pingtest = CMCall(pcfg, GetBoolean, "pingtest");
        else
            poolconf->pingtest = CMTrue;
        if (CMCall(pcfg, Get, "testonborrow"))
            poolconf->testonborrow = CMCall(pcfg, GetBoolean, "testonborrow");
        else
            poolconf->testonborrow = CMTrue;
        // no test statement here means the one of the DBMS module.
        poolconf->testsql = testsql?
                    CMStrdup(CMCall(testsql, GetCString)):NULL;

        CMCall(ictx->poolconfs, Put, sid, poolconf, NULL);
        res = CMTrue;
    } else {
        CMLogError("PoolConfig does not have 'id' attribute.");
    }
    return res;
}

CMDBM_STATIC CMBool CMDBM_ContextParseMappers(
        CMDBM_Database *db, CMUTIL_JsonArray *mappers)
{
    uint32_t i;
    CMBool res = CMTrue;
    for (i=0; res && i<CMCall(mappers, GetSize); i++) {
        CMUTIL_JsonObject *mcfg =
                (CMUTIL_JsonObject*)CMCall(mappers, Get, i);
        const char *stype = CMCall(mcfg, GetCString, "type");
        if (stype == NULL) {
            CMLogErrorS("mapper configuration has no 'type' attribute.");
            res = CMFalse;
        } else if (strcasecmp(stype, "mapperSet") == 0) {
            // add mapper set
            const char *bpath = CMCall(mcfg, GetCString, "basepath");
            const char *fpattern = CMCall(mcfg, GetCString, "filepattern");
            CMBool recur = CMCall(mcfg, GetBoolean, "recursive");
            res = CMCall(db, AddMapperSet, bpath, fpattern, recur);
        } else if (strcasecmp(stype, "mapper") == 0) {
            // add mapper
            const char *fpath = CMCall(mcfg, GetCString, "filepath");
            res = CMCall(db, AddMapper, fpath);
        } else {
            CMLogErrorS("unknown mapper type: %s", stype);
            res = CMFalse;
        }
    }
    return res;
}

// keys of a datasource configuration which describe the datasource itself,
// not the connection. all configuration keys are lowercased before use.
CMDBM_STATIC CMBool CMDBM_ContextIsReservedKey(const char *key)
{
    static const char *reserved[] = {
        "type", "id", "charset", "pool", "mappers", "params",
        "monitorinterval", NULL
    };
    const char **p;
    for (p = reserved; *p; p++)
        if (strcmp(*p, key) == 0)
            return CMTrue;
    return CMFalse;
}

CMDBM_STATIC CMBool CMDBM_ContextParseDatabase(
        CMDBM_Context *context,
        const char *sdbtype,
        CMUTIL_JsonObject *dcfg)
{
    CMBool res = CMFalse;
    CMDBM_Context_Internal *ictx = (CMDBM_Context_Internal*)context;
    CMUTIL_JsonObject *pcfg =
            (CMUTIL_JsonObject*)CMCall(dcfg, Get, "pool");
    CMUTIL_JsonValue *pref = pcfg?
            (CMUTIL_JsonValue*)CMCall(pcfg, Get, "confref"):NULL;
    CMUTIL_JsonValue *testsql = pcfg?
            (CMUTIL_JsonValue*)CMCall(pcfg, Get, "testsql"):NULL;
    CMUTIL_JsonValue *id =
            (CMUTIL_JsonValue*)CMCall(dcfg, Get, "id");
    CMUTIL_JsonValue *charset =
            (CMUTIL_JsonValue*)CMCall(dcfg, Get, "charset");
    CMUTIL_Json *minterval = CMCall(dcfg, Get, "monitorinterval");
    const char *sid = NULL, *scharset = NULL;
    CMDBM_Database *db = NULL;
    CMDBM_PoolConfig *pconf = NULL;
    CMUTIL_JsonObject *param = NULL;
    CMUTIL_StringArray *keys = NULL;
    uint32_t i;

    if (id == NULL) {
        CMLogErrorS("There is no 'id' attribute in '%s' type datasource.",
                    sdbtype);
        goto ENDPOINT;
    }
    sid = CMCall(id, GetCString);

    if (charset == NULL)
        scharset = "utf-8";
    else
        scharset = CMCall(charset, GetCString);

    pconf = CMAlloc(sizeof(CMDBM_PoolConfig));

    if (pref) {
        const char *refkey = CMCall(pref, GetCString);
        CMDBM_PoolConfig *ref = (CMDBM_PoolConfig*)CMCall(
                    ictx->poolconfs, Get, refkey);
        if (ref == NULL) {
            CMLogErrorS("pool config reference '%s' not found.", refkey);
            goto ENDPOINT;
        }
        // the referenced configuration owns its own test statement, this
        // one needs a copy of whichever it ends up with.
        memcpy(pconf, ref, sizeof(CMDBM_PoolConfig));
        if (testsql)
            pconf->testsql = CMStrdup(CMCall(testsql, GetCString));
        else if (pconf->testsql)
            pconf->testsql = CMStrdup(pconf->testsql);
    } else {
        memset(pconf, 0x0, sizeof(CMDBM_PoolConfig));
        pconf->initcnt = 5;
        pconf->maxcnt = 20;
        pconf->pingterm = 30;
        pconf->pingtest = CMTrue;
        pconf->testonborrow = CMTrue;
        // no test statement here means the one of the DBMS module.
        pconf->testsql = testsql?
                    CMStrdup(CMCall(testsql, GetCString)):NULL;
    }

    if (pcfg) {
        if (CMCall(pcfg, Get, "initcount"))
            pconf->initcnt = (uint32_t)CMCall(pcfg, GetLong, "initcount");

        if (CMCall(pcfg, Get, "maxcount"))
            pconf->maxcnt = (uint32_t)CMCall(pcfg, GetLong, "maxcount");

        if (CMCall(pcfg, Get, "pinginterval"))
            pconf->pingterm =(uint32_t)CMCall(pcfg, GetLong, "pinginterval");

        if (CMCall(pcfg, Get, "pingtest"))
            pconf->pingtest = CMCall(pcfg, GetBoolean, "pingtest");

        if (CMCall(pcfg, Get, "testonborrow"))
            pconf->testonborrow = CMCall(pcfg, GetBoolean, "testonborrow");
    }

    if (CMCall(dcfg, Get, "params")) {
        CMUTIL_Json *json = CMCall(dcfg, Get, "params");
        param = (CMUTIL_JsonObject*)CMCall(json, Clone);
    } else {
        param = CMUTIL_JsonObjectCreate();
    }
    keys = CMCall(dcfg, GetKeys);
    for (i=0; i<CMCall(keys, GetSize); i++) {
        const char *key = CMCall(keys, GetCString, i);
        CMUTIL_Json *item = CMCall(dcfg, Get, key);
        CMJsonType type = CMCall(item, GetType);
        // datasource meta attributes are not connection parameters.
        // (modules like PgSQL pass every parameter to the client library,
        //  which rejects unknown keywords.)
        if (CMDBM_ContextIsReservedKey(key))
            continue;
        if (type == CMJsonTypeValue) {
            CMUTIL_Json *nitem = CMCall(item, Clone);
            CMCall(param, Put, key, nitem);
        }
    }
    CMCall(keys, Destroy);

    db = CMDBM_DatabaseCreate(sid, sdbtype, scharset, pconf, param);
    if (!db) {
        CMLogErrorS("cannot create database(%s)", sid);
        goto ENDPOINT;
    }

    // parse mappers
    if (CMCall(dcfg, Get, "mappers")) {
        CMUTIL_Json *mappers = CMCall(dcfg, Get, "mappers");
        if (CMCall(mappers, GetType) == CMJsonTypeArray) {
            if (!CMDBM_ContextParseMappers(db, (CMUTIL_JsonArray*)mappers)) {
                goto ENDPOINT;
            }
        } else {
            CMLogError("invalid mapper configuration. for database %s", sid);
            goto ENDPOINT;
        }
    } else {
        CMLogWarn("database(%s) has no mappers. "
                  "any request on this database will be failed.", sid);
    }

    // how often the mapper files are rescanned. it must be set before the
    // datasource is added to the context: that is where the reloader task
    // is scheduled. without it the datasource keeps its default of 30s.
    if (minterval) {
        if (CMCall(minterval, GetType) == CMJsonTypeValue) {
            int64_t ival = CMCall((CMUTIL_JsonValue*)minterval, GetLong);
            if (ival > 0)
                CMCall(db, SetMonitor, (int)ival);
            else
                CMLogWarn("'monitorInterval' of the datasource(%s) must be "
                          "positive. the default is kept.", sid);
        } else {
            CMLogWarn("'monitorInterval' of the datasource(%s) is not a "
                      "scalar value. the default is kept.", sid);
        }
    }

    if (!CMCall(context, AddDatabase, db)) {
        CMLogErrorS("database(%s) cannot be added to context.", sid);
        goto ENDPOINT;
    }

    res = CMTrue;
ENDPOINT:
    if (pconf) CMDBM_ContextPoolConfDestroyer(pconf);
    if (param)
        CMUTIL_JsonDestroy(param);
    if (!res && db)
        CMCall(db, Destroy);
    return res;
}

// a logging flag accepts a JSON boolean and the strings "true"/"false"
// as well, so that a configuration converted from the XML form (where every
// attribute is a string) keeps working.
CMDBM_STATIC CMBool CMDBM_ContextParseLogFlag(
        CMUTIL_JsonObject *lcfg, const char *key, CMBool defval)
{
    const char *sdef = defval? "true":"false";
    CMUTIL_Json *item = CMCall(lcfg, Get, key);
    CMUTIL_JsonValue *jval = NULL;
    const char *sval = NULL;

    if (item == NULL)
        return defval;
    if (CMCall(item, GetType) != CMJsonTypeValue) {
        CMLogWarn("'logging.%s' is not a scalar value. %s is used.",
                  key, sdef);
        return defval;
    }
    jval = (CMUTIL_JsonValue*)item;
    if (CMCall(jval, GetValueType) == CMJsonValueBoolean)
        return CMCall(jval, GetBoolean);
    if (CMCall(jval, GetValueType) == CMJsonValueString) {
        sval = CMCall(jval, GetCString);
        if (sval) {
            if (strcasecmp(sval, "true") == 0)
                return CMTrue;
            if (strcasecmp(sval, "false") == 0)
                return CMFalse;
        }
    }
    CMLogWarn("unknown value of 'logging.%s': '%s'. %s is used.",
              key, sval? sval:"(not a boolean)", sdef);
    return defval;
}

// the 'Logging' section is optional: an absent or malformed one leaves the
// defaults set by CMDBM_ContextInitialize in place, it is not a parse error.
CMDBM_STATIC void CMDBM_ContextParseLogging(
        CMDBM_Context_Internal *ictx, CMUTIL_JsonObject *jconf)
{
    CMUTIL_JsonObject *lcfg = NULL;
    CMUTIL_Json *item = CMCall(jconf, Get, "logging");

    if (item == NULL)
        return;
    if (CMCall(item, GetType) != CMJsonTypeObject) {
        CMLogWarn("'Logging' configuration is not an object. ignored.");
        return;
    }
    lcfg = (CMUTIL_JsonObject*)item;
    ictx->logqueryid = CMDBM_ContextParseLogFlag(
                lcfg, "queryid", ictx->logqueryid);
    ictx->logquery = CMDBM_ContextParseLogFlag(
                lcfg, "query", ictx->logquery);
    ictx->logresult = CMDBM_ContextParseLogFlag(
                lcfg, "result", ictx->logresult);
}

CMDBM_STATIC CMBool CMDBM_ContextParseConfig(
        CMDBM_Context *context,
        CMUTIL_Json *config)
{
    uint32_t i;
    CMBool res = CMFalse;
    CMUTIL_Json *item = NULL;
    CMUTIL_JsonArray *jarr = NULL;
    CMUTIL_JsonObject *jconf = NULL;
    CMUTIL_String *type = NULL, *ltype = NULL;

    if (CMCall(config, GetType) != CMJsonTypeObject) {
        CMLogErrorS("invalid configuration structure.");
        goto ENDPOINT;
    }

    jconf = (CMUTIL_JsonObject*)config;

    CMDBM_ContextConfigClean(config);

    // load pool config
    item = CMCall(jconf, Get, "poolconfigurations");
    if (item) {
        if (CMCall(item, GetType) != CMJsonTypeArray) {
            CMLogErrorS("invalid configuration structure.");
            goto ENDPOINT;
        }
        jarr = (CMUTIL_JsonArray*)item;
        for (i=0; i<CMCall(jarr, GetSize); i++) {
            CMUTIL_JsonObject *pcfg = NULL;

            item = CMCall(jarr, Get, i);
            if (CMCall(item, GetType) != CMJsonTypeObject) {
                CMLogErrorS("invalid configuration structure.");
                goto ENDPOINT;
            }

            pcfg = (CMUTIL_JsonObject*)item;;
            if (!CMDBM_ContextParsePoolConfig(context, pcfg)) {
                CMLogErrorS("pool configuration parse failed.");
                goto ENDPOINT;
            }
        }
    }

    // load database config
    item = CMCall((CMUTIL_JsonObject*)config, Get, "databases");
    if (!item) {
        CMLogErrorS("database configuration not found.");
        goto ENDPOINT;
    }
    if (CMCall(item, GetType) != CMJsonTypeArray) {
        CMLogErrorS("invalid configuration structure.");
        goto ENDPOINT;
    }
    jarr = (CMUTIL_JsonArray*)item;
    for (i=0; i<CMCall(jarr, GetSize); i++) {
        CMUTIL_JsonObject *dcfg = NULL;

        item = CMCall(jarr, Get, i);
        if (CMCall(item, GetType) != CMJsonTypeObject) {
            CMLogErrorS("invalid configuration structure.");
            goto ENDPOINT;
        }

        dcfg = (CMUTIL_JsonObject*)item;;
        type = (CMUTIL_String*)CMCall(dcfg, GetString, "type");
        if (type == NULL) {
            CMLogErrorS("database item does not have 'type' property.");
            goto ENDPOINT;
        }
        ltype = CMCall(type, ToLower);
        if (!CMDBM_ContextParseDatabase(
                    context, CMCall(ltype, GetCString), dcfg)) {
            CMLogError("database configuration parse failed.");
            goto ENDPOINT;
        }
        CMCall(ltype, Destroy);
        ltype = NULL;
    }

    // load logging config
    CMDBM_ContextParseLogging((CMDBM_Context_Internal*)context, jconf);

    res = CMTrue;
ENDPOINT:
    if (ltype) CMCall(ltype, Destroy);

    return res;
}

// the format of a configuration file is decided by its content, not by its
// name: an XML document begins with its declaration or with an element, a
// JSON document with a brace. the file name of the public API is spelled
// 'confjson' for compatibility, it takes either form.
CMDBM_STATIC CMUTIL_Json *CMDBM_ContextLoadConfig(const char *confpath)
{
    CMUTIL_Json *res = NULL;
    CMUTIL_File *cfile = CMUTIL_FileCreate(confpath);
    CMUTIL_String *content = cfile? CMCall(cfile, GetContents):NULL;

    if (content) {
        const char *p = CMCall(content, GetCString);
        while (*p && strchr(CMDBM_SPACES, *p)) p++;
        if (*p == '<') {
            CMUTIL_XmlNode *root = CMUTIL_XmlParse(content);
            if (root) {
                res = CMDBM_ConfigFromXml(root);
                CMCall(root, Destroy);
            }
            if (res)
                CMLogDebug("configuration '%s' read as XML.", confpath);
            else
                CMLogError("cannot parse '%s' as an XML configuration.",
                           confpath);
        } else {
            res = CMUTIL_JsonParse(content);
            if (res)
                CMLogDebug("configuration '%s' read as JSON.", confpath);
            else
                CMLogError("cannot parse '%s' as a JSON configuration.",
                           confpath);
        }
    } else {
        CMLogError("cannot read the configuration file '%s'.", confpath);
    }

    if (cfile) CMCall(cfile, Destroy);
    if (content) CMCall(content, Destroy);
    return res;
}

CMDBM_STATIC CMBool CMDBM_ContextInitialize(
        CMDBM_Context_Internal *ictx,
        const char *confjson,
        const char *progcharset,
        CMUTIL_Timer *timer)
{
    CMBool res = CMFalse;
    CMUTIL_Json *conf = NULL;

    if (confjson) {
        conf = CMDBM_ContextLoadConfig(confjson);
        if (conf == NULL) {
            CMLogError("configuration loading failed.");
            goto ENDPOINT;
        }
    }

    // logging defaults. the query id and the statement are logged as they
    // were before the 'Logging' section existed, the result is opt-in
    // because it can be arbitrarily large. these hold for a context created
    // without a configuration file too.
    ictx->logqueryid = CMTrue;
    ictx->logquery = CMTrue;
    ictx->logresult = CMFalse;

    if (!progcharset) progcharset = "UTF-8";
    ictx->progcs = CMStrdup(progcharset);
    if (timer) {
        ictx->timer = timer;
    } else {
        ictx->timer = CMUTIL_TimerCreateEx(1000, 2);
        ictx->istimerinternal = CMTrue;
    }

    if (conf) {
        // both configuration formats end up here, the XML one having been
        // converted into the very same object.
        if (!CMDBM_ContextParseConfig((CMDBM_Context*)ictx, conf)) {
            CMLogError("configuration parsing failed.");
            goto ENDPOINT;
        }
    }

    res = CMTrue;
ENDPOINT:
    if (conf) CMUTIL_JsonDestroy(conf);
    return res;
}

CMDBM_STATIC CMBool CMDBM_ContextAddDatabase(
        CMDBM_Context *ctx, CMDBM_Database *db)
{
    CMBool res = CMFalse;
    CMDBM_Context_Internal *ictx = (CMDBM_Context_Internal*)ctx;
    CMDBM_DatabaseEx *edb = (CMDBM_DatabaseEx*)db;
    const char *dbid = NULL;

    if (!edb) {
        CMLogError("invalid parameter.");
        goto ENDPOINT;
    }
    dbid = CMCall(edb, GetId);

    if (!CMCall(edb, Initialize, ictx->timer, ictx->progcs)) {
        CMLogError("database(%s) initialization failed.", dbid);
        goto ENDPOINT;
    }
    CMCall(ictx->databases, Put, dbid, db, NULL);
    res = CMTrue;
ENDPOINT:
    return res;
}

CMDBM_STATIC CMDBM_Session *CMDBM_ContextGetSession(CMDBM_Context *ctx)
{
    CMDBM_Session *res = NULL;
    CMDBM_ContextEx *ectx = (CMDBM_ContextEx*)ctx;
    res = CMDBM_SessionCreate(ectx);
    if (res == NULL)
        CMLogError("cannot create session.");
    return res;
}

CMDBM_STATIC void CMDBM_ContextDestroy(CMDBM_Context *ctx)
{
    CMDBM_Context_Internal *ictx = (CMDBM_Context_Internal*)ctx;
    if (ictx) {
        // databases must be destroyed before the timer:
        // each database cancels its reloader task on destroy,
        // which must not outlive the timer that owns the tasks.
        if (ictx->databases) CMCall(ictx->databases, Destroy);
        if (ictx->istimerinternal) CMCall(ictx->timer, Destroy);
        if (ictx->poolconfs) CMCall(ictx->poolconfs, Destroy);
        if (ictx->progcs) CMFree(ictx->progcs);
        CMFree(ictx);
    }
}

CMDBM_STATIC CMDBM_DatabaseEx *CMDBM_ContextGetDatabase(
        CMDBM_ContextEx *ctx, const char *dbid)
{
    CMDBM_Context_Internal *ictx = (CMDBM_Context_Internal*)ctx;
    CMDBM_DatabaseEx *res = NULL;

    res = (CMDBM_DatabaseEx*)CMCall(ictx->databases, Get, dbid);
    if (res == NULL)
        CMLogError("source id(%s) not found in this CMDBM context.", dbid);
    return res;
}

CMDBM_STATIC void CMDBM_ContextGetLogFlags(
        CMDBM_ContextEx *ctx, CMBool *logqueryid, CMBool *logquery,
        CMBool *logresult)
{
    CMDBM_Context_Internal *ictx = (CMDBM_Context_Internal*)ctx;

    if (logqueryid) *logqueryid = ictx->logqueryid;
    if (logquery) *logquery = ictx->logquery;
    if (logresult) *logresult = ictx->logresult;
}

static CMDBM_ContextEx g_cmdbm_context = {
    {
        CMDBM_ContextAddDatabase,
        CMDBM_ContextGetSession,
        CMDBM_ContextDestroy
    },
    CMDBM_ContextGetDatabase,
    CMDBM_ContextGetLogFlags
};

CMDBM_Context *CMDBM_ContextCreate(
        const char            *confjson,        /* optional */
        const char            *progcharset,    /* optional: utf-8 default. */
        CMUTIL_Timer        *timer)            /* optional */
{
    CMDBM_Context_Internal *res = CMAlloc(sizeof(CMDBM_Context_Internal));

    memset(res, 0x0, sizeof(CMDBM_Context_Internal));
    memcpy(res, &g_cmdbm_context, sizeof(CMDBM_ContextEx));

    res->databases = CMUTIL_MapCreateEx(
                32, CMFalse, CMDBM_ContextDatabaseDestroyer, 0.75f);
    res->poolconfs = CMUTIL_MapCreateEx(
                16, CMFalse, CMDBM_ContextPoolConfDestroyer, 0.75f);

    if (!CMDBM_ContextInitialize(res, confjson, progcharset, timer)) {
        CMLogError("context initializing failed.");
        CMCall((CMDBM_Context*)res, Destroy);
        res = NULL;
    }

    return (CMDBM_Context*)res;
}

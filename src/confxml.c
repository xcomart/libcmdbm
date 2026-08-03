/*
 * The XML form of the configuration file.
 *
 * The XML tree is not consumed directly: it is converted into exactly the
 * CMUTIL_JsonObject the JSON form produces, and handed to the very same
 * CMDBM_ContextParseConfig. That keeps one set of configuration semantics
 * instead of two.
 *
 * Every key this file produces is lowercased, because
 * CMDBM_ContextConfigClean lowercases the whole tree anyway and because an
 * attribute and a child tag which differ only in case must not end up as two
 * distinct entries. Values are left alone: the datasource type and the
 * mapper type are compared case-insensitively by the parser.
 */
#include "functions.h"

CMUTIL_LogDefine("cmdbm.confxml")

// keys whose value the JSON parser reads with GetLong. an XML attribute is
// always a string, so they are converted at this point.
static const char *g_cmdbm_confxml_numerics[] = {
    "initcount", "maxcount", "pinginterval", "port", "monitorinterval", NULL
};

// keys whose value the JSON parser reads with GetBoolean.
static const char *g_cmdbm_confxml_booleans[] = {
    "pingtest", "testonborrow", "recursive", "serialize", "show", NULL
};

typedef enum CMDBM_ConfXmlKind {
    CMDBM_ConfXmlAuto = 0,      // decided by the key name
    CMDBM_ConfXmlNumber,
    CMDBM_ConfXmlBoolean,
    CMDBM_ConfXmlString
} CMDBM_ConfXmlKind;

CMDBM_STATIC CMBool CMDBM_ConfXmlIsListed(
        const char **list, const char *key)
{
    const char **p;
    for (p = list; *p; p++)
        if (strcmp(*p, key) == 0)
            return CMTrue;
    return CMFalse;
}

// all configuration keys are looked up in lowercase.
CMDBM_STATIC void CMDBM_ConfXmlLowerKey(
        char *buf, size_t bufsz, const char *key)
{
    size_t i;
    for (i = 0; key[i] && i < bufsz - 1; i++)
        buf[i] = (char)tolower((unsigned char)key[i]);
    buf[i] = 0x0;
}

CMDBM_STATIC void CMDBM_ConfXmlPutAs(
        CMUTIL_JsonObject *obj, const char *key, const char *val,
        CMDBM_ConfXmlKind kind)
{
    char lkey[256];

    if (val == NULL) val = "";
    CMDBM_ConfXmlLowerKey(lkey, sizeof(lkey), key);

    if (kind == CMDBM_ConfXmlAuto) {
        if (CMDBM_ConfXmlIsListed(g_cmdbm_confxml_numerics, lkey))
            kind = CMDBM_ConfXmlNumber;
        else if (CMDBM_ConfXmlIsListed(g_cmdbm_confxml_booleans, lkey))
            kind = CMDBM_ConfXmlBoolean;
        else
            kind = CMDBM_ConfXmlString;
    }

    switch (kind) {
    case CMDBM_ConfXmlNumber: {
        char *endp = NULL;
        long long lval = strtoll(val, &endp, 10);
        if (endp != val && endp && *endp == 0x0) {
            CMCall(obj, PutLong, lkey, (int64_t)lval);
            return;
        }
        CMLogWarn("'%s' is not a number: '%s'. kept as a string.", key, val);
        break;
    }
    case CMDBM_ConfXmlBoolean:
        if (strcasecmp(val, "true") == 0 || strcasecmp(val, "yes") == 0 ||
                strcmp(val, "1") == 0) {
            CMCall(obj, PutBoolean, lkey, CMTrue);
            return;
        }
        if (strcasecmp(val, "false") == 0 || strcasecmp(val, "no") == 0 ||
                strcmp(val, "0") == 0) {
            CMCall(obj, PutBoolean, lkey, CMFalse);
            return;
        }
        CMLogWarn("'%s' is not a boolean: '%s'. kept as a string.", key, val);
        break;
    default:
        break;
    }
    CMCall(obj, PutString, lkey, val);
}

CMDBM_STATIC void CMDBM_ConfXmlPut(
        CMUTIL_JsonObject *obj, const char *key, const char *val)
{
    CMDBM_ConfXmlPutAs(obj, key, val, CMDBM_ConfXmlAuto);
}

// an attribute lookup which ignores the case of the attribute name, so that
// 'monitorInterval' and 'monitorinterval' are the same attribute.
// the returned string belongs to the node.
CMDBM_STATIC const char *CMDBM_ConfXmlAttr(
        CMUTIL_XmlNode *node, const char *name)
{
    uint32_t i;
    const char *res = NULL;
    CMUTIL_StringArray *names = CMCall(node, GetAttributeNames);

    if (names == NULL) return NULL;
    for (i = 0; res == NULL && i < CMCall(names, GetSize); i++) {
        const char *aname = CMCall(names, GetCString, i);
        if (aname && strcasecmp(aname, name) == 0) {
            CMUTIL_String *val = CMCall(node, GetAttribute, aname);
            if (val) res = CMCall(val, GetCString);
        }
    }
    CMCall(names, Destroy);
    return res;
}

// the concatenated text children of a tag, trimmed. never NULL, the caller
// destroys it.
CMDBM_STATIC CMUTIL_String *CMDBM_ConfXmlText(CMUTIL_XmlNode *node)
{
    uint32_t i;
    CMUTIL_String *res = CMUTIL_StringCreate();

    for (i = 0; i < (uint32_t)CMCall(node, ChildCount); i++) {
        CMUTIL_XmlNode *child = CMCall(node, ChildAt, i);
        if (CMCall(child, GetType) == CMXmlNodeText) {
            const char *text = CMCall(child, GetName);
            // an empty addition is an error to CMUTIL_String, and an empty
            // element is perfectly normal here.
            if (text && *text) CMCall(res, AddString, text);
        }
    }
    CMCall(res, SelfTrim);
    return res;
}

CMDBM_STATIC CMBool CMDBM_ConfXmlHasTagChild(CMUTIL_XmlNode *node)
{
    uint32_t i;
    for (i = 0; i < (uint32_t)CMCall(node, ChildCount); i++) {
        CMUTIL_XmlNode *child = CMCall(node, ChildAt, i);
        if (CMCall(child, GetType) == CMXmlNodeTag)
            return CMTrue;
    }
    return CMFalse;
}

CMDBM_STATIC void CMDBM_ConfXmlCopyAttrs(
        CMUTIL_JsonObject *obj, CMUTIL_XmlNode *node)
{
    uint32_t i;
    CMUTIL_StringArray *names = CMCall(node, GetAttributeNames);

    if (names == NULL) return;
    for (i = 0; i < CMCall(names, GetSize); i++) {
        const char *aname = CMCall(names, GetCString, i);
        CMUTIL_String *val = aname? CMCall(node, GetAttribute, aname):NULL;
        if (val) {
            const char *sval = CMCall(val, GetCString);
            CMDBM_ConfXmlPut(obj, aname, sval);
        }
    }
    CMCall(names, Destroy);
}

// fold a '<Tag>value</Tag>' child into the key 'tag' of 'obj'.
// a child tag is the more specific spelling of a setting, so it overrides an
// attribute of the same name, which was copied first.
CMDBM_STATIC void CMDBM_ConfXmlFoldChild(
        CMUTIL_JsonObject *obj, CMUTIL_XmlNode *child, const char *ctxname)
{
    const char *cname = CMCall(child, GetName);
    CMUTIL_String *text = NULL;

    if (CMDBM_ConfXmlHasTagChild(child)) {
        CMLogWarn("unknown element '%s' in %s. ignored.", cname, ctxname);
        return;
    }
    text = CMDBM_ConfXmlText(child);
    {
        const char *stext = CMCall(text, GetCString);
        CMDBM_ConfXmlPut(obj, cname, stext);
    }
    CMCall(text, Destroy);
}

// a node whose attributes and scalar child tags are all key/value settings:
// '<Pool>', '<PoolConfig>' and '<MapperSet>'.
CMDBM_STATIC CMUTIL_JsonObject *CMDBM_ConfXmlKeyValueNode(
        CMUTIL_XmlNode *node, const char *ctxname)
{
    uint32_t i;
    CMUTIL_JsonObject *res = CMUTIL_JsonObjectCreate();

    CMDBM_ConfXmlCopyAttrs(res, node);
    for (i = 0; i < (uint32_t)CMCall(node, ChildCount); i++) {
        CMUTIL_XmlNode *child = CMCall(node, ChildAt, i);
        if (CMCall(child, GetType) != CMXmlNodeTag) continue;
        CMDBM_ConfXmlFoldChild(res, child, ctxname);
    }
    return res;
}

// '<Param key="k" value="v"/>' and '<Param key="k">v</Param>' are both
// accepted. the 'value' attribute wins when both are given.
CMDBM_STATIC void CMDBM_ConfXmlParam(
        CMUTIL_JsonObject *params, CMUTIL_XmlNode *node)
{
    const char *key = CMDBM_ConfXmlAttr(node, "key");
    const char *val = CMDBM_ConfXmlAttr(node, "value");
    CMUTIL_String *text = NULL;

    if (key == NULL || *key == 0x0) {
        CMLogWarn("a 'Param' element without a 'key' attribute. ignored.");
        return;
    }
    if (val == NULL) {
        text = CMDBM_ConfXmlText(node);
        val = CMCall(text, GetCString);
    }
    // a connection parameter is whatever the module makes of it, always a
    // string.
    CMDBM_ConfXmlPutAs(params, key, val, CMDBM_ConfXmlString);
    if (text) CMCall(text, Destroy);
}

CMDBM_STATIC CMUTIL_JsonObject *CMDBM_ConfXmlMapper(CMUTIL_XmlNode *node)
{
    CMUTIL_JsonObject *res = CMUTIL_JsonObjectCreate();
    const char *path = CMDBM_ConfXmlAttr(node, "file");

    CMCall(res, PutString, "type", "mapper");
    if (path == NULL) path = CMDBM_ConfXmlAttr(node, "filePath");
    if (path == NULL) {
        CMUTIL_String *text = CMDBM_ConfXmlText(node);
        const char *stext = CMCall(text, GetCString);
        if (*stext)
            CMCall(res, PutString, "filepath", stext);
        else
            CMLogWarn("a 'Mapper' element without a file path.");
        CMCall(text, Destroy);
    } else {
        CMCall(res, PutString, "filepath", path);
    }
    return res;
}

// '<Mappers monitorInterval="n">' does not describe a mapper but how often
// the datasource rescans them, so it is lifted to the datasource itself.
CMDBM_STATIC CMUTIL_JsonArray *CMDBM_ConfXmlMappers(
        CMUTIL_JsonObject *dcfg, CMUTIL_XmlNode *node)
{
    uint32_t i;
    CMUTIL_JsonArray *res = CMUTIL_JsonArrayCreate();
    const char *minterval = CMDBM_ConfXmlAttr(node, "monitorInterval");

    if (minterval)
        CMDBM_ConfXmlPutAs(dcfg, "monitorInterval", minterval,
                           CMDBM_ConfXmlNumber);

    for (i = 0; i < (uint32_t)CMCall(node, ChildCount); i++) {
        CMUTIL_XmlNode *child = CMCall(node, ChildAt, i);
        const char *cname = NULL;
        if (CMCall(child, GetType) != CMXmlNodeTag) continue;
        cname = CMCall(child, GetName);
        if (strcasecmp(cname, "Mapper") == 0) {
            CMUTIL_JsonObject *mcfg = CMDBM_ConfXmlMapper(child);
            CMCall(res, Add, (CMUTIL_Json*)mcfg);
        } else if (strcasecmp(cname, "MapperSet") == 0) {
            CMUTIL_JsonObject *mcfg =
                    CMDBM_ConfXmlKeyValueNode(child, "a mapper set");
            CMCall(mcfg, PutString, "type", "mapperSet");
            CMCall(res, Add, (CMUTIL_Json*)mcfg);
        } else {
            CMLogWarn("unknown element '%s' in 'Mappers'. ignored.", cname);
        }
    }
    return res;
}

// the tag name of a datasource is its type, so any key registered with
// CMDBM_RegisterDBMS can be spelled as a tag.
CMDBM_STATIC CMUTIL_JsonObject *CMDBM_ConfXmlDatabase(CMUTIL_XmlNode *node)
{
    uint32_t i;
    CMUTIL_JsonObject *res = CMUTIL_JsonObjectCreate();
    CMUTIL_JsonObject *params = NULL;
    const char *tname = CMCall(node, GetName);

    CMCall(res, PutString, "type", tname);
    CMDBM_ConfXmlCopyAttrs(res, node);

    for (i = 0; i < (uint32_t)CMCall(node, ChildCount); i++) {
        CMUTIL_XmlNode *child = CMCall(node, ChildAt, i);
        const char *cname = NULL;
        if (CMCall(child, GetType) != CMXmlNodeTag) continue;
        cname = CMCall(child, GetName);
        if (strcasecmp(cname, "Param") == 0) {
            if (params == NULL) {
                params = CMUTIL_JsonObjectCreate();
                CMCall(res, Put, "params", (CMUTIL_Json*)params);
            }
            CMDBM_ConfXmlParam(params, child);
        } else if (strcasecmp(cname, "Pool") == 0) {
            CMUTIL_JsonObject *pcfg =
                    CMDBM_ConfXmlKeyValueNode(child, "a pool configuration");
            CMCall(res, Put, "pool", (CMUTIL_Json*)pcfg);
        } else if (strcasecmp(cname, "Mappers") == 0) {
            CMUTIL_JsonArray *mappers = CMDBM_ConfXmlMappers(res, child);
            CMCall(res, Put, "mappers", (CMUTIL_Json*)mappers);
        } else {
            CMDBM_ConfXmlFoldChild(res, child, "a datasource");
        }
    }
    return res;
}

CMDBM_STATIC CMUTIL_JsonArray *CMDBM_ConfXmlDatabases(CMUTIL_XmlNode *node)
{
    uint32_t i;
    CMUTIL_JsonArray *res = CMUTIL_JsonArrayCreate();

    for (i = 0; i < (uint32_t)CMCall(node, ChildCount); i++) {
        CMUTIL_XmlNode *child = CMCall(node, ChildAt, i);
        CMUTIL_JsonObject *dcfg = NULL;
        if (CMCall(child, GetType) != CMXmlNodeTag) continue;
        dcfg = CMDBM_ConfXmlDatabase(child);
        CMCall(res, Add, (CMUTIL_Json*)dcfg);
    }
    return res;
}

CMDBM_STATIC CMUTIL_JsonArray *CMDBM_ConfXmlPoolConfigs(CMUTIL_XmlNode *node)
{
    uint32_t i;
    CMUTIL_JsonArray *res = CMUTIL_JsonArrayCreate();

    for (i = 0; i < (uint32_t)CMCall(node, ChildCount); i++) {
        CMUTIL_XmlNode *child = CMCall(node, ChildAt, i);
        const char *cname = NULL;
        CMUTIL_JsonObject *pcfg = NULL;
        if (CMCall(child, GetType) != CMXmlNodeTag) continue;
        cname = CMCall(child, GetName);
        if (strcasecmp(cname, "PoolConfig") != 0) {
            CMLogWarn("unknown element '%s' in 'PoolConfigurations'. "
                      "ignored.", cname);
            continue;
        }
        pcfg = CMDBM_ConfXmlKeyValueNode(child, "a pool configuration");
        CMCall(res, Add, (CMUTIL_Json*)pcfg);
    }
    return res;
}

// '<QueryId show="true"/>' and '<QueryId>true</QueryId>' are both accepted.
CMDBM_STATIC CMUTIL_JsonObject *CMDBM_ConfXmlLogging(CMUTIL_XmlNode *node)
{
    uint32_t i;
    CMUTIL_JsonObject *res = CMUTIL_JsonObjectCreate();

    for (i = 0; i < (uint32_t)CMCall(node, ChildCount); i++) {
        CMUTIL_XmlNode *child = CMCall(node, ChildAt, i);
        const char *cname = NULL, *sval = NULL;
        CMUTIL_String *text = NULL;
        if (CMCall(child, GetType) != CMXmlNodeTag) continue;
        cname = CMCall(child, GetName);
        sval = CMDBM_ConfXmlAttr(child, "show");
        if (sval == NULL) {
            text = CMDBM_ConfXmlText(child);
            sval = CMCall(text, GetCString);
        }
        if (*sval)
            CMDBM_ConfXmlPutAs(res, cname, sval, CMDBM_ConfXmlBoolean);
        else
            CMLogWarn("'Logging/%s' has no value. ignored.", cname);
        if (text) CMCall(text, Destroy);
    }
    return res;
}

CMUTIL_Json *CMDBM_ConfigFromXml(CMUTIL_XmlNode *root)
{
    uint32_t i;
    CMUTIL_JsonObject *res = NULL;
    const char *rname = NULL;

    if (root == NULL) return NULL;
    rname = CMCall(root, GetName);
    if (rname == NULL || strcasecmp(rname, "Configuration") != 0) {
        CMLogErrorS("the root element of an XML configuration must be "
                    "'Configuration', not '%s'.", rname? rname:"(none)");
        return NULL;
    }

    res = CMUTIL_JsonObjectCreate();
    for (i = 0; i < (uint32_t)CMCall(root, ChildCount); i++) {
        CMUTIL_XmlNode *child = CMCall(root, ChildAt, i);
        const char *cname = NULL;
        if (CMCall(child, GetType) != CMXmlNodeTag) continue;
        cname = CMCall(child, GetName);
        if (strcasecmp(cname, "Databases") == 0) {
            CMUTIL_JsonArray *dbs = CMDBM_ConfXmlDatabases(child);
            CMCall(res, Put, "databases", (CMUTIL_Json*)dbs);
        } else if (strcasecmp(cname, "PoolConfigurations") == 0) {
            CMUTIL_JsonArray *pcs = CMDBM_ConfXmlPoolConfigs(child);
            CMCall(res, Put, "poolconfigurations", (CMUTIL_Json*)pcs);
        } else if (strcasecmp(cname, "Logging") == 0) {
            CMUTIL_JsonObject *lcfg = CMDBM_ConfXmlLogging(child);
            CMCall(res, Put, "logging", (CMUTIL_Json*)lcfg);
        } else {
            CMLogWarn("unknown element '%s' in 'Configuration'. ignored.",
                      cname);
        }
    }
    return (CMUTIL_Json*)res;
}

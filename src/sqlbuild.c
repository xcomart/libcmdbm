
#include "mapper.h"

CMUTIL_LogDefine("cmdbm.sqlbuild")

CMDBM_STATIC const char* CMDBM_RevCaseEnds(
        const char *q, const char *limit, const char *n)
{
	int ed = (int)strlen(n);
    const char *p = n + ed - 1;
    while (limit < p && n < q && toupper(*q) == toupper(*p)) {
        q--; p--;
    }
	return n == p && toupper(*q) == toupper(*p)? q:NULL;
}

CMDBM_STATIC const char* CMDBM_CaseStarts(
        const char *p, const char *limit, const char *n)
{
	while (*p && *n && p < limit && toupper(*p) == toupper(*n)) {
		p++; n++;
	}
	return (!*n)? p:NULL;
}

CMDBM_STATIC CMBool CMDBM_TestExpr(CMDBM_Session *sess,
        CMUTIL_JsonObject *params, CMUTIL_Iterator *iter, CMBool isOpen)
{
    CMBool res = CMFalse;
	while (CMCall(iter, HasNext)) {
		CMDBM_CompItem *c = (CMDBM_CompItem*)CMCall(iter, Next);
		if (c->type == CMDBM_ETOpen) {
			res = CMDBM_TestExpr(sess, params, iter, CMTrue);
		} else if (c->type == CMDBM_ETClose) {
			return res;
		} else if (c->type == CMDBM_ETAnd) {
			if (!res) {
				if (isOpen) {
					while (CMCall(iter, HasNext)) {
						CMDBM_CompItem *o =
								(CMDBM_CompItem*)CMCall(iter, Next);
						if (o->type == CMDBM_ETClose)
							return res;
					}
				} else {
					return res;
				}
			}
		} else if (c->type == CMDBM_ETOr) {
			if (res) {
				if (isOpen) {
					while (CMCall(iter, HasNext)) {
						CMDBM_CompItem *o =
								(CMDBM_CompItem*)CMCall(iter, Next);
						if (o->type == CMDBM_ETClose)
							return res;
					}
				} else {
					return res;
				}
			}
		} else {
			const char *sp1 = CMCall(c->aname, GetCString);
			const char *sp2 = CMCall(c->bname, GetCString);
			if (!c->aconst) {
				sp1 = CMCall(params, GetCString, sp1);
			}
			if (!c->bconst) {
				sp2 = CMCall(params, GetCString, sp2);
			}
			res = c->comparator(sp1, sp2);
		}
	}
	return res;
}

CMBool CMDBM_BuildNode(
		CMDBM_Session *sess,
		CMDBM_Connection *conn,
		CMUTIL_XmlNode *node,
		CMUTIL_JsonObject *params,
		CMUTIL_JsonArray *bindings,
		CMUTIL_List *after,
		CMUTIL_String *obuf,
		CMUTIL_JsonObject *outs,
		CMUTIL_List *rembuf);

CMDBM_STATIC CMBool CMDBM_BuildChildren(
		CMDBM_Session *sess,
		CMDBM_Connection *conn,
		CMUTIL_XmlNode *node,
		CMUTIL_JsonObject *params,
		CMUTIL_JsonArray *bindings,
		CMUTIL_List *after,
		CMUTIL_String *obuf,
		CMUTIL_JsonObject *outs,
		CMUTIL_List *rembuf)
{
    uint32_t i;
    size_t size = CMCall(node, ChildCount);

	for (i=0; i<size; i++) {
		CMUTIL_XmlNode *cld = CMCall(node, ChildAt, i);
		if (!CMDBM_BuildNode(sess, conn, cld, params, bindings,
							 after, obuf, outs, rembuf))
			return CMFalse;
	}
	return CMTrue;
}

CMDBM_STATIC CMBool CMDBM_BuildText(
		CMDBM_Session *sess,
		CMDBM_Connection *conn,
		CMUTIL_XmlNode *node,
		CMUTIL_JsonObject *params,
		CMUTIL_JsonArray *bindings,
		CMUTIL_List *after,
		CMUTIL_String *obuf,
		CMUTIL_JsonObject *outs,
		CMUTIL_List *rembuf)
{
	const char *text = CMCall(node, GetName);
	CMCall(obuf, AddString, text);
	CMUTIL_UNUSED(sess, conn, params, bindings, after, outs, rembuf);
	return CMTrue;
}

CMDBM_STATIC CMBool CMDBM_BuildParamSet(
		CMDBM_Session *sess,
		CMDBM_Connection *conn,
		CMUTIL_XmlNode *node,
		CMUTIL_JsonObject *params,
		CMUTIL_JsonArray *bindings,
		CMUTIL_List *after,
		CMUTIL_String *obuf,
		CMUTIL_JsonObject *outs,
		CMUTIL_List *rembuf)
{
	CMUTIL_String *name, *val, *type;

	name = CMCall(node, GetAttribute, "name");
	val = CMCall(node, GetAttribute, "value");
	type = CMCall(node, GetAttribute, "type");	// optional(string default)
	if (name && val && params) {
		const char *sname = CMCall(name, GetCString);
		const char *sval = CMCall(val, GetCString);
		const char *stype = "string";
		if (type) stype = CMCall(type, GetCString);
		if (strcasecmp(stype, "string") == 0) {
			CMCall(params, PutString, sname, sval);
		} else if (strcasecmp(stype, "int") ||
				   strcasecmp(stype, "long")) {
			CMCall(params, PutLong, sname, atoll(sval));
		} else if (strcasecmp(stype, "float") ||
				   strcasecmp(stype, "double")) {
			CMCall(params, PutDouble, sname, atof(sval));
		} else {
			CMLogErrorS("unknown parameter type: %s", sval);
			return CMFalse;
		}
		return CMTrue;
	}
	CMUTIL_UNUSED(sess, conn, bindings, after, obuf, outs, rembuf);
	return CMFalse;
}

CMDBM_STATIC CMBool CMDBM_BuildOutParam(
		CMDBM_Session *sess,
		CMDBM_Connection *conn,
		CMUTIL_XmlNode *node,
		CMUTIL_JsonObject *params,
		CMUTIL_JsonArray *bindings,
		CMUTIL_List *after,
		CMUTIL_String *obuf,
		CMUTIL_JsonObject *outs,
		CMUTIL_List *rembuf)
{
	const char *key = CMCall(node, GetName);
	char pbuf[1024];
    uint32_t idx = (uint32_t)CMCall(bindings, GetSize);
	CMUTIL_Json *value = CMCall(params, Get, key);
    CMUTIL_JsonValue *jval = (CMUTIL_JsonValue*)value;
    CMJsonValueType vtype = CMCall(jval, GetValueType);

    CMCall(conn, GetBindString, idx, pbuf, vtype);
	CMCall(obuf, AddString, pbuf);
	if (!value) {
		CMCall(params, PutString, key, "1");
		value = CMCall(params, Get, key);
	}
	sprintf(pbuf, "%d", idx);
	CMCall(outs, Put, pbuf, value);
	CMCall(bindings, Add, value);
	CMUTIL_UNUSED(sess, after, rembuf);
	return CMTrue;
}

CMDBM_STATIC CMBool CMDBM_BuildReplace(
		CMDBM_Session *sess,
		CMDBM_Connection *conn,
		CMUTIL_XmlNode *node,
		CMUTIL_JsonObject *params,
		CMUTIL_JsonArray *bindings,
		CMUTIL_List *after,
		CMUTIL_String *obuf,
		CMUTIL_JsonObject *outs,
		CMUTIL_List *rembuf)
{
	const char *key = CMCall(node, GetName);
	CMUTIL_Json *pitem = CMCall(params, Get, key);
    CMJsonType jtype = CMCall(pitem, GetType);
	CMUTIL_UNUSED(sess, conn, bindings, after, outs, rembuf);
    if (jtype == CMJsonTypeValue) {
		CMUTIL_String *str = CMCall(params, GetString, key);
		CMCall(obuf, AddAnother, str);
		return CMTrue;
	} else {
		CMLogErrorS("replacement of parameter is not value type.(key:%s)", key);
		return CMFalse;
	}
}

CMDBM_STATIC CMBool CMDBM_BuildInclude(
		CMDBM_Session *sess,
		CMDBM_Connection *conn,
		CMUTIL_XmlNode *node,
		CMUTIL_JsonObject *params,
		CMUTIL_JsonArray *bindings,
		CMUTIL_List *after,
		CMUTIL_String *obuf,
		CMUTIL_JsonObject *outs,
		CMUTIL_List *rembuf)
{
	CMUTIL_XmlNode *refqry = NULL;

	const char *nodename = CMCall(node, GetName);
	refqry = (CMUTIL_XmlNode*)CMCall(conn, GetQuery, nodename);
	if (refqry) {
		if (CMDBM_MapperGetNodeType(refqry) == CMDBM_NTSqlFrag) {
			return CMDBM_BuildNode(sess, conn, refqry, params, bindings, after,
								   obuf, outs, rembuf);
		} else {
			CMLogErrorS("included item is not sql tag: %s", nodename);
		}
	} else {
		CMLogErrorS("sql item not exists: %s", nodename);
	}
	return CMFalse;
}

CMDBM_STATIC CMBool CMDBM_BuildBind(
		CMDBM_Session *sess,
		CMDBM_Connection *conn,
		CMUTIL_XmlNode *node,
		CMUTIL_JsonObject *params,
		CMUTIL_JsonArray *bindings,
		CMUTIL_List *after,
		CMUTIL_String *obuf,
		CMUTIL_JsonObject *outs,
		CMUTIL_List *rembuf)
{
	const char *key = CMCall(node, GetName);
	CMUTIL_Json *data = CMCall(params, Get, key);
	CMUTIL_UNUSED(sess, after, outs, rembuf);
	if (data) {
        uint32_t index = (uint32_t)CMCall(bindings, GetSize);
		char buf[50];
        CMUTIL_JsonValue *value = (CMUTIL_JsonValue*)data;
        CMJsonValueType vtype = CMCall(value, GetValueType);
        CMCall(conn, GetBindString, index, buf, vtype);
		CMCall(obuf, AddString, buf);
		CMCall(bindings, Add, data);
		return CMTrue;
	} else {
		CMLogErrorS("parameter has no key: %s", key);
		return CMFalse;
	}
}

CMDBM_STATIC CMBool CMDBM_BuildTrim(
		CMDBM_Session *sess,
		CMDBM_Connection *conn,
		CMUTIL_XmlNode *node,
		CMUTIL_JsonObject *params,
		CMUTIL_JsonArray *bindings,
		CMUTIL_List *after,
		CMUTIL_String *obuf,
		CMUTIL_JsonObject *outs,
		CMUTIL_List *rembuf)
{
    CMBool res = CMFalse;
	CMUTIL_String *sbuf = CMUTIL_StringCreate();

	if (CMDBM_BuildChildren(sess, conn, node, params, bindings,
							after, sbuf, outs, rembuf)) {
		CMUTIL_String *sprfx = CMCall(node,GetAttribute,"prefix");
		CMUTIL_String *ssufx = CMCall(node,GetAttribute,"suffix");
		CMUTIL_String *sprov = CMCall(node,GetAttribute,"prefixOverrides");
		CMUTIL_String *ssuov = CMCall(node,GetAttribute,"suffixOverrides");
		const char *prfx = CMCall(sprfx, GetCString);
		const char *sufx = CMCall(ssufx, GetCString);
		const char *prov = CMCall(sprov, GetCString);
		const char *suov = CMCall(ssuov, GetCString);

        const char *p = CMCall(sbuf, GetCString);
        const char *q = p, *r;
		CMUTIL_StringArray *subs = NULL;
        uint32_t i;

		// save string end pointer
		r = p + CMCall(sbuf, GetSize);

		// remove preceeding spaces
		while (*p && strchr(CMDBM_SPACES, *p)) p++;
		// remove trailing spaces
		while (strchr(CMDBM_SPACES, *(r-1)) && p < (r-1)) r--;

		// override suffix
		if (suov) {
			subs = CMUTIL_StringSplit(suov, "|");
			for (i=0; i<CMCall(subs, GetSize); i++) {
				CMUTIL_String *sd = CMCall(subs, GetAt, i);
                const char *d = CMCall(sd, GetCString);
				q = CMDBM_RevCaseEnds(r-1, p, d);
				if (q && (strchr(CMDBM_SQLDELIMS, *(q-1)) ||
						  strchr(CMDBM_SQLDELIMS, *d))) {
					r = q;
					break;
				}
			}
			CMCall(subs, Destroy);
		}

		// override prefix
		if (prov) {
			subs = CMUTIL_StringSplit(prov, "|");
			for (i=0; i<CMCall(subs, GetSize); i++) {
				CMUTIL_String *sd = CMCall(subs, GetAt, i);
                const char *d = CMCall(sd, GetCString);
				q = CMDBM_CaseStarts(p, r, d);
				if (q && (strchr(CMDBM_SQLDELIMS, *q) ||
						strchr(CMDBM_SQLDELIMS, *d))) {
					p = q;
					break;
				}
			}
			CMCall(subs, Destroy);
		}

		// remove preceeding spaces
		while (*p && strchr(CMDBM_SPACES, *p) && p < r) p++;

		if (p < r) {
			CMCall(obuf, AddChar, ' ');
			if (prfx)
				CMCall(obuf, AddString, prfx);
            CMCall(obuf, AddNString, p, (size_t)(r-p));
			CMCall(obuf, AddChar, ' ');
			if (sufx)
				CMCall(obuf, AddString, sufx);
		}

		res = CMTrue;
	}

	CMCall(sbuf, Destroy);
	return res;
}

CMDBM_STATIC CMBool CMDBM_BuildForeach(
		CMDBM_Session *sess,
		CMDBM_Connection *conn,
		CMUTIL_XmlNode *node,
		CMUTIL_JsonObject *params,
		CMUTIL_JsonArray *bindings,
		CMUTIL_List *after,
		CMUTIL_String *obuf,
		CMUTIL_JsonObject *outs,
		CMUTIL_List *rembuf)
{
	CMUTIL_String *scoll, *sopen, *sclose, *ssep, *sitem, *sindex;
	const char *sitemkey, *sindexkey, *scolkey;
	CMUTIL_Json *itembackup, *indexbackup;
	CMUTIL_JsonArray *collection;
    uint32_t i;
    size_t size;

	scoll = CMCall(node, GetAttribute, "collection");
	sopen = CMCall(node, GetAttribute, "open");
	sclose = CMCall(node, GetAttribute, "close");
	ssep = CMCall(node, GetAttribute, "separator");

	if (scoll == NULL) {
		CMLogErrorS("foreach tag must have collection attribute.");
		return CMFalse;
	}
	scolkey = CMCall(scoll, GetCString);

	// dynamic named variables.
	sitem = CMCall(node, GetAttribute, "item");
	sindex = CMCall(node, GetAttribute, "index");

	if (sitem == NULL)
		CMLogWarn("foreach tag has no item attribute. are you intended?");

	sitemkey = sitem? CMCall(sitem, GetCString):NULL;
	sindexkey = sindex? CMCall(sindex, GetCString):NULL;

	collection = (CMUTIL_JsonArray*)CMCall(params, Get, scolkey);
	if (collection == NULL) {
		CMLogErrorS("parameter have no collection with key '%s'", scolkey);
		return CMFalse;
	}
    if (CMCall(&(collection->parent), GetType) != CMJsonTypeArray) {
		CMLogErrorS("parameter item '%s' is not a collection.", scolkey);
		return CMFalse;
	}
	size = CMCall(collection, GetSize);

	itembackup = sitem? CMCall(params, Remove, sitemkey):NULL;
	indexbackup = sindex? CMCall(params, Remove, sindexkey):NULL;

	if (sopen) CMCall(obuf, AddAnother, sopen);
	for (i=0; i<size; i++) {
		CMUTIL_Json *item = CMCall(collection, Get, i);
		CMUTIL_Json *idx;
		CMCall(params, Put, sitemkey, item);
		CMCall(params, PutLong, sindexkey, i);

		// build child nodes
		if (!CMDBM_BuildChildren(sess, conn, node, params, bindings,
								 after, obuf, outs, rembuf))
			return CMFalse;

		if (ssep && i < (size-1))
			CMCall(obuf, AddAnother, ssep);

		// remove item for not be destroyed.
		CMCall(params, Remove, sitemkey);

		// save index item to destroy after execution.
		idx = CMCall(params, Remove, sindexkey);
		CMCall(rembuf, AddTail, idx);
	}
	if (sclose) CMCall(obuf, AddAnother, sclose);

	if (itembackup) CMCall(params, Put, sitemkey, itembackup);
	if (indexbackup) CMCall(params, Put, sindexkey, indexbackup);

	return CMTrue;
}

CMDBM_STATIC CMBool CMDBM_BuildIfBase(
		CMDBM_Session *sess,
		CMDBM_Connection *conn,
		CMUTIL_XmlNode *node,
		CMUTIL_JsonObject *params,
		CMUTIL_JsonArray *bindings,
		CMUTIL_List *after,
		CMUTIL_String *obuf,
		CMUTIL_JsonObject *outs,
		CMUTIL_List *rembuf,
        CMBool *isAdded)
{
    CMBool res = CMFalse;
	CMUTIL_List *data = CMCall(node, GetUserData);
	CMUTIL_Iterator *iter = CMCall(data, Iterator);
	if (CMDBM_TestExpr(sess, params, iter, CMFalse)) {
		res = CMDBM_BuildChildren(sess, conn, node, params, bindings,
								  after, obuf, outs, rembuf);
		if (isAdded)
			*isAdded = CMTrue;
	} else {
		res = CMTrue;
	}
	if (iter)
		CMCall(iter, Destroy);
	return res;
}

CMDBM_STATIC CMBool CMDBM_BuildChoose(
		CMDBM_Session *sess,
		CMDBM_Connection *conn,
		CMUTIL_XmlNode *node,
		CMUTIL_JsonObject *params,
		CMUTIL_JsonArray *bindings,
		CMUTIL_List *after,
		CMUTIL_String *obuf,
		CMUTIL_JsonObject *outs,
		CMUTIL_List *rembuf)
{
    CMBool isApplied = CMFalse;
    uint32_t i;
    size_t size = CMCall(node, ChildCount);

	for(i=0; i<size && !isApplied; i++) {
		CMUTIL_XmlNode *child = CMCall(node, ChildAt, i);
		switch(CMDBM_MapperGetNodeType(child)) {
		case CMDBM_NTSqlIf:
			if (!isApplied) {
				if (!CMDBM_BuildIfBase(sess, conn, child, params, bindings,
									   after, obuf, outs, rembuf, &isApplied))
					return CMFalse;
			}
			break;
		case CMDBM_NTSqlOtherwise:
			if (!isApplied) {
				if (!CMDBM_BuildNode(sess, conn, child, params, bindings,
									 after, obuf, outs, rembuf))
					return CMFalse;
				isApplied = CMTrue;
			}
			break;
		default:
			if (!CMDBM_BuildNode(sess, conn, child, params, bindings,
								 after, obuf, outs, rembuf))
				return CMFalse;
			break;
		}
	}

	return CMTrue;
}

CMDBM_STATIC CMBool CMDBM_BuildIf(
		CMDBM_Session *sess,
		CMDBM_Connection *conn,
		CMUTIL_XmlNode *node,
		CMUTIL_JsonObject *params,
		CMUTIL_JsonArray *bindings,
		CMUTIL_List *after,
		CMUTIL_String *obuf,
		CMUTIL_JsonObject *outs,
		CMUTIL_List *rembuf)
{
	return CMDBM_BuildIfBase(sess, conn, node, params, bindings,
							 after, obuf, outs, rembuf, NULL);
}

CMDBM_STATIC CMBool CMDBM_BuildSelectKey(
		CMDBM_Session *sess,
		CMDBM_Connection *conn,
		CMUTIL_XmlNode *node,
		CMUTIL_JsonObject *params,
		CMUTIL_JsonArray *bindings,
		CMUTIL_List *after,
		CMUTIL_String *obuf,
		CMUTIL_JsonObject *outs,
		CMUTIL_List *rembuf)
{
    CMBool res = CMFalse;
	const char *key, *order;
	CMUTIL_String *sbuf = CMUTIL_StringCreate();
	CMUTIL_JsonArray *nbinds = CMUTIL_JsonArrayCreate();
    CMBool beval = CMFalse;
	CMUTIL_String *stmp;

	CMUTIL_UNUSED(bindings, obuf);

	stmp = CMCall(node, GetAttribute, "order");
	order = CMCall(stmp, GetCString);
	if (order && strcasecmp(order, "before") == 0) {
		beval = CMTrue;
	} else {
		if (CMCall(after, Remove, node) != NULL)
			// calling after execution of main query so execute it.
			beval = CMTrue;
		else
			// main query evaluation, so add it to after list.
			CMCall(after, AddTail, node);
	}
	if (beval) {
		stmp = CMCall(node, GetAttribute, "keyProperty");
		key = CMCall(stmp, GetCString);

		res = CMDBM_BuildChildren(
					sess, conn, node, params, nbinds, after, sbuf, outs, rembuf);
		if (res) {
			CMUTIL_JsonValue *value =
					CMCall(conn, GetObject, sbuf, nbinds, NULL);
			if (value) {
				CMCall(params, Put, key, (CMUTIL_Json*)value);
				res = CMTrue;
			} else {
				CMLogError("query execution failed for selectKey. -> %s", sbuf);
			}
		}
	} else {
		res = CMTrue;
	}
	CMUTIL_JsonDestroy(nbinds);
	CMCall(sbuf, Destroy);
	return res;
}

static CMDBM_BuildFunc g_cmdbm_buildfuncs[] = {
		 NULL					//unknown
		,NULL					//nodeXmlText
		,NULL					//nodeXmlTag

		,CMDBM_BuildChildren	//nodeSqlGroup
		,CMDBM_BuildText		//nodeSqlText
		,CMDBM_BuildParamSet	//nodeSqlParamSet
		,CMDBM_BuildOutParam	//nodeSqlOutParam
		,CMDBM_BuildReplace		//nodeSqlReplace

		,NULL					//nodeSqlMap
		,CMDBM_BuildChildren	//nodeSqlFrag
		,CMDBM_BuildChildren	//nodeSqlSelect
		,CMDBM_BuildChildren	//nodeSqlUpdate
		,CMDBM_BuildChildren	//nodeSqlDelete
		,CMDBM_BuildChildren	//nodeSqlInsert
		,CMDBM_BuildInclude		//nodeSqlInclude
		,CMDBM_BuildBind		//nodeSqlBind
		,CMDBM_BuildTrim		//nodeSqlTrim
		,CMDBM_BuildForeach		//nodeSqlForeach
		,CMDBM_BuildTrim		//nodeSqlWhere
		,CMDBM_BuildTrim		//nodeSqlSet
		,CMDBM_BuildChoose		//nodeSqlChoose
		,CMDBM_BuildChildren	//nodeSqlOtherwise
		,CMDBM_BuildIf			//nodeSqlIf
		,CMDBM_BuildSelectKey	//nodeSqlSelectKey
};

CMBool CMDBM_BuildNode(
		CMDBM_Session *sess,
		CMDBM_Connection *conn,
		CMUTIL_XmlNode *node,
		CMUTIL_JsonObject *params,
		CMUTIL_JsonArray *bindings,
		CMUTIL_List *after,
		CMUTIL_String *obuf,
		CMUTIL_JsonObject *outs,
		CMUTIL_List *rembuf)
{
	CMDBM_NodeType ntype = CMDBM_MapperGetNodeType(node);
	CMDBM_BuildFunc bfunc = g_cmdbm_buildfuncs[ntype];
	if (bfunc)
		return bfunc(sess, conn, node, params, bindings,
					 after, obuf, outs, rembuf);
	return CMFalse;
}

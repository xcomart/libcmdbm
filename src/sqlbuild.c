
#include "mapper.h"

CMUTIL_LogDefine("cmdbm.sqlbuild")

CMDBM_STATIC char* CMDBM_RevCaseEnds(char *q, char *limit, char *n)
{
	int ed = (int)strlen(n);
	char *p = n + ed - 1;
	while (limit < p && n < q && toupper(*q) == toupper(*p))
		q--, p--;
	return n == p && toupper(*q) == toupper(*p)? q:NULL;
}

CMDBM_STATIC char* CMDBM_CaseStarts(char *p, char *limit, char *n)
{
	while (*p && *n && p < limit && toupper(*p) == toupper(*n)) {
		p++; n++;
	}
	return (!*n)? p:NULL;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_TestExpr(CMDBM_Session *sess,
		CMUTIL_JsonObject *params, CMUTIL_Iterator *iter, CMUTIL_Bool isOpen)
{
	CMUTIL_Bool res = CMUTIL_False;
	while (CMUTIL_CALL(iter, HasNext)) {
		CMDBM_CompItem *c = (CMDBM_CompItem*)CMUTIL_CALL(iter, Next);
		if (c->type == CMDBM_ETOpen) {
			res = CMDBM_TestExpr(sess, params, iter, CMUTIL_True);
		} else if (c->type == CMDBM_ETClose) {
			return res;
		} else if (c->type == CMDBM_ETAnd) {
			if (!res) {
				if (isOpen) {
					while (CMUTIL_CALL(iter, HasNext)) {
						CMDBM_CompItem *o =
								(CMDBM_CompItem*)CMUTIL_CALL(iter, Next);
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
					while (CMUTIL_CALL(iter, HasNext)) {
						CMDBM_CompItem *o =
								(CMDBM_CompItem*)CMUTIL_CALL(iter, Next);
						if (o->type == CMDBM_ETClose)
							return res;
					}
				} else {
					return res;
				}
			}
		} else {
			const char *sp1 = CMUTIL_CALL(c->aname, GetCString);
			const char *sp2 = CMUTIL_CALL(c->bname, GetCString);
			if (!c->aconst) {
				sp1 = CMUTIL_CALL(params, GetCString, sp1);
			}
			if (!c->bconst) {
				sp2 = CMUTIL_CALL(params, GetCString, sp2);
			}
			res = c->comparator(sp1, sp2);
		}
	}
	return res;
}

CMUTIL_Bool CMDBM_BuildNode(
		CMDBM_Session *sess,
		CMDBM_Connection *conn,
		CMUTIL_XmlNode *node,
		CMUTIL_JsonObject *params,
		CMUTIL_JsonArray *bindings,
		CMUTIL_List *after,
		CMUTIL_String *obuf,
		CMUTIL_JsonObject *outs,
		CMUTIL_List *rembuf);

CMDBM_STATIC CMUTIL_Bool CMDBM_BuildChildren(
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
	int i, size = CMUTIL_CALL(node, ChildCount);

	for (i=0; i<size; i++) {
		CMUTIL_XmlNode *cld = CMUTIL_CALL(node, ChildAt, i);
		if (!CMDBM_BuildNode(sess, conn, cld, params, bindings,
							 after, obuf, outs, rembuf))
			return CMUTIL_False;
	}
	return CMUTIL_True;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_BuildText(
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
	const char *text = CMUTIL_CALL(node, GetName);
	CMUTIL_CALL(obuf, AddString, text);
	CMUTIL_UNUSED(sess, conn, params, bindings, after, outs, rembuf);
	return CMUTIL_True;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_BuildParamSet(
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

	name = CMUTIL_CALL(node, GetAttribute, "name");
	val = CMUTIL_CALL(node, GetAttribute, "value");
	type = CMUTIL_CALL(node, GetAttribute, "type");	// optional(string default)
	if (name && val && params) {
		const char *sname = CMUTIL_CALL(name, GetCString);
		const char *sval = CMUTIL_CALL(val, GetCString);
		const char *stype = "string";
		if (type) stype = CMUTIL_CALL(type, GetCString);
		if (strcasecmp(stype, "string") == 0) {
			CMUTIL_CALL(params, PutString, sname, sval);
		} else if (strcasecmp(stype, "int") ||
				   strcasecmp(stype, "long")) {
			CMUTIL_CALL(params, PutLong, sname, atoll(sval));
		} else if (strcasecmp(stype, "float") ||
				   strcasecmp(stype, "double")) {
			CMUTIL_CALL(params, PutDouble, sname, atof(sval));
		} else {
			CMLogErrorS("unknown parameter type: %s", sval);
			return CMUTIL_False;
		}
		return CMUTIL_True;
	}
	CMUTIL_UNUSED(sess, conn, bindings, after, obuf, outs, rembuf);
	return CMUTIL_False;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_BuildOutParam(
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
	const char *key = CMUTIL_CALL(node, GetName);
	char pbuf[1024];
	int idx = CMUTIL_CALL(bindings, GetSize);
	CMUTIL_Json *value = CMUTIL_CALL(params, Get, key);

	CMUTIL_CALL(conn, GetBindString, idx, pbuf);
	CMUTIL_CALL(obuf, AddString, pbuf);
	if (!value) {
		CMUTIL_CALL(params, PutString, key, "1");
		value = CMUTIL_CALL(params, Get, key);
	}
	sprintf(pbuf, "%d", idx);
	CMUTIL_CALL(outs, Put, pbuf, value);
	CMUTIL_CALL(bindings, Add, value);
	CMUTIL_UNUSED(sess, after, rembuf);
	return CMUTIL_True;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_BuildReplace(
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
	const char *key = CMUTIL_CALL(node, GetName);
	CMUTIL_Json *pitem = CMUTIL_CALL(params, Get, key);
	CMUTIL_JsonType jtype = CMUTIL_CALL(pitem, GetType);
	CMUTIL_UNUSED(sess, conn, bindings, after, outs, rembuf);
	if (jtype == CMUTIL_JsonTypeValue) {
		CMUTIL_String *str = CMUTIL_CALL(params, GetString, key);
		CMUTIL_CALL(obuf, AddAnother, str);
		return CMUTIL_True;
	} else {
		CMLogErrorS("replacement of parameter is not value type.(key:%s)", key);
		return CMUTIL_False;
	}
}

CMDBM_STATIC CMUTIL_Bool CMDBM_BuildInclude(
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

	const char *nodename = CMUTIL_CALL(node, GetName);
	refqry = (CMUTIL_XmlNode*)CMUTIL_CALL(conn, GetQuery, nodename);
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
	return CMUTIL_False;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_BuildBind(
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
	const char *key = CMUTIL_CALL(node, GetName);
	CMUTIL_Json *data = CMUTIL_CALL(params, Get, key);
	CMUTIL_UNUSED(sess, after, outs, rembuf);
	if (data) {
		int index = CMUTIL_CALL(bindings, GetSize);
		char buf[50];
		CMUTIL_CALL(conn, GetBindString, index, buf);
		CMUTIL_CALL(obuf, AddString, buf);
		CMUTIL_CALL(bindings, Add, data);
		return CMUTIL_True;
	} else {
		CMLogErrorS("parameter has no key: %s", key);
		return CMUTIL_False;
	}
}

CMDBM_STATIC CMUTIL_Bool CMDBM_BuildTrim(
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
	CMUTIL_Bool res = CMUTIL_False;
	CMUTIL_String *sbuf = CMUTIL_StringCreate();

	if (CMDBM_BuildChildren(sess, conn, node, params, bindings,
							after, sbuf, outs, rembuf)) {
		CMUTIL_String *sprfx = CMUTIL_CALL(node,GetAttribute,"prefix");
		CMUTIL_String *ssufx = CMUTIL_CALL(node,GetAttribute,"suffix");
		CMUTIL_String *sprov = CMUTIL_CALL(node,GetAttribute,"prefixOverrides");
		CMUTIL_String *ssuov = CMUTIL_CALL(node,GetAttribute,"suffixOverrides");
		const char *prfx = CMUTIL_CALL(sprfx, GetCString);
		const char *sufx = CMUTIL_CALL(ssufx, GetCString);
		const char *prov = CMUTIL_CALL(sprov, GetCString);
		const char *suov = CMUTIL_CALL(ssuov, GetCString);

		char *p = (char*)CMUTIL_CALL(sbuf, GetCString);
		char *q = p, *r;
		CMUTIL_StringArray *subs = NULL;
		int i;

		// save string end pointer
		r = p + CMUTIL_CALL(sbuf, GetSize);

		// remove preceeding spaces
		while (*p && strchr(CMDBM_SPACES, *p)) p++;
		// remove trailing spaces
		while (strchr(CMDBM_SPACES, *(r-1)) && p < (r-1)) r--;

		// override suffix
		if (suov) {
			subs = CMUTIL_StringSplit(suov, "|");
			for (i=0; i<CMUTIL_CALL(subs, GetSize); i++) {
				CMUTIL_String *sd = CMUTIL_CALL(subs, GetAt, i);
				char *d = (char*)CMUTIL_CALL(sd, GetCString);
				q = CMDBM_RevCaseEnds(r-1, p, d);
				if (q && (strchr(CMDBM_SQLDELIMS, *(q-1)) ||
						  strchr(CMDBM_SQLDELIMS, *d))) {
					r = q;
					break;
				}
			}
			CMUTIL_CALL(subs, Destroy);
		}

		// override prefix
		if (prov) {
			subs = CMUTIL_StringSplit(prov, "|");
			for (i=0; i<CMUTIL_CALL(subs, GetSize); i++) {
				CMUTIL_String *sd = CMUTIL_CALL(subs, GetAt, i);
				char *d = (char*)CMUTIL_CALL(sd, GetCString);
				q = CMDBM_CaseStarts(p, r, d);
				if (q && (strchr(CMDBM_SQLDELIMS, *q) ||
						strchr(CMDBM_SQLDELIMS, *d))) {
					p = q;
					break;
				}
			}
			CMUTIL_CALL(subs, Destroy);
		}

		// remove preceeding spaces
		while (*p && strchr(CMDBM_SPACES, *p) && p < r) p++;

		if (p < r) {
			CMUTIL_CALL(obuf, AddChar, ' ');
			if (prfx)
				CMUTIL_CALL(obuf, AddString, prfx);
			CMUTIL_CALL(obuf, AddNString, p, r-p);
			CMUTIL_CALL(obuf, AddChar, ' ');
			if (sufx)
				CMUTIL_CALL(obuf, AddString, sufx);
		}

		res = CMUTIL_True;
	}

	CMUTIL_CALL(sbuf, Destroy);
	return res;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_BuildForeach(
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
	int i, size;

	scoll = CMUTIL_CALL(node, GetAttribute, "collection");
	sopen = CMUTIL_CALL(node, GetAttribute, "open");
	sclose = CMUTIL_CALL(node, GetAttribute, "close");
	ssep = CMUTIL_CALL(node, GetAttribute, "separator");

	if (scoll == NULL) {
		CMLogErrorS("foreach tag must have collection attribute.");
		return CMUTIL_False;
	}
	scolkey = CMUTIL_CALL(scoll, GetCString);

	// dynamic named variables.
	sitem = CMUTIL_CALL(node, GetAttribute, "item");
	sindex = CMUTIL_CALL(node, GetAttribute, "index");

	if (sitem == NULL)
		CMLogWarn("foreach tag has no item attribute. are you intended?");

	sitemkey = sitem? CMUTIL_CALL(sitem, GetCString):NULL;
	sindexkey = sindex? CMUTIL_CALL(sindex, GetCString):NULL;

	collection = (CMUTIL_JsonArray*)CMUTIL_CALL(params, Get, scolkey);
	if (collection == NULL) {
		CMLogErrorS("parameter have no collection with key '%s'", scolkey);
		return CMUTIL_False;
	}
	if (CMUTIL_CALL(&(collection->parent), GetType) != CMUTIL_JsonTypeArray) {
		CMLogErrorS("parameter item '%s' is not a collection.", scolkey);
		return CMUTIL_False;
	}
	size = CMUTIL_CALL(collection, GetSize);

	itembackup = sitem? CMUTIL_CALL(params, Remove, sitemkey):NULL;
	indexbackup = sindex? CMUTIL_CALL(params, Remove, sindexkey):NULL;

	if (sopen) CMUTIL_CALL(obuf, AddAnother, sopen);
	for (i=0; i<size; i++) {
		CMUTIL_Json *item = CMUTIL_CALL(collection, Get, i);
		CMUTIL_Json *idx;
		CMUTIL_CALL(params, Put, sitemkey, item);
		CMUTIL_CALL(params, PutLong, sindexkey, i);

		// build child nodes
		if (!CMDBM_BuildChildren(sess, conn, node, params, bindings,
								 after, obuf, outs, rembuf))
			return CMUTIL_False;

		if (ssep && i < (size-1))
			CMUTIL_CALL(obuf, AddAnother, ssep);

		// remove item for not be destroyed.
		CMUTIL_CALL(params, Remove, sitemkey);

		// save index item to destroy after execution.
		idx = CMUTIL_CALL(params, Remove, sindexkey);
		CMUTIL_CALL(rembuf, AddTail, idx);
	}
	if (sclose) CMUTIL_CALL(obuf, AddAnother, sclose);

	if (itembackup) CMUTIL_CALL(params, Put, sitemkey, itembackup);
	if (indexbackup) CMUTIL_CALL(params, Put, sindexkey, indexbackup);

	return CMUTIL_True;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_BuildIfBase(
		CMDBM_Session *sess,
		CMDBM_Connection *conn,
		CMUTIL_XmlNode *node,
		CMUTIL_JsonObject *params,
		CMUTIL_JsonArray *bindings,
		CMUTIL_List *after,
		CMUTIL_String *obuf,
		CMUTIL_JsonObject *outs,
		CMUTIL_List *rembuf,
		CMUTIL_Bool *isAdded)
{
	CMUTIL_Bool res = CMUTIL_False;
	CMUTIL_List *data = CMUTIL_CALL(node, GetUserData);
	CMUTIL_Iterator *iter = CMUTIL_CALL(data, Iterator);
	if (CMDBM_TestExpr(sess, params, iter, CMUTIL_False)) {
		res = CMDBM_BuildChildren(sess, conn, node, params, bindings,
								  after, obuf, outs, rembuf);
		if (isAdded)
			*isAdded = CMUTIL_True;
	} else {
		res = CMUTIL_True;
	}
	if (iter)
		CMUTIL_CALL(iter, Destroy);
	return res;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_BuildChoose(
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
	CMUTIL_Bool isApplied = CMUTIL_False;
	int i, size = CMUTIL_CALL(node, ChildCount);

	for(i=0; i<size && !isApplied; i++) {
		CMUTIL_XmlNode *child = CMUTIL_CALL(node, ChildAt, i);
		switch(CMDBM_MapperGetNodeType(child)) {
		case CMDBM_NTSqlIf:
			if (!isApplied) {
				if (!CMDBM_BuildIfBase(sess, conn, child, params, bindings,
									   after, obuf, outs, rembuf, &isApplied))
					return CMUTIL_False;
			}
			break;
		case CMDBM_NTSqlOtherwise:
			if (!isApplied) {
				if (!CMDBM_BuildNode(sess, conn, child, params, bindings,
									 after, obuf, outs, rembuf))
					return CMUTIL_False;
				isApplied = CMUTIL_True;
			}
			break;
		default:
			if (!CMDBM_BuildNode(sess, conn, child, params, bindings,
								 after, obuf, outs, rembuf))
				return CMUTIL_False;
			break;
		}
	}

	return CMUTIL_True;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_BuildIf(
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

CMDBM_STATIC CMUTIL_Bool CMDBM_BuildSelectKey(
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
	CMUTIL_Bool res = CMUTIL_False;
	const char *key, *order;
	CMUTIL_String *sbuf = CMUTIL_StringCreate();
	CMUTIL_JsonArray *nbinds = CMUTIL_JsonArrayCreate();
	CMUTIL_Bool beval = CMUTIL_False;
	CMUTIL_String *stmp;

	CMUTIL_UNUSED(bindings, obuf);

	stmp = CMUTIL_CALL(node, GetAttribute, "order");
	order = CMUTIL_CALL(stmp, GetCString);
	if (order && strcasecmp(order, "before") == 0) {
		beval = CMUTIL_True;
	} else {
		if (CMUTIL_CALL(after, Remove, node) != NULL)
			// calling after execution of main query so execute it.
			beval = CMUTIL_True;
		else
			// main query evaluation, so add it to after list.
			CMUTIL_CALL(after, AddTail, node);
	}
	if (beval) {
		stmp = CMUTIL_CALL(node, GetAttribute, "keyProperty");
		key = CMUTIL_CALL(stmp, GetCString);

		res = CMDBM_BuildChildren(
					sess, conn, node, params, nbinds, after, sbuf, outs, rembuf);
		if (res) {
			CMUTIL_JsonValue *value =
					CMUTIL_CALL(conn, GetObject, sbuf, nbinds, NULL);
			if (value) {
				CMUTIL_CALL(params, Put, key, (CMUTIL_Json*)value);
				res = CMUTIL_True;
			} else {
				CMLogError("query execution failed for selectKey. -> %s", sbuf);
			}
		}
	} else {
		res = CMUTIL_True;
	}
	CMUTIL_JsonDestroy(nbinds);
	CMUTIL_CALL(sbuf, Destroy);
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

CMUTIL_Bool CMDBM_BuildNode(
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
	return CMUTIL_False;
}

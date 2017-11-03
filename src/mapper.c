
#include "mapper.h"

#include <math.h>

CMUTIL_LogDefine("cmdbm.mapper")

#define CMDBM_OPERS       "!=<>"
#define CMDBM_ALLDELIMS   " \r\n\t!=<>()#${}"

#define MapperError(n,f,...) do {							\
    char fbuf[1024]; \
    const char *id=CMDBM_MapperGetId(n, CMTrue);	\
	sprintf(fbuf, "mapping error occurred in '%s': %s",		\
			id? id:"unknown", f);							\
	CMLogErrorS(fbuf, ## __VA_ARGS__);						\
} while(0)

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperRebuildItem(
		CMUTIL_Map *queries, CMUTIL_XmlNode *node);
CMDBM_STATIC const char *CMDBM_MapperGetId(
        CMUTIL_XmlNode *node, CMUTIL_Bool slient);

CMDBM_STATIC CMUTIL_XmlNode *CMDBM_MapperXmlCreateText(
        const char *a, size_t len)
{
	const char *p = a;
    while (len > 0 && strchr(CMDBM_SPACES, *p)) {
        p++; len--;
    }

	if (len > 0) {
        if (strchr(CMDBM_SPACES, *a) && a < p) {
            p--; len++;
        }
		return CMUTIL_XmlNodeCreateWithLen(CMUTIL_XmlNodeText, p, len);
	} else {
		return CMUTIL_XmlNodeCreateWithLen(CMUTIL_XmlNodeText, " ", 1);
	}
}

CMDBM_STATIC void CMDBM_CompItemDestroy(CMDBM_CompItem* item)
{
	if (item) {
		if (item->aname)
			CMCall(item->aname, Destroy);
		if (item->bname)
			CMCall(item->bname, Destroy);
		CMFree(item);
	}
}

CMDBM_STATIC const char* CMDBM_MapperNextToken(
		register const char *inp,
		register char *buf,
		char *delims,
		CMUTIL_Bool *isconst,
		CMUTIL_Bool dlmtoken,
		const char **prev)
{
	if (inp && buf && delims) {
		char delim = 0;
		CMUTIL_Bool isEscape = CMFalse;
		CMUTIL_Bool isNumeric = CMFalse;

		// save previous position
		if (prev)
			*prev = inp;

		// remove preceeding spaces
		while (strchr(CMDBM_SPACES, *inp)) inp++;

		if (strchr("'\"", *inp)) {
			delim = *inp;
			*isconst = CMTrue;
			inp++;
		} else if (strchr("+-0123456789", *inp)){
			isNumeric = CMTrue;
		} else {
			*isconst = CMFalse;
		}

		while (*inp) {
			if (isEscape) {
				isEscape = CMFalse;
			} else {
				if (*inp == '\\') {
					isEscape = CMTrue;
					inp++;
					continue;
				} else if (*isconst && *inp == delim) {
					inp++;
					break;
				} else if (!*isconst && (
						   (!dlmtoken && strchr(delims, *inp)) ||
						   (dlmtoken && !strchr(delims, *inp)))) {
					break;
				}
			}
			if (isNumeric && strchr("+-0123456789", *inp) == NULL)
				isNumeric = CMFalse;
			*buf++ = *inp++;
		}
		*buf = 0x0;
		if (isNumeric)
			*isconst = CMTrue;
		return inp;
	} else {
		return NULL;
	}
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperIsNumeric(const char *a)
{
	return strchr("+-0123456789.", *a)? CMTrue:CMFalse;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperNumEquals(const char* a, const char* b)
{
	double af = atof(a);
	double bf = atof(b);
    // compare with 7 significant digits after floating point.
    // drop remaining digits for comparison.
    return fabs(af - bf) < 1e7 ? CMTrue:CMFalse;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperEqual(const char* a, const char* b)
{
	register const char *s = a, *e = b;
	char delim;

	if (b == NULL || strcasecmp(b, "null") == 0)
		return a == NULL? CMTrue:CMFalse;
	if (a == NULL)
		return CMFalse;

	if (*e) {
		CMUTIL_Bool isEscape = CMFalse;
		CMUTIL_Bool hasQuote = CMFalse;
		// check numeric comparison.
		if (CMDBM_MapperIsNumeric(e))
			return CMDBM_MapperNumEquals(a, b);

		// start quote
		if (strchr("\'\"", *e)) {
			hasQuote = CMTrue;
			delim = *e++;
		}
		while (*e && *s) {
			if (isEscape) {
				isEscape = CMFalse;
			} else if (*e == '\\') {
				e++;
				isEscape = CMTrue;
				continue;
			}
			if (*e != *s)
				return CMFalse;
			e++; s++;
		}

		// both of parameters has no remain characters
		if (((hasQuote && *e == delim) || !*e) && !*s)
			return CMTrue;

		return CMFalse;
	} else {
		return *a == *b? CMTrue:CMFalse;
	}
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperNotEqual(const char *a, const char *b)
{
	return CMDBM_MapperEqual(a, b)? CMFalse:CMTrue;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperGreaterThan(const char* a, const char* b)
{
	double af = atof(a);
	double bf = atof(b);
	return af > bf? CMTrue:CMFalse;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperLessThan(const char* a, const char* b)
{
	double af = atof(a);
	double bf = atof(b);
	return af < bf? CMTrue:CMFalse;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperGreaterThanOrEqual(
		const char* a, const char* b)
{
	// this means not less than.
	return CMDBM_MapperLessThan(a, b)? CMFalse:CMTrue;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperLessThanOrEqual(
		const char* a, const char* b)
{
	// this means not greater than
	return CMDBM_MapperGreaterThan(a, b)? CMFalse:CMTrue;
}

CMDBM_STATIC CMDBM_TestFunc CMDBM_MapperOperatorToFunc(char *op)
{
	CMUTIL_Bool eq, gt, lt, nt;
	eq = gt = lt = nt = CMFalse;

	while (*op) {
		switch (*op) {
		case '!': nt = CMTrue; break;
		case '=': eq = CMTrue; break;
		case '>': gt = CMTrue; break;
		case '<': lt = CMTrue; break;
		}
		op++;
	}

	if (nt && eq)       // !=, =!
		return CMDBM_MapperNotEqual;
	else if (gt && eq)  // >=, =>
		return CMDBM_MapperGreaterThanOrEqual;
	else if (lt && eq)  // <=, =<
		return CMDBM_MapperLessThanOrEqual;
	else if (gt && lt)  // <>, ><
		return CMDBM_MapperNotEqual;
	else if (gt)        // >
		return CMDBM_MapperGreaterThan;
	else if (lt)        // <
		return CMDBM_MapperLessThan;
	else if (eq)        // =, ==
		return CMDBM_MapperEqual;

	// no comparable operator found
	return NULL;
}

CMDBM_STATIC const char *CMDBM_MapperGetAttr(
		CMUTIL_XmlNode *node, const char *attr, CMUTIL_Bool silent)
{
	if (node) {
		CMUTIL_String *sattr = CMCall(node, GetAttribute, attr);
		if (sattr)
            return CMCall(sattr, GetCString);
		else
			return CMDBM_MapperGetAttr(
						CMCall(node, GetParent), attr, silent);
	} else {
		if (!silent)
			MapperError(node, "attribute '%s' does not exists.", attr);
	}
	return NULL;
}

CMDBM_STATIC const char *CMDBM_MapperGetId(CMUTIL_XmlNode *node, CMUTIL_Bool slient)
{
	CMUTIL_String *id = CMCall(node, GetAttribute, "CMDBM_IDRebuilt");
	if (id == NULL) {
        const char *sns = CMDBM_MapperGetAttr(node, "namespace", CMTrue);
        const char *sid = CMDBM_MapperGetAttr(node, "id", CMTrue);
		if (sns && sid && strchr(sid, '.') == NULL) {
			CMUTIL_String *newid = CMUTIL_StringCreate();
			const char *snewid;
			CMCall(newid, AddPrint, "%s.%s", sns, sid);
			snewid = CMCall(newid, GetCString);
			CMCall(node, SetAttribute, "CMDBM_IDRebuilt", snewid);
			CMCall(newid, Destroy);
			id = CMCall(node, GetAttribute, "CMDBM_IDRebuilt");
		} else {
			if (!slient) {
				if (sns == NULL) {
					MapperError(node, "mapper has no namespace.");
				} else if (sid == NULL) {
					MapperError(node, "'id' attribute required.");
				} else {
					MapperError(node, "id cannot contain 'dot'(.).");
				}
			}
		}
	}
	if (id != NULL)
        return CMCall(id, GetCString);
	return NULL;
}

CMDBM_STATIC const char *CMDBM_MapperGetNamespace(CMUTIL_XmlNode *node)
{
	return CMDBM_MapperGetAttr(node, "namespace", CMTrue);
}

CMDBM_STATIC void CMDBM_MapperItemProc(
		CMUTIL_Map *queries,
		CMUTIL_XmlNode *node,
		CMDBM_NodeType type)
{
	char buf[10];
	sprintf(buf, "%d", (int)type);
	CMUTIL_UNUSED(queries);
	CMCall(node, SetAttribute, "CMDBM_NodeType", buf);
}

CMDBM_NodeType CMDBM_MapperGetNodeType(CMUTIL_XmlNode *node)
{
	CMDBM_NodeType res;
	CMUTIL_String *ntype = NULL;
	CMUTIL_XmlNodeKind type = CMCall(node, GetType);
	res = type == CMUTIL_XmlNodeTag? CMDBM_NTXmlTag:CMDBM_NTXmlText;
	ntype = CMCall(node, GetAttribute, "CMDBM_NodeType");
	if (ntype)
        res = (CMDBM_NodeType)(CMDBM_NTXmlText +
                               atoi(CMCall(ntype, GetCString)) - 1);
	return res;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperItemAddProc(
		CMUTIL_Map *queries,
		CMUTIL_XmlNode *node,
		CMDBM_NodeType type)
{
	const char *qid;
	CMDBM_MapperItemProc(queries, node, type);
	qid = CMDBM_MapperGetId(node, CMFalse);
	if (qid) {
		CMCall(queries, Put, qid, node);
		CMLogTrace("query %s added to repository.", qid);
		return CMTrue;
	} else {
		return CMFalse;
	}
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperItemSqlMap(
		CMUTIL_Map *queries, CMUTIL_XmlNode *node)
{
	CMDBM_MapperItemProc(queries, node, CMDBM_NTSqlMap);
	return CMTrue;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperItemSql(
		CMUTIL_Map *queries, CMUTIL_XmlNode *node)
{
	return CMDBM_MapperItemAddProc(queries, node, CMDBM_NTSqlFrag);
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperItemSelect(
		CMUTIL_Map *queries, CMUTIL_XmlNode *node)
{
	return CMDBM_MapperItemAddProc(queries, node, CMDBM_NTSqlSelect);
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperItemUpdate(
		CMUTIL_Map *queries, CMUTIL_XmlNode *node)
{
	return CMDBM_MapperItemAddProc(queries, node, CMDBM_NTSqlUpdate);
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperItemInclude(
		CMUTIL_Map *queries, CMUTIL_XmlNode *node)
{
	CMUTIL_Bool res = CMFalse;
	CMUTIL_String *refid = NULL;

	CMDBM_MapperItemProc(NULL, node, CMDBM_NTSqlInclude);

	refid = CMCall(node, GetAttribute, "refid");
	if (refid) {
		char idbuf[1024];
		const char *srefid = CMCall(refid, GetCString);

		// build absolute query reference ID
		if (strchr(srefid, '.') == NULL) {
			sprintf(idbuf, "%s.%s",
					CMDBM_MapperGetNamespace(node),
					srefid);
			srefid = idbuf;
		}
		CMCall(node, SetName, srefid);
		res = CMTrue;
	}
	CMUTIL_UNUSED(queries);
	return res;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperItemBind(
		CMUTIL_Map *queries, CMUTIL_XmlNode *node)
{
	CMDBM_MapperItemProc(queries, node, CMDBM_NTSqlParamSet);
	return CMTrue;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperItemTrim(
		CMUTIL_Map *queries, CMUTIL_XmlNode *node)
{
	CMDBM_MapperItemProc(queries, node, CMDBM_NTSqlTrim);
	return CMTrue;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperItemForeach(
		CMUTIL_Map *queries, CMUTIL_XmlNode *node)
{
	CMDBM_MapperItemProc(queries, node, CMDBM_NTSqlForeach);
	return CMTrue;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperItemTrimLike(
		CMUTIL_Map *queries,
		CMUTIL_XmlNode *node,
		const char *prfx,
		const char *prfxo,
		const char *sufxo)
{
	CMUTIL_UNUSED(queries);
	CMDBM_MapperItemProc(queries, node, CMDBM_NTSqlTrim);
	if (prfx)
		CMCall(node, SetAttribute, "prefix", prfx);
	if (prfxo)
		CMCall(node, SetAttribute, "prefixOverrides", prfxo);
	if (sufxo)
		CMCall(node, SetAttribute, "suffixOverrides", sufxo);
	return CMTrue;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperItemWhere(
		CMUTIL_Map *queries, CMUTIL_XmlNode *node)
{
	return CMDBM_MapperItemTrimLike(
			queries, node, "WHERE ", "AND|OR", "AND|OR");
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperItemSet(
		CMUTIL_Map *queries, CMUTIL_XmlNode *node)
{
	return CMDBM_MapperItemTrimLike(
			queries, node, "SET ", ",", ",");
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperItemChoose(
		CMUTIL_Map *queries, CMUTIL_XmlNode *node)
{
	CMDBM_MapperItemProc(queries, node, CMDBM_NTSqlChoose);
	return CMTrue;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperItemOtherwise(
		CMUTIL_Map *queries, CMUTIL_XmlNode *node)
{
	CMDBM_MapperItemProc(queries, node, CMDBM_NTSqlOtherwise);
	return CMTrue;
}

CMDBM_STATIC void CMDBM_CompListDestroyer(void *a)
{
	CMCall((CMUTIL_List*)a, Destroy);
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperItemIf(
		CMUTIL_Map *queries, CMUTIL_XmlNode *node)
{
	if (node) {
		char buf[1024];
		CMUTIL_String *test = CMCall(node, GetAttribute, "test");
		const char *p = test? CMCall(test, GetCString):NULL;
		CMUTIL_List *data = (CMUTIL_List*)CMCall(node, GetUserData);

		if (data == NULL) {
			data = CMUTIL_ListCreateEx((void(*)(void*))CMDBM_CompItemDestroy);
			CMCall(node, SetUserData, data, CMDBM_CompListDestroyer);
		}
		CMDBM_MapperItemProc(queries, node, CMDBM_NTSqlIf);

		// parse tests
		while (CMTrue) {
			CMDBM_CompItem *item = CMAlloc(sizeof(CMDBM_CompItem));
			CMUTIL_Bool isconst = CMFalse;
			const char *prev = NULL;

			memset(item, 0x0, sizeof(CMDBM_CompItem));

			// remove preceeding spaces
			while (*p && strchr(CMDBM_SPACES, *p)) p++;

			if (!*p) {
				// we've got end of test.
				CMFree(item);
				break;
			} else if (*p == '(') {
				p++;
				item->type = CMDBM_ETOpen;
			} else if (*p == ')') {
				p++;
				item->type = CMDBM_ETClose;
			} else {
				// get first operand
				p = CMDBM_MapperNextToken(
						p, buf, CMDBM_ALLDELIMS, &isconst, CMFalse, &prev);
				if (!p) {
					MapperError(node, "cannot get next token, "
								"former operand expected.");
					goto FAILEDPOSITION;
				}

				if (strcasecmp("and", buf) == 0) {
					item->type = CMDBM_ETAnd;
				} else if (strcasecmp("or", buf) == 0) {
					item->type = CMDBM_ETOr;
				} else {
					item->aname = CMUTIL_StringCreateEx(10, buf);
					item->aconst = isconst;

					// get operator.
					p = CMDBM_MapperNextToken(p, buf, CMDBM_OPERS, &isconst,
							CMTrue, &prev);
					if (!p) {
						MapperError(node, "cannot get next token, "
									"operator expected.");
						goto FAILEDPOSITION;
					}

					// set comparator function corresponding to operator.
					item->comparator = CMDBM_MapperOperatorToFunc(buf);
					if (!item->comparator) {
						MapperError(node, "invalid operator. "
										  "no comparator exists "
										  "for operator: %s", buf);
						goto FAILEDPOSITION;
					}

					// get second operand.
					p = CMDBM_MapperNextToken(
								p, buf, CMDBM_ALLDELIMS, &isconst,
								CMFalse, &prev);
					if (!p) {
						MapperError(node, "cannot get next token. "
									"latter operand expected: ");
						goto FAILEDPOSITION;
					}
					item->bname = CMUTIL_StringCreateEx(10, buf);
					item->bconst = isconst;
					item->type = CMDBM_ETComp;
				}
			}

			CMCall(data, AddTail, item);
			continue;
FAILEDPOSITION:
			CMDBM_CompItemDestroy(item);
			return CMFalse;
		}
		return CMTrue;
	} else {
		return CMFalse;
	}
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperItemSelectKey(
		CMUTIL_Map *queries, CMUTIL_XmlNode *node)
{
	CMDBM_MapperItemProc(queries, node, CMDBM_NTSqlSelectKey);
	return CMTrue;
}

static CMUTIL_Map *g_cmdbm_mapper_tagfuncs = NULL;

void CMDBM_MapperInit()
{
	g_cmdbm_mapper_tagfuncs = CMUTIL_MapCreate();
#define MAPPER_FUNC(a, b)	CMCall(g_cmdbm_mapper_tagfuncs, Put, a, (void*)b);

	MAPPER_FUNC("mapper"	,CMDBM_MapperItemSqlMap		);
	MAPPER_FUNC("sql"		,CMDBM_MapperItemSql		);
	MAPPER_FUNC("select"	,CMDBM_MapperItemSelect		);
	MAPPER_FUNC("update"	,CMDBM_MapperItemUpdate		);
	MAPPER_FUNC("delete"	,CMDBM_MapperItemUpdate		);
	MAPPER_FUNC("insert"	,CMDBM_MapperItemUpdate		);
	MAPPER_FUNC("include"	,CMDBM_MapperItemInclude	);
	MAPPER_FUNC("bind"		,CMDBM_MapperItemBind		);
	MAPPER_FUNC("trim"		,CMDBM_MapperItemTrim		);
	MAPPER_FUNC("foreach"	,CMDBM_MapperItemForeach	);
	MAPPER_FUNC("where"		,CMDBM_MapperItemWhere		);
	MAPPER_FUNC("set"		,CMDBM_MapperItemSet		);
	MAPPER_FUNC("choose"	,CMDBM_MapperItemChoose		);
	MAPPER_FUNC("if"		,CMDBM_MapperItemIf			);
	MAPPER_FUNC("selectKey"	,CMDBM_MapperItemSelectKey	);
	MAPPER_FUNC("otherwise"	,CMDBM_MapperItemOtherwise	);
	MAPPER_FUNC("when"		,CMDBM_MapperItemIf			);

}

void CMDBM_MapperClear()
{
	CMCall(g_cmdbm_mapper_tagfuncs, Destroy);
	g_cmdbm_mapper_tagfuncs = NULL;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperRebuildChildren(
		CMUTIL_Map *queries, CMUTIL_XmlNode *node)
{
	if (queries && node) {
        uint32_t i;
		for (i=0; i<CMCall(node, ChildCount); i++) {
			CMUTIL_XmlNode *cld = CMCall(node, ChildAt, i);
			if (!CMDBM_MapperRebuildItem(queries, cld))
				return CMFalse;
		}
		return CMTrue;
	}
	return CMFalse;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperRebuildTag(
		CMUTIL_Map *queries, CMUTIL_XmlNode *node)
{
	CMUTIL_Bool res = CMFalse;
	const char *tname = CMCall(node, GetName);
	CMDBM_TagFunc tf = (CMDBM_TagFunc)CMCall(
				g_cmdbm_mapper_tagfuncs, Get, tname);
	if (tf)
		res = tf(queries, node);

	if (res)
		res = CMDBM_MapperRebuildChildren(queries, node);

	return res;
}

CMDBM_STATIC CMUTIL_Bool CMDBM_MapperRebuildText(
		CMUTIL_Map *queries, CMUTIL_XmlNode *node)
{
	// process text node
	CMUTIL_XmlNode *child = NULL;
	char buf[1024];
    const char *p, *q;

	CMDBM_MapperItemProc(queries, node, CMDBM_NTSqlGroup);

    const char *r = CMCall(node, GetName);
	p = strstr(r, "#{");
	q = strstr(r, "${");
	while (CMTrue) {
        const char *s = NULL, *t = NULL;
		CMDBM_NodeType ntype;

		buf[0] = 0x0;
		s = p && q? (p < q? p:q):(p? p:(q? q:NULL));
		if (!s) break;

		if (s < r) break;

        child = CMUTIL_XmlNodeCreateWithLen(
                    CMUTIL_XmlNodeText, r, (uint64_t)(s-r));
		CMDBM_MapperItemProc(queries, child, CMDBM_NTSqlText);;
		CMCall(node, AddChild, child);
		ntype = p && s == p? CMDBM_NTSqlBind:CMDBM_NTSqlReplace;

		s += 2;
		r = strchr(s, '}');
        strncat(buf, s, (uint64_t)(r - s));
		if (ntype == CMDBM_NTSqlBind) {
			// check out parameter
            uint32_t i;
			CMUTIL_StringArray *subs = CMUTIL_StringSplit(buf, ",");
			if (CMCall(subs, GetSize) > 1) {
				for (i=1; i<CMCall(subs, GetSize); i++) {
					CMUTIL_StringArray *nv = CMUTIL_StringSplit(
								(char*)CMCall(subs, GetAt, i), "=");
					if (CMCall(nv, GetSize) > 1) {
						const char *n = CMCall(nv, GetCString, 0);
						const char *v = CMCall(nv, GetCString, 1);
						if (strcasecmp("mode", n) == 0 &&
								strcasecmp("out", v) == 0) {
							ntype = CMDBM_NTSqlOutParam;
							CMCall(nv, Destroy);
							break;
						}
					}
					CMCall(nv, Destroy);
				}
				// save parameter name to buf
				strcpy(buf, CMCall(subs, GetCString, 0));
			}
			CMCall(subs, Destroy);
		}

		t = CMUTIL_StrTrim(buf);
		child = CMUTIL_XmlNodeCreate(CMUTIL_XmlNodeTag, t);
		CMDBM_MapperItemProc(queries, child, ntype);
		CMCall(node, AddChild, child);
		r++;

		if (ntype == CMDBM_NTSqlBind)
			p = strstr(r, "#{");
		else
			q = strstr(r, "${");
	}

    p = CMCall(node, GetName);
	child = CMUTIL_XmlNodeCreateWithLen(
                CMUTIL_XmlNodeText, r, strlen(p)-(uint64_t)(r-p));
	CMDBM_MapperItemProc(queries, child, CMDBM_NTSqlText);
	CMCall(node, AddChild, child);

	CMCall(node, SetName, "");

	return CMTrue;
}

CMUTIL_Bool CMDBM_MapperRebuildItem(CMUTIL_Map *queries, CMUTIL_XmlNode *node)
{
	CMDBM_NodeType ntype = CMDBM_MapperGetNodeType(node);
    if (ntype == CMDBM_NTXmlTag) {
		return CMDBM_MapperRebuildTag(queries, node);
	} else {
		return CMDBM_MapperRebuildText(queries, node);
	}
}

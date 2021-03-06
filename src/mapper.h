#ifndef MAPPER_H__
#define MAPPER_H__

#include "functions.h"

typedef enum {
	 CMDBM_NTXmlText = 1
	,CMDBM_NTXmlTag

	,CMDBM_NTSqlGroup
	,CMDBM_NTSqlText
	,CMDBM_NTSqlParamSet
	,CMDBM_NTSqlOutParam
	,CMDBM_NTSqlReplace

	,CMDBM_NTSqlMap
	,CMDBM_NTSqlFrag
	,CMDBM_NTSqlSelect
	,CMDBM_NTSqlUpdate
	,CMDBM_NTSqlDelete
	,CMDBM_NTSqlInsert
	,CMDBM_NTSqlInclude
	,CMDBM_NTSqlBind
	,CMDBM_NTSqlTrim
	,CMDBM_NTSqlForeach
	,CMDBM_NTSqlWhere
	,CMDBM_NTSqlSet
	,CMDBM_NTSqlChoose
	,CMDBM_NTSqlOtherwise
	,CMDBM_NTSqlIf
	,CMDBM_NTSqlSelectKey
} CMDBM_NodeType;

typedef enum {
	 CMDBM_ETComp
	,CMDBM_ETAnd
	,CMDBM_ETOr
	,CMDBM_ETOpen
	,CMDBM_ETClose
} CMDBM_ExprType;

typedef CMBool (*CMDBM_TestFunc)(
		const char *a,
		const char *b);
typedef CMBool (*CMDBM_TagFunc)(
		CMUTIL_Map *queries,
		CMUTIL_XmlNode *item);
typedef CMBool (*CMDBM_BuildFunc)(
		CMDBM_Session *sess,
		CMDBM_Connection *db,
		CMUTIL_XmlNode *node,
		CMUTIL_JsonObject *params,
		CMUTIL_JsonArray *bindings,
		CMUTIL_List *after,
		CMUTIL_String *obuf,
		CMUTIL_JsonObject *outs,
		CMUTIL_List *rembuf);

typedef struct CMDBM_CompItem {
	CMDBM_ExprType	type;
	CMBool		isnot;
	CMUTIL_String	*aname;
	CMUTIL_String	*bname;
	CMBool		aconst;
	CMBool		bconst;
	CMDBM_TestFunc	comparator;
} CMDBM_CompItem;

CMDBM_NodeType CMDBM_MapperGetNodeType(CMUTIL_XmlNode *node);

#endif // MAPPER_H__


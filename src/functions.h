#ifndef FUNCTIONS_H__
#define FUNCTIONS_H__

#define CMDBM_EXPORT

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif


#include "types.h"
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>
#include <ctype.h>

#if defined(DEBUG)
# define CMDBM_STATIC
#else
# define CMDBM_STATIC	static
#endif

#define CMDBM_SPACES		" \r\n\t"
#define CMDBM_SQLDELIMS		" \r\r\n\t{}[]+%\'./():,*\"=<>@;-|!~^"


void CMDBM_MapperInit(void);
void CMDBM_MapperClear(void);


CMUTIL_Bool CMDBM_MapperRebuildItem(
		CMUTIL_Map *queries,
		CMUTIL_XmlNode *node);

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

CMDBM_Session *CMDBM_SessionCreate(
		CMDBM_ContextEx *ctx);

CMDBM_DBType CMDBM_DatabaseType(const char *dbtype);

#endif // FUNCTIONS_H__


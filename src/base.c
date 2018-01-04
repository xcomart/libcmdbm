
#include "functions.h"

void CMDBM_Init()
{
    CMUTIL_Init(CMMemSystem);
	CMDBM_MapperInit();
}

void CMDBM_Clear()
{
	CMDBM_MapperClear();
    CMUTIL_Clear();
}

const char *CMDBM_GetLibVersion()
{
    static char *verstr = LIBCMDBM_VER;
    return verstr;
}

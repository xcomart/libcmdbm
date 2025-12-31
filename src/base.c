
#include "functions.h"

#define LIBCMDBM_VER LIBCMDBM_VERSION_STRING

void CMDBM_Init()
{
    CMUTIL_Init(CMMemSystem);
    CMDBM_MapperInit();
    CMDBM_DatabaseInit();
}

void CMDBM_Clear()
{
    CMDBM_DatabaseClear();
    CMDBM_MapperClear();
    CMUTIL_Clear();
}

const char *CMDBM_GetLibVersion()
{
    static char *verstr = LIBCMDBM_VER;
    return verstr;
}

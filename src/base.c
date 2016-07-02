
#include "functions.h"

void CMDBM_Init()
{
    CMUTIL_Init(CMUTIL_MemSystem);
	CMDBM_MapperInit();
}

void CMDBM_Clear()
{
	CMDBM_MapperClear();
    CMUTIL_Clear();
}

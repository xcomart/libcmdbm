TEMPLATE = app
CONFIG += console
CONFIG -= app_bundle
CONFIG -= qt

SOURCES += \
    src/base.c \
    src/connection.c \
    src/context.c \
    src/database.c \
    src/mapper.c \
    src/session.c \
    src/sqlbuild.c \
    modules/cmdbm_mysql.c \
    modules/cmdbm_oracle.c \
    modules/cmdbm_pgsql.c

HEADERS += \
    src/functions.h \
    src/libcmdbm.h \
    src/mapper.h \
    src/types.h

DISTFILES += \
    data/cmdbm_config.json \
    data/cmdbm_config.xml \
    data/cmdbm_sqlmap.xml \
    data/cmdbm_config.dtd \
    data/cmdbm_sqlmap.dtd \
    README.md

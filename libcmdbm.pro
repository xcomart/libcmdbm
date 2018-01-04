TEMPLATE = lib
TARGET = cmdbm
CONFIG -= qt

VER_MAJ = 0
VER_MIN = 1
VER_PAT = 0

DEFINES += CMDBM_LIBRARY
CONFIG += CMDBM_MYSQL
CONFIG += CMDBM_ORACLE
CONFIG += CMDBM_ODBC
CONFIG += CMDBM_PGSQL

!win32-msvc* {
    CONFIG += link_pkgconfig
}

BUILD_DIR = ../build
COPY_CMD = cp
LIB_DIR = lib

win32 {
    LIB_DIR = bin
    win32-msvc* {
        COPY_CMD = copy
    }
}

contains(CONFIG, CMDBM_ORACLE) {
    DEFINES += CMDBM_ORACLE
    ORACLE_HOME = $$(ORACLE_HOME)
    INCLUDEPATH += $$ORACLE_HOME/sdk/include
    LIBS += -L$$ORACLE_HOME
    win32 {
        LIBS += -loci
    } else {
        LIBS += -lclntsh
    }
}

contains(CONFIG, CMDBM_MYSQL) {
    DEFINES += CMDBM_MYSQL
    !win32-msvc* {
        win32: {
            PKGCONFIG += mariadb
        } else {
            PKGCONFIG += mysqlclient
        }
    }
}

contains(CONFIG, CMDBM_ODBC) {
    DEFINES += CMDBM_ODBC
    win32 {
        LIBS += -lodbc32
    } else {
        LIBS += -lodbc
    }
}

contains(CONFIG, CMDBM_PGSQL) {
    DEFINES += CMDBM_PGSQL
    !win32-msvc* {
        PKGCONFIG += libpq
    }
}

DESTDIR = $$BUILD_DIR/$$LIB_DIR
CONFIG(debug, debug|release) {
    DEFINES += DEBUG
}

INCLUDEPATH += $$BUILD_DIR/include
INCLUDEPATH += src

LIBS += -L$$DESTDIR -lcmutils

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
    modules/cmdbm_pgsql.c \
    modules/cmdbm_odbc.c

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

!win32-msvc* {
    INCLUDE_TARGET.COMMANDS = $$COPY_CMD ../libcmdbm/src/libcmdbm.h $$BUILD_DIR/include

    QMAKE_EXTRA_TARGETS += INCLUDE_TARGET

    POST_TARGETDEPS += INCLUDE_TARGET
}

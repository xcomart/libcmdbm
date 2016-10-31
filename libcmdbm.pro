TEMPLATE = app
CONFIG += console
CONFIG -= app_bundle
CONFIG -= qt

DEFINES += CMDBM_LIBRARY

BUILD_DIR = ../build
COPY_CMD = cp
LIB_DIR = lib

win32 {
    LIB_DIR = bin
#    COPY_CMD = copy
}

CONFIG(debug, debug|release) {
    DEFINES += DEBUG
    DESTDIR = $$BUILD_DIR/$$LIB_DIR/debug
}

CONFIG(release, debug|release) {
    DEFINES += DEBUG
    DESTDIR = $$BUILD_DIR/$$LIB_DIR/release
}

INCLUDEPATH += $$BUILD_DIR/include

LIBS += -L$DESTDIR -lcmutils

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

INCLUDE_TARGET.COMMANDS = $$COPY_CMD ../libcmdbm/src/libcmdbm.h $$BUILD_DIR/include

QMAKE_EXTRA_TARGETS += INCLUDE_TARGET

POST_TARGETDEPS += INCLUDE_TARGET

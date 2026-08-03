
CMAKE_MINIMUM_REQUIRED ( VERSION 3.10 )

PROJECT ( cmdbm LANGUAGES C )

SET (CMAKE_C_STANDARD 99)

FILE ( READ "VERSION" VERSION_CONTENTS )
STRING ( STRIP ${VERSION_CONTENTS} VERSION_CONTENTS )
IF ( VERSION_CONTENTS MATCHES "([0-9]+)\\.([0-9]+)\\.([0-9]+)" )
    SET ( VERSION_MAJOR ${CMAKE_MATCH_1} )
    SET ( VERSION_MINOR ${CMAKE_MATCH_2} )
    SET ( VERSION_PATCH ${CMAKE_MATCH_3} )
ENDIF ()
MESSAGE ( STATUS "Configuring cmdbm version ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}" )
SET ( LIBCMDBM_VERSION_STRING ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH} )
ADD_COMPILE_DEFINITIONS( LIBCMDBM_VERSION_STRING="${LIBCMDBM_VERSION_STRING}" )

ADD_SUBDIRECTORY( libcmutils )

OPTION ( SUPPORT_MYSQL "Support MySQL database" OFF )
OPTION ( SUPPORT_MARIA "Support MariaDB database" ON )
OPTION ( SUPPORT_ORACLE "Support Oracle database" OFF )
OPTION ( SUPPORT_PGSQL "Support PgSQL database" ON )
OPTION ( SUPPORT_SQLITE "Support SQLite database" ON )
OPTION ( SUPPORT_ODBC "Support ODBC" ON )

INCLUDE_DIRECTORIES ( src )

MESSAGE( "building for ${CMAKE_SYSTEM_NAME}...")

SET ( LIB_SRCS
    src/base.c
    src/confxml.c
    src/connection.c
    src/context.c
    src/database.c
    src/mapper.c
    src/session.c
    src/sqlbuild.c
    modules/cmdbm_mysql.c
    modules/cmdbm_odbc.c
    modules/cmdbm_oracle.c
    modules/cmdbm_pgsql.c
    modules/cmdbm_sqlite.c )

ADD_LIBRARY ( cmdbm SHARED ${LIB_SRCS} )
ADD_LIBRARY ( cmdbmStatic STATIC ${LIB_SRCS} )

IF (APPLE)
    SET(ENV{PKG_CONFIG_PATH} "$ENV{PKG_CONFIG_PATH}:$ENV{HOMEBREW_PREFIX}/lib/pkgconfig")
ENDIF ()
# $ENV{HOMEBREW_PREFIX}/opt/unixodbc/lib/pkgconfig

FIND_PACKAGE ( PkgConfig REQUIRED )

SET ( CMDBM_TARGETS cmdbm cmdbmStatic )

# apply a pkg-config result to both the shared and the static target.
# link flags must be given to TARGET_LINK_LIBRARIES (not TARGET_LINK_OPTIONS),
# otherwise the shared library gets no DT_NEEDED entry for the client library
# and every application has to link it again by hand.
FUNCTION ( CMDBM_USE_PKG PREFIX )
    FOREACH ( TGT ${CMDBM_TARGETS} )
        TARGET_LINK_LIBRARIES( ${TGT} PRIVATE ${${PREFIX}_LDFLAGS} )
        TARGET_INCLUDE_DIRECTORIES( ${TGT} PRIVATE ${${PREFIX}_INCLUDE_DIRS} )
        TARGET_COMPILE_OPTIONS( ${TGT} PRIVATE ${${PREFIX}_CFLAGS_OTHER} )
    ENDFOREACH ()
ENDFUNCTION ()

TARGET_LINK_LIBRARIES( cmdbm PUBLIC cmutils )
TARGET_LINK_LIBRARIES( cmdbmStatic PUBLIC cmutilsStatic )
TARGET_INCLUDE_DIRECTORIES( cmdbm PUBLIC libcmutils/src )
TARGET_INCLUDE_DIRECTORIES( cmdbmStatic PUBLIC libcmutils/src )

IF ( SUPPORT_MYSQL )
    IF (APPLE)
        SET(ENV{PKG_CONFIG_PATH} "$ENV{PKG_CONFIG_PATH}:$ENV{HOMEBREW_PREFIX}/opt/mysql-connector-c/lib/pkgconfig")
    ENDIF ()
    IF ( APPLE )
        SET ( MYSQLCLIENT_BASE /usr/local/opt/mysql-connector-c )
        IF ( NOT EXISTS ${MYSQLCLIENT_BASE} )
            MESSAGE ( FATAL_ERROR "mysql client library not found" )
        ENDIF ()
        SET ( MYSQLCLIENT_LDFLAGS -L${MYSQLCLIENT_BASE}/lib -lmysqlclient )
        SET ( MYSQLCLIENT_INCLUDE_DIRS ${MYSQLCLIENT_BASE}/include )
    ELSE ()
        PKG_CHECK_MODULES ( MYSQLCLIENT REQUIRED mysqlclient )
    ENDIF ()
    MESSAGE( "found mysqlclient ${MYSQLCLIENT_LDFLAGS}" )
    CMDBM_USE_PKG ( MYSQLCLIENT )
    ADD_DEFINITIONS ( -DCMDBM_MYSQL )
ENDIF ( SUPPORT_MYSQL )

IF ( SUPPORT_MARIA )
    IF (APPLE)
        SET(ENV{PKG_CONFIG_PATH} "$ENV{PKG_CONFIG_PATH}:$ENV{HOMEBREW_PREFIX}/opt/mariadb-connector-c/lib/pkgconfig")
    ENDIF ()
    PKG_CHECK_MODULES ( MARIACLIENT REQUIRED libmariadb )
    MESSAGE( "found mariadb ${MARIACLIENT_LDFLAGS}" )
    CMDBM_USE_PKG ( MARIACLIENT )
    ADD_DEFINITIONS ( -DCMDBM_MARIA )
ENDIF ( SUPPORT_MARIA )

IF ( SUPPORT_ORACLE )
    MESSAGE( "found oracle $ENV{ORACLE_HOME}" )
    SET ( ORACLECLIENT_LDFLAGS -L$ENV{ORACLE_HOME} -lclntsh -lnnz )
    SET ( ORACLECLIENT_INCLUDE_DIRS $ENV{ORACLE_HOME}/sdk/include )
    CMDBM_USE_PKG ( ORACLECLIENT )
    ADD_DEFINITIONS ( -DCMDBM_ORACLE )
ENDIF ( SUPPORT_ORACLE )

IF ( SUPPORT_PGSQL )
    IF (APPLE)
        SET(ENV{PKG_CONFIG_PATH} "$ENV{PKG_CONFIG_PATH}:$ENV{HOMEBREW_PREFIX}/opt/libpq/lib/pkgconfig")
    ENDIF ()
    PKG_CHECK_MODULES ( LIBPQ REQUIRED libpq )
    MESSAGE( "found libpq ${LIBPQ_LDFLAGS}" )
    CMDBM_USE_PKG ( LIBPQ )
    ADD_DEFINITIONS ( -DCMDBM_PGSQL )
ENDIF ( SUPPORT_PGSQL )

IF ( SUPPORT_SQLITE )
    IF (APPLE)
        SET(ENV{PKG_CONFIG_PATH} "$ENV{PKG_CONFIG_PATH}:$ENV{HOMEBREW_PREFIX}/opt/sqlite/lib/pkgconfig")
    ENDIF ()
    PKG_CHECK_MODULES ( SQLITE REQUIRED sqlite3 )
    MESSAGE( "found sqlite3 ${SQLITE_LDFLAGS}" )
    CMDBM_USE_PKG ( SQLITE )
    ADD_DEFINITIONS ( -DCMDBM_SQLITE )
ENDIF ( SUPPORT_SQLITE )

IF ( SUPPORT_ODBC )
    IF (APPLE)
        SET(ENV{PKG_CONFIG_PATH} "$ENV{PKG_CONFIG_PATH}:$ENV{HOMEBREW_PREFIX}/opt/unixodbc/lib/pkgconfig")
    ENDIF ()
    PKG_CHECK_MODULES ( ODBC REQUIRED odbc )
    MESSAGE( "found odbc ${ODBC_LDFLAGS}" )
    CMDBM_USE_PKG ( ODBC )
    ADD_DEFINITIONS ( -DCMDBM_ODBC )
ENDIF ( SUPPORT_ODBC )

SET_TARGET_PROPERTIES ( cmdbm PROPERTIES VERSION ${LIBCMDBM_VERSION_STRING} )
SET_TARGET_PROPERTIES ( cmdbmStatic PROPERTIES OUTPUT_NAME cmdbm )

# the API reference. adds the cmdbm_docs target when doxygen is installed,
# and says so when it is not.
ADD_SUBDIRECTORY ( doc )

# BUILD_TESTS is shared with the libcmutils submodule. the tests of this
# project are named cmdbm_*, so 'ctest -R cmdbm_' runs only those.
IF ( BUILD_TESTS )
    MESSAGE ( "tests enabled, building test..." )
    ENABLE_TESTING ()
    ADD_SUBDIRECTORY ( test )
ENDIF ()

INSTALL(TARGETS cmdbm cmdbmStatic
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib )
INSTALL(FILES src/libcmdbm.h DESTINATION include)

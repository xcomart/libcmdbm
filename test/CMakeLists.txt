
INCLUDE_DIRECTORIES ( ../src ../libcmutils/src )

# the tests read their mapper and configuration files from 'data',
# relative to the working directory they are run in.
FILE ( COPY data DESTINATION ${CMAKE_CURRENT_BINARY_DIR} )

# every test is named cmdbm_* so that 'ctest -R cmdbm_' selects the tests
# of this project without the ones of the libcmutils submodule.
# the target is prefixed as well: the libcmutils submodule has tests of
# the same name.
FUNCTION ( ADD_CMDBM_TEST NAME )
    ADD_EXECUTABLE ( cmdbm_${NAME} ${NAME}.c mockdb.c )
    TARGET_LINK_LIBRARIES ( cmdbm_${NAME} PRIVATE cmdbm )
    ADD_TEST ( NAME cmdbm_${NAME} COMMAND cmdbm_${NAME}
               WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR} )
    IF ( ${CMAKE_SYSTEM_NAME} STREQUAL "Windows" )
        SET_TESTS_PROPERTIES ( cmdbm_${NAME} PROPERTIES
            ENVIRONMENT_MODIFICATION
            PATH=path_list_append:$<TARGET_FILE_DIR:cmdbm> )
    ENDIF ()
ENDFUNCTION ()

ADD_CMDBM_TEST ( config_test )
ADD_CMDBM_TEST ( mapper_test )
ADD_CMDBM_TEST ( libinit_test )
ADD_CMDBM_TEST ( pool_test )

# the only module which can be exercised without a database server
IF ( SUPPORT_SQLITE )
    ADD_CMDBM_TEST ( sqlite_test )
ENDIF ()

# the modules of the client/server databases need a server to talk to.
# test/docker/compose.yml brings one of each up, test/docker/env.sh points
# the tests at them - without those variables a test exits with 77, which
# ctest reports as a skip instead of a failure.
FUNCTION ( ADD_CMDBM_DBTEST NAME )
    ADD_CMDBM_TEST ( ${NAME} )
    SET_TESTS_PROPERTIES ( cmdbm_${NAME} PROPERTIES SKIP_RETURN_CODE 77 )
ENDFUNCTION ()

# MariaDB and MySQL are one module registered under two keys: the test runs
# against whichever of them this build knows.
IF ( SUPPORT_MARIA OR SUPPORT_MYSQL )
    ADD_CMDBM_DBTEST ( maria_test )
    IF ( SUPPORT_MARIA )
        TARGET_COMPILE_DEFINITIONS ( cmdbm_maria_test
            PRIVATE CMDBM_TEST_DBTYPE="MARIA" )
    ELSE ()
        TARGET_COMPILE_DEFINITIONS ( cmdbm_maria_test
            PRIVATE CMDBM_TEST_DBTYPE="MYSQL" )
    ENDIF ()
ENDIF ()

IF ( SUPPORT_PGSQL )
    ADD_CMDBM_DBTEST ( pgsql_test )
ENDIF ()

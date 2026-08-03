
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

# the only module which can be exercised without a database server
IF ( SUPPORT_SQLITE )
    ADD_CMDBM_TEST ( sqlite_test )
ENDIF ()

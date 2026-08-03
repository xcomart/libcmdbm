#
# API reference for libcmdbm.
#
# Adds a "docs" target when doxygen is available. It is never part of "all":
# the reference is built on request, not on every compile.
#
# The target is named cmdbm_docs, not docs: the libcmutils submodule defines
# a "docs" target of its own and two targets of one name do not configure.
# Same reason the tests of this project are named cmdbm_*.
#

FIND_PACKAGE ( Doxygen )

IF ( NOT DOXYGEN_FOUND )
    MESSAGE ( STATUS "Doxygen not found, the cmdbm docs target is unavailable" )
    RETURN ()
ENDIF ()

# The Doxyfile reads the version out of the environment so that a bare
# "doxygen" run in this directory works too.
ADD_CUSTOM_TARGET ( cmdbm_docs
    COMMAND ${CMAKE_COMMAND} -E env
            LIBCMDBM_VERSION=${LIBCMDBM_VERSION_STRING}
            ${DOXYGEN_EXECUTABLE} Doxyfile
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Generating the API reference into doc/html"
    VERBATIM )

MESSAGE ( STATUS "Doxygen found, \"cmdbm_docs\" target available" )

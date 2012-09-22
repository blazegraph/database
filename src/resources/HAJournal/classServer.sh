#!/bin/bash

# Setup the environment.
source ./HAJournal.env

# Start ClassServer (on one or more machines)
java \
 com.sun.jini.tool.ClassServer\
 -verbose -stoppable -port ${CODEBASE_PORT}\
 -dir ${JINI_LIBDL}

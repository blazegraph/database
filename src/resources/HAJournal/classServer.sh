#!/bin/bash                                                                                                                                    

# Setup the environment.                                                                                                                       
source src/resources/HAJournal/HAJournal.env

# Start ClassServer (on one or more machines)                                                                                                  
java \
 -cp ${CLASSPATH}\
 com.sun.jini.tool.ClassServer\
 -verbose -stoppable -port ${CODEBASE_PORT}\
 -dir ${JINI_LIBDL}
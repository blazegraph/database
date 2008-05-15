# Sets various environment variables.
# 
# Note: Run as "source ./env.sh".
#

export ANT_HOME=/usr/java/apache-ant-1.7.0
export JAVA_OPTS="-server -Xmx2048m -ea"
export JAVA_HOME=/usr/java/jdk1.6.0_03
export NAS=/nas
export PATH=$ANT_HOME/bin:${JAVA_HOME}/bin:${NAS}/scripts

# Note: There is an assumption that you have checked out the code for
# the various bigdata modules into this directory.
#
# Note: You MUST also run the ANT script bigdata-rdf/build.xml to build
# the source.

export SRC=${NAS}/src/

#:${SRC}bigdata/target/classes
#:${SRC}bigdata/target/test-classes
#:${SRC}bigdata-jini/target/classes
#:${SRC}bigdata-jini/target/test-classes
#:${SRC}bigdata-rdf/target/classes
#:${SRC}bigdata-rdf/target/test-classes

export CLASSPATH=\
${SRC}bigdata-rdf.jar\
:${SRC}bigdata/lib/log4j-1.2.8.jar\
:${SRC}bigdata/lib/ctc_utils-5-4-2005.jar\
:${SRC}bigdata/lib/icu/icu4j-3_6.jar\
:${SRC}bigdata/lib/icu/icu4jni.jar\
:${SRC}bigdata/lib/cweb-junit-ext-1.1-b3-dev.jar\
:${SRC}bigdata/lib/lucene/lucene-analyzers-2.2.0.jar\
:${SRC}bigdata/lib/lucene/lucene-core-2.2.0.jar\
:${SRC}bigdata/lib/cweb-extser-0.1-b2-dev.jar\
:${SRC}bigdata/lib/junit-3.8.1.jar\
:${SRC}bigdata/lib/cweb-commons-1.1-b2-dev.jar\
:${SRC}bigdata/lib/unimi/colt-1.2.0.jar\
:${SRC}bigdata/lib/lgpl-utils-1.0-b1-dev.jar\
:${SRC}bigdata/lib/unimi/fastutil-5.1.4.jar\
:${SRC}bigdata-jini/lib/jini/classserver.jar\
:${SRC}bigdata-jini/lib/jini/jini-core.jar\
:${SRC}bigdata-jini/lib/jini/jini-ext.jar\
:${SRC}bigdata-jini/lib/jini/jsk-lib.jar\
:${SRC}bigdata-jini/lib/jini/jsk-platform.jar\
:${SRC}bigdata-jini/lib/jini/jsk-resources.jar\
:${SRC}bigdata-jini/lib/jini/reggie.jar\
:${SRC}bigdata-jini/lib/jini/start.jar\
:${SRC}bigdata-jini/lib/jini/sun-util.jar\
:${SRC}bigdata-jini/lib/jini/tools.jar\
:${SRC}bigdata-rdf/lib/openrdf-sesame-2.0.1-onejar.jar\
:${SRC}bigdata-rdf/lib/slf4j-api-1.4.3.jar\
:${SRC}bigdata-rdf/lib/slf4j-log4j12-1.4.3.jar

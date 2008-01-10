source ./env.sh

# Run a test using main() (vs the junit runner).

# Note: This assumes a maven build, which puts the classes into target/classes.
#
# Note: You must do something like 'mtestui' in order for the test-classes to be built.

# APSTARS synthetic data set.
#export documents_ontology=/metrics/metricsOntology_v1.9.rdfs
#export documents_directory=/metrics/smallDocuments
#export documents_directory=/metrics/documents

# Lehigh synthetic data set.
export documents_ontology=/metrics/lehigh/univ-bench.owl
export documents_directory=/metrics/lehigh/U100

export JOURNAL=/metrics/tmp.jnl

export DATASET="\
 -Ddocuments.directory=${documents_directory}\
 -Ddocuments.ontology=${documents_ontology}\
 -Dfile=${JOURNAL}\
 "

# initialExtent -- also controls the incremental growth rate of the file.
# 100M
#export initialExtent=104857600
# 1G
export initialExtent=1073741824

# 128 is best so far when flush:=false
export branchingFactor=32
#export branchingFactor=64
#export branchingFactor=128
#export branchingFactor=256
#export branchingFactor=4096

# Note: Can take a while to remove the old store file.
echo "Removing old files and $JOURNAL"
rm pid.txt iostat.txt pidstat.txt vmstat.txt nohup.out TestMetrics-metrics.csv ResourceManager.log ${JOURNAL}

# Show disk free before run.
df -H

# Garbage collector options (JRockit).
##export GC="-verbose:gc -XgcReport"
export GC="-verbose:gc -XgcReport -Xgcprio:pausetime"
##export GC="-verbose:gc -XgcReport -Xgcprio:pausetime -Xpausetarget=100ms"
##export GC="-verbose:gc -XgcReport -Xgc:gencon"

# Garbage collector options (Sun).
#export GC="-verbose:gc"
#export GC="-verbose:gc -Xincgc"

# Heap size (2G ram on server).  2G starts to swap. 1G ok.  512M runs very slow.
#export memory="-Xmx2048m"
export memory="-Xmx1024m"
#export memory="-Xmx512m"

# JVM choice.
#export JAVA_HOME=/usr/java/jdk1.6.0_03
#export JAVA_HOME=/usr/java/jrockit-R27.3.0-jdk1.5.0_11
#export JAVA_HOME=/usr/java/jrockit-R27.3.0-jdk1.6.0_01
export JAVA_HOME=/usr/java/jrockit-R27.4.0-jdk1.6.0_02

# And update the PATH.
export PATH=${JAVA_HOME}/bin:${PATH}

# create file so that we can tail it.
#touch nohup.out

nohup java -cp \
:bigdata-rdf/src/resources/logging/log4j.properties\
:bigdata/target/classes\
:bigdata/target/test-classes\
:bigdata/lib/ctc_utils-5-4-2005.jar\
:bigdata/lib/cweb-commons-1.1-b2-dev.jar\
:bigdata/lib/cweb-extser-0.1-b2-dev.jar\
:bigdata/lib/cweb-junit-ext-1.1-b3-dev.jar\
:bigdata/lib/junit-3.8.1.jar\
:bigdata/lib/lgpl-utils-1.0-b1-dev.jar\
:bigdata/lib/log4j-1.2.8.jar\
:bigdata/lib/icu/icu4j-3_6.jar\
:bigdata-rdf/target/classes\
:bigdata-rdf/target/test-classes\
:bigdata-rdf/lib/openrdf-model-1.2.7.jar:\
:bigdata-rdf/lib/openrdf-util-1.2.7.jar:\
:bigdata-rdf/lib/sesame-1.2.7.jar:\
:bigdata-rdf/lib/rio-1.2.7.jar\
 -server ${memory} \
 ${GC}\
 -Dlog4j.configuration=bigdata-rdf/src/resources/logging/log4j.properties\
 ${DATASET}\
 -DtestClass=com.bigdata.rdf.store.TestLocalTripleStore\
 -DdataLoader.closure=None\
 -DdataLoader.commit=None\
 -DrdfsOnly=true\
 -Djustify=false\
 -DdataLoader.flush=false\
 -DmaxFiles=100000\
 -DinitialExtent=${initialExtent}\
 -DbranchingFactor=${branchingFactor}\
 com.bigdata.rdf.metrics.TestMetrics \
 $1 $2 $3 $4 $5 $6 $7 $8 $9 \
 &

# The PID for the java process.
export pid="$!"

echo "pid=$pid"

echo "$pid" > pid.txt

## Run process using nohup.
#nohup -p $pid&

# collect server wide CPU, swap, and block IO information.
vmstat -n 60 > vmstat.txt&

# collect process specific CPU information.
pidstat -u -I -p $pid 60 > pidstat.txt&

# collect process specific memory information trace.
pidstat -r -p $pid 60 > memstat.txt&

# collect IO information (alternative view to vmstat).
iostat 60 > iostat.txt&

# Tail nohup output for the job now running in background.
#tail -f nohup.out&

exit

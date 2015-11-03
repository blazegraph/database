#!/bin/bash

if [ -z "${BLZG_HOME}" ]; then
   # set BLZG_HOME to the top-level of the installation.
   pushd `dirname $0` > /dev/null;cd ..;BLZG_HOME=`pwd`;popd > /dev/null
   echo "BLZG_HOME Not specified: using ${BLZG_HOME}";
fi

export INSTALL_DIR="${BLZG_HOME}"

if [ -z "${DATA_DIR}" ] ; then
	export DATA_DIR=${BLZG_HOME}/data
fi

##
# HAJournalServer configuration parameter overrides (see HAJournal.config).
#
# The bigdata HAJournal.config file may be heavily parameterized through 
# environment variables that get passed through into the JVM started by 
# this script and are thus made available to the HAJournalServer when it
# interprets the contents of the HAJournal.config file. See HAJournal.config
# for the meaning of these environment variables.
#
# Note: Many of these properties have defaults.
##

# Conditional defaults for required properties.  These can (and should) be
# overridden from the environment outside of this script.  The defaults are
# not suitable for deployment.
if [ -z "${FEDNAME}" ]; then
   export FEDNAME=installTest
fi

if [ -z "${LOGICAL_SERVICE_ID}" ]; then
   export LOGICAL_SERVICE_ID=HAJournalServer-1
fi

if [ -z "${FED_DIR}" ]; then
   export FED_DIR=$INSTALL_DIR
fi

if [ -z "${JETTY_PORT}" ]; then
   export JETTY_PORT="9999"
fi

if [ -z "${JETTY_XML}" ]; then
   export JETTY_XML="${BLZG_CONF}/jetty.xml"
fi

if [ -z "${JETTY_RESOURCE_BASE}" ]; then
   export JETTY_RESOURCE_BASE="${BLZG_HOME}/war"
fi

if [ -z "${GROUPS}" ]; then
    export GROUPS="$FEDNAME"
fi

if [ -z "${LOCATORS}" ]; then
   echo "Must specify: LOCATORS"
   exit 1
fi

if [ -z "${ZK_SERVERS}" ]; then
   echo "Must specify and start: ZK_SERVERS"
   exit 1
fi

export HA_OPTS="\
 -DFEDNAME=${FEDNAME}\
 -DLOGICAL_SERVICE_ID=${LOGICAL_SERVICE_ID}\
 -DFED_DIR=${FED_DIR}\
 -DDATA_DIR=${DATA_DIR}\
 -DREPLICATION_FACTOR=${REPLICATION_FACTOR}\
 -DGROUPS=${GROUPS}\
 -DLOCATORS=${LOCATORS}\
 -DZK_SERVERS=${ZK_SERVERS}\
 -DRMI_PORT=${RMI_PORT}\
 -DHA_PORT=${HA_PORT}\
 -DgroupCommit=${GROUP_COMMIT}\
 -DWRITE_CACHE_BUFFER_COUNT=${WRITE_CACHE_BUFFER_COUNT}\
 "-Dcom.bigdata.hostname=${BIGDATA_HOSTNAME}"\
 "-Djetty.port=${JETTY_PORT}"\
 "-Djetty.threads.min=${JETTY_THREADS_MIN}"\
 "-Djetty.threads.max=${JETTY_THREADS_MAX}"\
 "-Djetty.threads.timeout=${JETTY_THREADS_TIMEOUT}"\
 "-Djetty.resourceBase=${JETTY_RESOURCE_BASE}"\
 "-Djetty.home=${JETTY_RESOURCE_BASE}"\
 "-DJETTY_XML=${JETTY_XML}"\
 -DCOLLECT_QUEUE_STATISTICS=${COLLECT_QUEUE_STATISTICS}\
 -DCOLLECT_PLATFORM_STATISTICS=${COLLECT_PLATFORM_STATISTICS}\
 -DGANGLIA_REPORT=${GANGLIA_REPORT}\
 -DGANGLIA_LISTEN=${GANGLIA_LISTEN}\
 -DSYSSTAT_DIR=${SYSSTAT_DIR}\
 -Dcom.bigdata.counters.linux.sysstat.path=${SYSSTAT_DIR}\
 -Dcom.bigdata.rdf.sail.webapp.HALoadBalancerServlet.policy=${LBS_POLICY}\
 -Dcom.bigdata.rdf.sail.webapp.HALoadBalancerServlet.rewriter=${LBS_REWRITER}\
 -Dcom.bigdata.rdf.sail.webapp.lbs.AbstractHostLBSPolicy.hostScoringRule=${LBS_HOST_SCORING_RULE}\
 -Dcom.bigdata.rdf.sail.webapp.lbs.AbstractHostLBSPolicy.localForwardThreshold=${LBS_LOCAL_FORWARD_THRESHOLD}\
 -Dcom.bigdata.rdf.sail.webapp.lbs.AbstractHostLBSPolicy.hostDiscoveryInitialDelay=${LBS_HOST_DISCOVERY_INITIAL_DELAY}\
 -Dcom.bigdata.rdf.sail.webapp.lbs.AbstractHostLBSPolicy.hostDiscoveryDelay=${LBS_HOST_DISCOVERY_DELAY}\
"

##
# ServiceStarter configuration parameters (see startHAServices.conf).
##

if [ -z "${INSTALL_DIR}" ] ; then
	export LIB_DIR=${INSTALL_DIR}/lib
fi

if [ -z "$CONFIG_DIR" ]; then
	export CONFIG_DIR=${INSTALL_DIR}/var/config
fi

if [ -z "$JINI_CLASS_SERVER_PORT" ] ; then
	export JINI_CLASS_SERVER_PORT=8081
fi

if [ -z "$JINI_CONFIG" ] ; then
	export JINI_CONFIG=${CONFIG_DIR}/jini/startHAServices.config
fi

if [ -z "$POLICY_FILE" ] ; then
	export POLICY_FILE=${CONFIG_DIR}/policy/policy.all
fi

if [ -z "$LOGGING_CONFIG" ] ; then
	export LOGGING_CONFIG=${CONFIG_DIR}/logging/logging.properties
fi

if [ -z "$LOG4J_CONFIG" ] ; then
	export LOG4J_CONFIG=${CONFIG_DIR}/logging/log4jHA.properties
fi

# TODO Explicitly enumerate JARs so we can control order if necessary and
# deploy on OS without find and tr.
export HAJOURNAL_CLASSPATH=`find ${LIB_DIR} -name '*.jar' -print0 | tr '\0' ':'`

export JAVA_OPTS="\
 ${JVM_OPTS}\
 ${HA_OPTS}\
 -Djava.security.policy=${POLICY_FILE}\
 -Djava.util.logging.config.file=${LOGGING_CONFIG}\
 -Dlog4j.configuration=${LOG4J_CONFIG}\
 -DLIB_DIR=${INSTALL_DIR}/lib\
 -DLIBDL_DIR=${INSTALL_DIR}/lib\
 -DCONFIG_DIR=${CONFIG_DIR}\
 -DPOLICY_FILE=${POLICY_FILE}\
 -DJINI_CLASS_SERVER_PORT=${JINI_CLASS_SERVER_PORT}\
 -DHAJOURNAL_CLASSPATH=${HAJOURNAL_CLASSPATH}\
"

# Note: To obtain the pid, do: read pid < "$pidFile"

export INSTALL_DIR=${BLZG_HOME}
if [ "${INSTALL_TYPE}" == "BREW" ]; then
   export LIB_DIR=${INSTALL_DIR}/libexec
else
   export LIB_DIR=${INSTALL_DIR}/lib
fi

export JETTY_CLASSPATH=`find ${LIB_DIR} -name '*.jar' -print0 | tr '\0' ':'`


if [ ! -d "${DATA_DIR}" ]; then
   mkdir -p "${DATA_DIR}"/"${FEDNAME}"/"${LOGICAL_SERVICE_ID}"
fi

# -Djetty.home=${JETTY_RESOURCE_BASE}\

cmd="java ${JAVA_OPTS} \
    -cp ${HAJOURNAL_CLASSPATH} \
    com.sun.jini.start.ServiceStarter \
    ${JINI_CONFIG}"

case "$1" in
  start)

      echo "Running: $cmd"
      # Note: This redirects console logger output to dev/null!
      # This is only valid if all logger output is explicitly
      # directed into a file, which it is not when using the
      # default log4j and java.util.logging configuration. I am
      # leaving the brew installer behavior as its historical
      # value to avoid breaking it, but it is very likely to be
      # incorrect.
      if [ "${INSTALL_TYPE}" == "BREW" ]; then
         $cmd >> $BLZG_LOG 2>&1 &
      else
         $cmd >> $BLZG_LOG 2>&1 &
      fi
      pid=$!
      retval=$$
      # echo "PID=$pid"
      echo "$pid">$BLZG_PID
      exit $retval
      ;;
    stop)
#
# Stop the ServiceStarter and all child services.
#
        if [ -f "$BLZG_PID" ]; then
            read pid < "$BLZG_PID"
            pidno=$( ps ax | grep $pid | awk '{ print $1 }' | grep $pid )
            if [ -z "$pidno" ]; then
# The process has died so remove the old pid file.
                echo $"`date` : `hostname` : $pid died?"
                rm -f "$BLZG_PID"
            else
                echo -ne $"`date` : `hostname` : bringing blazegraph service down ... "
                kill $pid
                rm -f "$BLZG_PID"
                echo "done!"
            fi
        fi
        ;;
    status)
#
# Report status for the ServicesManager (up or down).
#
        if [ -f "$BLZG_PID" ]; then
            read pid < "$BLZG_PID"
            pidno=$( ps ax | grep $pid | awk '{ print $1 }' | grep $pid )
            if [ -z "$pidno" ]; then
                echo $"`date` : `hostname` : process died? pid=$pid."
            else
                echo $"`date` : `hostname` : running as $pid."
            fi
        else
            echo $"`date` : `hostname` : not running."
        fi
        ;;
#
# Simply stop then start.
#
    restart)
        $0 stop
        $0 start
        ;;
    *)
 me=`basename $0`
        echo $"Usage: $0 {start|stop|status|restart}"
        exit 1
esac

exit 0

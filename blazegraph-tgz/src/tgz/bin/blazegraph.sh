#!/bin/bash
set -e

if [ -z "${BLZG_HOME}" ]; then
   # set BLZG_HOME to the top-level of the installation.
   pushd `dirname $0` > /dev/null;cd ..;BLZG_HOME=`pwd`;popd > /dev/null
   echo "BLZG_HOME Not specified: using ${BLZG_HOME}";
   export BLZG_HOME
fi

#Check for default configuration file
if [ -f "${BLZG_HOME}/conf/blazegraph" ] ; then
   . "${BLZG_HOME}/conf/blazegraph"
fi

export INSTALL_DIR=${BLZG_HOME}
if [ "${INSTALL_TYPE}" == "BREW" ]; then
   export LIB_DIR=${INSTALL_DIR}/libexec
else
   export LIB_DIR=${INSTALL_DIR}/lib
fi

export JETTY_CLASSPATH=`find ${LIB_DIR} -name 'blazegraph-*.jar' -print0 | tr '\0' ':'`:`find ${LIB_DIR} -name '*.jar' -print0 | tr '\0' ':'`

export DATA_DIR=${BLZG_HOME}/data

if [ ! -d "${DATA_DIR}" ]; then
   mkdir -p "${DATA_DIR}"
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


export JETTY_OPTS="\
 -Djetty.port=${JETTY_PORT}\
 -Djetty.resourceBase=${JETTY_RESOURCE_BASE}\
 -Djetty.home=${JETTY_RESOURCE_BASE}\
 -Djetty.overrideWebXml=${JETTY_RESOURCE_BASE}/WEB-INF/override-web.xml\
 -DJETTY_XML=${JETTY_XML}\
 -Djava.util.logging.config.file=${LOGGING_CONFIG}\
 -Dlog4j.configuration=${LOG4J_CONFIG}\
"

export JAVA_OPTS="\
 ${JVM_OPTS}\
 ${JETTY_OPTS}\
"

cmd="java ${JAVA_OPTS} \
    -cp ${JETTY_CLASSPATH} \
    $NSS \
    -jettyXml ${JETTY_XML} \
    $JETTY_PORT \
    $NSS_NAMESPACE \
    $NSS_PROPERTIES\
"

case "$1" in
  start)

      if [ -f "$BLZG_PID" ]; then
            echo "$BLZG_PID exists...trying to stop."
            $0 stop
      fi

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
      retval=$?
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

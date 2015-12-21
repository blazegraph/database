#!/bin/sh -x
#
# /etc/init.d/blazegraph -- startup script for Blazegraph
#
# chkconfig: 2345 65 25
# description:  Blazegraph High Performance Graph Database
#
# processname: blazegraph
# config: /etc/blazegraph/blazegraph
# pid:  /var/run/blazegraph.pid
#
# Modified from the tomcat7 script
# Written by Miquel van Smoorenburg <miquels@cistron.nl>.
# Modified for Debian GNU/Linux	by Ian Murdock <imurdock@gnu.ai.mit.edu>.
# Modified for Tomcat by Stefan Gybas <sgybas@debian.org>.
# Modified for Tomcat6 by Thierry Carrez <thierry.carrez@ubuntu.com>.
# Modified for Tomcat7 by Ernesto Hernandez-Novich <emhn@itverx.com.ve>.
# Additional improvements by Jason Brittain <jason.brittain@mulesoft.com>.
#
### BEGIN INIT INFO
# Provides:          blazegraph
# Required-Start:    $local_fs $remote_fs $network
# Required-Stop:     $local_fs $remote_fs $network
# Should-Start:      $named
# Should-Stop:       $named
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Start Blazegraph
# Description:       Start the Blazegraph High Performance Database.
### END INIT INFO

# source function library
. /etc/rc.d/init.d/functions

# pull in sysconfig settings
[ -f /etc/sysconfig/blazegraph ] && . /etc/sysconfig/blazegraph


PATH=/bin:/usr/bin:/sbin:/usr/sbin

if [ `id -u` -ne 0 ]; then
	echo "You need root privileges to run this script"
	exit 1
fi
 
# Make sure blazegraph is started with system locale
if [ -r /etc/default/locale ]; then
	. /etc/default/locale
	export LANG
fi


NAME="$(basename $0)"

NAME=blazegraph
DESC="Blazegraph High Performance Database"
DEFAULT=/etc/${NAME}/$NAME
JVM_TMP=/tmp/blazegraph-$NAME-tmp

unset ISBOOT
if [ "${NAME:0:1}" = "S" -o "${NAME:0:1}" = "K" ]; then
    NAME="${NAME:3}"
    ISBOOT="1"
fi

# For SELinux we need to use 'runuser' not 'su'
if [ -x "/sbin/runuser" ]; then
    SU="/sbin/runuser -s /bin/sh"
else
    SU="/bin/su -s /bin/sh"
fi


# The following variables can be overwritten in $DEFAULT

# this is a work-around until there is a suitable runtime replacement 
# for dpkg-architecture for arch:all packages
# this function sets the variable OPENJDKS
find_openjdks()
{
        for jvmdir in /usr/lib/jvm/java-7-openjdk-*
        do
                if [ -d "${jvmdir}" -a "${jvmdir}" != "/usr/lib/jvm/java-7-openjdk-common" ]
                then
                        OPENJDKS=$jvmdir
                fi
        done
        for jvmdir in /usr/lib/jvm/java-6-openjdk-*
        do
                if [ -d "${jvmdir}" -a "${jvmdir}" != "/usr/lib/jvm/java-6-openjdk-common" ]
                then
                        OPENJDKS="${OPENJDKS} ${jvmdir}"
                fi
        done
}

OPENJDKS=""
find_openjdks
# The first existing directory is used for JAVA_HOME (if JAVA_HOME is not
# defined in $DEFAULT)
JDK_DIRS="/usr/lib/jvm/default-java ${OPENJDKS} /usr/lib/jvm/java-6-openjdk /usr/lib/jvm/java-6-sun /usr/lib/jvm/java-7-oracle /usr"

# Look for the right JVM to use
for jdir in $JDK_DIRS; do
    if [ -r "$jdir/bin/java" -a -z "${JAVA_HOME}" ]; then
	JAVA_HOME="$jdir"
    fi
done
export JAVA_HOME

# Directory where the Blazegraph distribution resides
if [ -z "$BLZG_HOME" ] ; then
	BLZG_HOME=/usr/local/$NAME
fi

# Directory for per-instance configuration files and webapps
if [ -z "${BLZG_BASE}" ] ; then
	BLZG_BASE=$BLZG_HOME
fi

# Use the Java security manager? (yes/no)
BLZG_SECURITY=no

# Default Java options
# Set java.awt.headless=true if JAVA_OPTS is not set so the
# Xalan XSL transformer can work without X11 display on JDK 1.4+
# It also looks like the default heap size of 64M is not enough for most cases
# so the maximum heap size is set to 128M
if [ -z "$JAVA_OPTS" ]; then
	JAVA_OPTS="-Djava.awt.headless=true -Xmx128M"
fi

# End of variables that can be overwritten in $DEFAULT

# overwrite settings from default file
if [ -f "$DEFAULT" ]; then
	. "$DEFAULT"
fi

if [ ! -f "$BLZG_HOME/bin/blazegraph.sh" ]; then
	echo "$NAME is not installed"
	exit 1
fi

#POLICY_CACHE="$BLZG_BASE/work/catalina.policy"

if [ -z "$BLZG_TMPDIR" ]; then
	BLZG_TMPDIR="$JVM_TMP"
fi

# Set the JSP compiler if set in the blazegraph.default file
if [ -n "$JSP_COMPILER" ]; then
	JAVA_OPTS="$JAVA_OPTS -Dbuild.compiler=\"$JSP_COMPILER\""
fi

SECURITY=""
if [ "$BLZG_SECURITY" = "yes" ]; then
	SECURITY="-security"
fi

# Define other required variables
BLZG_PID="/var/run/$NAME.pid"
BLZG_SH="$BLZG_HOME/bin/blazegraph.sh"

# Look for Java Secure Sockets Extension (JSSE) JARs
if [ -z "${JSSE_HOME}" -a -r "${JAVA_HOME}/jre/lib/jsse.jar" ]; then
    JSSE_HOME="${JAVA_HOME}/jre/"
fi

blazegraph_sh() {
	# Escape any double quotes in the value of JAVA_OPTS
	JAVA_OPTS="$(echo $JAVA_OPTS | sed 's/\"/\\\"/g')"

	AUTHBIND_COMMAND=""
	if [ "$AUTHBIND" = "yes" -a "$1" = "start" ]; then
		AUTHBIND_COMMAND="/usr/bin/authbind --deep /bin/bash -c "
	fi

#		source \"$DEFAULT\"; \

	# Define the command to run Tomcat's blazegraph.sh as a daemon
	# set -a tells sh to export assigned variables to spawned shells.
	BLZGCMD_SH="set -a; JAVA_HOME=\"$JAVA_HOME\"; \
		source \"$DEFAULT\"; \
		BLZG_HOME=\"$BLZG_HOME\"; \
		BLZG_BASE=\"$BLZG_BASE\"; \
		JAVA_OPTS=\"$JAVA_OPTS\"; \
		BLZG_PID=\"$BLZG_PID\"; \
		BLZG_LOG=\"$BLZG_LOG\"/blazegraph.out; \
		BLZG_TMPDIR=\"$BLZG_TMPDIR\"; \
		LANG=\"$LANG\"; JSSE_HOME=\"$JSSE_HOME\"; \
		cd \"$BLZG_BASE\"; \
		\"$BLZG_SH\" $@"

	echo "$BLZGCMD" 

	if [ "$AUTHBIND" = "yes" -a "$1" = "start" ]; then
		BLZGCMD_SH="'$BLZGCMD_SH'"
	fi

	# Run the blazegraph.sh script as a daemon
	set +e
	touch "$BLZG_PID" "$BLZG_LOG"/blazegraph.out
	chown $BLZG_USER "$BLZG_PID" "$BLZG_LOG"/blazegraph.out
	#start-stop-daemon --start -b -u "$BLZG_USER" -g "$BLZG_GROUP" \
	#	-c "$BLZG_USER" -d "$BLZG_TMPDIR" -p "$BLZG_PID" \
	#	-x /bin/bash -- -c "$AUTHBIND_COMMAND $BLZGCMD_SH"
	$SU - $BLZG_USER -c "$BLZGCMD_SH start"
	status="$?"
	set +a -e
	return $status
}

case "$1" in
  start)
	if [ -z "$JAVA_HOME" ]; then
		echo "no JDK or JRE found - please set JAVA_HOME"
		exit 1
	fi

	if [ ! -d "$BLZG_BASE/conf" ]; then
		echo "invalid BLZG_BASE: $BLZG_BASE"
		exit 1
	fi

	echo "Starting $DESC" "$NAME"
	#if start-stop-daemon --test --start --pidfile "$BLZG_PID" \
	#	--user $BLZG_USER --exec "$JAVA_HOME/bin/java" \
	#	>/dev/null; then

		# Regenerate POLICY_CACHE file
	#	umask 022
	#	echo "// AUTO-GENERATED FILE from /etc/blazegraph/policy.d/" \
	#		> "$POLICY_CACHE"
	#	echo ""  >> "$POLICY_CACHE"
	#	cat $BLZG_BASE/conf/policy.d/*.policy \
	#		>> "$POLICY_CACHE"

		# Remove / recreate JVM_TMP directory
		rm -rf "$JVM_TMP"
		mkdir -p "$JVM_TMP" || {
			echo "could not create JVM temporary directory"
			exit 1
		}
		chown $BLZG_USER "$JVM_TMP"

		blazegraph_sh start 
		sleep 5
	;;
  stop)
	echo "Stopping $DESC" "$NAME"

	set +e
	if [ -f "$BLZG_PID" ]; then 
		start-stop-daemon --stop --pidfile "$BLZG_PID" \
			--user "$BLZG_USER" \
			--retry=TERM/20/KILL/5 >/dev/null
		if [ $? -eq 1 ]; then
			echo "$DESC is not running but pid file exists, cleaning up"
		elif [ $? -eq 3 ]; then
			PID="`cat $BLZG_PID`"
			echo "Failed to stop $NAME (pid $PID)"
			exit 1
		fi
		rm -f "$BLZG_PID"
		rm -rf "$JVM_TMP"
	else
		echo "(not running)"
	fi
	echo 0
	set -e
	;;
   status)
	set +e
	start-stop-daemon --test --start --pidfile "$BLZG_PID" \
		--user $BLZG_USER --exec "$JAVA_HOME/bin/java" \
		>/dev/null 2>&1
	if [ "$?" = "0" ]; then

		if [ -f "$BLZG_PID" ]; then
		    echo "$DESC is not running, but pid file exists."
			exit 1
		else
		    echo "$DESC is not running."
			exit 3
		fi
	else
		echo "$DESC is running with pid `cat $BLZG_PID`"
	fi
	set -e
        ;;
  restart|force-reload)
	if [ -f "$BLZG_PID" ]; then
		$0 stop
		sleep 1
	fi
	$0 start
	;;
  try-restart)
        if start-stop-daemon --test --start --pidfile "$BLZG_PID" \
		--user $BLZG_USER --exec "$JAVA_HOME/bin/java" \
		>/dev/null; then
		$0 start
	fi
        ;;
  *)
	echo "Usage: $0 {start|stop|restart|try-restart|force-reload|status}"
	exit 1
	;;
esac

exit 0

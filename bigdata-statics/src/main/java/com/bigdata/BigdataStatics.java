/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Sep 10, 2009
 */

package com.bigdata;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

/**
 * A class for those few statics that it makes sense to reference from other
 * places.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class BigdataStatics {

    /**
     * A flag used for a variety of purposes during performance tuning. The use
     * of the flag makes it easier to figure out where those {@link System#err}
     * messages are coming from. This should always be off in the trunk.
     */
    public static final boolean debug = Boolean.getBoolean("com.bigdata.debug");

    /**
     * The name of an environment variable whose value will be used as the
     * canonical host name for the host running this JVM. This information is
     * used by the {@link com.bigdata.counters.AbstractStatisticsCollector},
     * which is responsible for obtaining and reporting the canonical hostname
     * for the {@link com.bigdata.Banner} and other purposes.
     * 
     * @see com.bigdata.counters.AbstractStatisticsCollector
     * @see com.bigdata.Banner
     * @see com.bigdata.ganglia.GangliaService#HOSTNAME
     * @see <a href="http://trac.blazegraph.com/ticket/886" >Provide workaround for
     *      bad reverse DNS setups</a>
     */
    public static final String HOSTNAME = "com.bigdata.hostname";

    /**
     * The #of lines of output from a child process which will be echoed onto
     * {@link System#out} when that child process is executed. This makes it
     * easy to track down why a child process dies during service start. If you
     * want to see all output from the child process, then you should set the
     * log level for the {@link com.bigdata.jini.start.process.ProcessHelper}
     * class to INFO.
     * <p>
     * Note: This needs to be more than the length of the
     * {@link com.bigdata.Banner} output in order for anything related to the
     * process behavior to be echoed on {@link System#out}.
     * 
     * @see com.bigdata.jini.start.process.ProcessHelper
     */
    public static int echoProcessStartupLineCount = 30;//Integer.MAX_VALUE;//100

    /**
     * Global switch controlling whether true thread local buffers or striped
     * locks are used for some things.
     * 
     * @deprecated This is ugly. Remove once the issue has been resolved.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/437 (Thread-local
     *      cache combined with unbounded thread pools causes effective memory
     *      leak)
     */
    public static final boolean threadLocalBuffers = Boolean
            .getBoolean("com.bigdata.threadLocalBuffers");

    /**
     * Used to ignore tests in CI that are known to fail. This helps make CI
     * green for people while still leaving us a trail for the tests that exist
     * to mark problems that should be fixed at some point.
     */
    public static final boolean runKnownBadTests = Boolean
            .getBoolean("com.bigdata.runKnownBadTests");

    /**
     * Return the web application context path for the default deployment of the
     * bigdata web application.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/730" >
     *      Allow configuration of embedded NSS jetty server using jetty-web.xml
     *      </a>
     */
    public static final String getContextPath() {

        return "/bigdata";
        
    }
    
    /**
	 * Write a thread dump onto the caller's object.
	 * <p>
	 * Note: This code should not obtain any locks. This is necessary in order
	 * for the code to run even when the server is in a deadlock.
	 * 
	 * @see <a href="http://trac.blazegraph.com/ticket/1082" > Add ability to dump
	 *      threads to status page </a>
	 */
	public static void threadDump(final Appendable w) {

		try {

			final DateFormat df = DateFormat.getDateTimeInstance();
			final Date date = new Date(System.currentTimeMillis());

			w.append(Banner.getBanner());
			w.append("Thread dump. Date:" + df.format(date));
			w.append("\n\n");

			// Setup an ordered map.
			final Map<Thread, StackTraceElement[]> dump = new TreeMap<Thread, StackTraceElement[]>(
					new Comparator<Thread>() {
						@Override
						public int compare(Thread o1, Thread o2) {
							return Long.compare(o1.getId(), o2.getId());
						}
					});

			// Add the stack trace for each thread.
			dump.putAll(Thread.getAllStackTraces());

			for (Map.Entry<Thread, StackTraceElement[]> threadEntry : dump
					.entrySet()) {

				final Thread thread = threadEntry.getKey();

				w.append("THREAD#" + thread.getId() + ", name="
						+ thread.getName() + ", state=" + thread.getState()
						+ ", priority=" + thread.getPriority() + ", daemon="
						+ thread.isDaemon() + "\n");

				for (StackTraceElement elem : threadEntry.getValue()) {

					w.append("\t" + elem.toString() + "\n");

				}

			}

		} catch (IOException ex) {
			
			throw new RuntimeException(ex);
			
		}
		
	}

}

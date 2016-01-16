/**

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
package com.bigdata.journal;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.ICounterSet;

/**
 * {@link IPlugin} for collecting statistics from the operating system.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class PlatformStatsPlugIn implements IPlugIn<Journal, AbstractStatisticsCollector> {

    private static final Logger log = Logger.getLogger(PlatformStatsPlugIn.class);

    public interface Options extends AbstractStatisticsCollector.Options {
        
        /**
         * Boolean option for the collection of statistics from the underlying
         * operating system (default
         * {@value #DEFAULT_COLLECT_PLATFORM_STATISTICS}).
         * 
         * @see AbstractStatisticsCollector#newInstance(Properties)
         */
        String COLLECT_PLATFORM_STATISTICS = Journal.class.getName()
                + ".collectPlatformStatistics";

        String DEFAULT_COLLECT_PLATFORM_STATISTICS = "false"; 

    }
    
    /**
     * Collects interesting statistics on the host and process.
     * 
     * @see Options#COLLECT_PLATFORM_STATISTICS
     */
    private AbstractStatisticsCollector platformStatisticsCollector = null;

    /**
     * {@inheritDoc}
     * <p>
     * Start collecting performance counters from the OS (if enabled).
     */
    @Override
    public void startService(final Journal indexManager) {
        
        final boolean collectPlatformStatistics = Boolean
                .valueOf(indexManager.getProperty(Options.COLLECT_PLATFORM_STATISTICS,
                        Options.DEFAULT_COLLECT_PLATFORM_STATISTICS));

        if (log.isInfoEnabled())
            log.info(Options.COLLECT_PLATFORM_STATISTICS + "="
                    + collectPlatformStatistics);

        if (!collectPlatformStatistics) {

            return;

        }

        final Properties p = indexManager.getProperties();

        if (p.getProperty(AbstractStatisticsCollector.Options.PROCESS_NAME) == null) {

            // Set default name for this process.
            p.setProperty(AbstractStatisticsCollector.Options.PROCESS_NAME,
                    "service" + ICounterSet.pathSeparator
                            + Journal.class.getName());

        }

        try {

            final AbstractStatisticsCollector tmp = AbstractStatisticsCollector
                    .newInstance(p);

            tmp.start();

            synchronized(this) {
                
                platformStatisticsCollector = tmp;
                
            }
            
            if (log.isInfoEnabled())
                log.info("Collecting platform statistics.");

        } catch (Throwable t) {

            log.error(t, t);
            
        }
   
    }

    /**
     * {@inheritDoc}
     * <p>
     * NOP - Collection is on the JVM and the OS. If it is shutdown with each
     * {@link Journal} shutdown, then collection would wind up disabled with the
     * first {@link Journal} shutdown even if there is more than one open
     * {@link Journal}.
     */
    @Override
    public void stopService(boolean immediateShutdown) {

        // NOP
        
    }

    @Override
    public AbstractStatisticsCollector getService() {

        synchronized(this) {

            return platformStatisticsCollector;
            
        }
        
    }

    @Override
    public boolean isRunning() {

        synchronized(this) {

            return platformStatisticsCollector != null;
            
        }

    }

}

package com.bigdata.counters.win;

import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CounterSet;

/**
 * Collects per-host performance counters on a Windows platform.
 * <p>
 * Note: The concept of a PID exists for Windows, but it is not relevant to
 * collection of performance counters since Windows does not appear to support
 * collection by PID, only by the process name. Since we run multiple services
 * per host, collection by process name is not sufficient to facilitate decision
 * making by the load balancer. Instead, the load balancer relies on service
 * specific performance counters, including the average queue length and
 * response time.
 * 
 * @see http://technet2.microsoft.com/windowsserver/en/library/45b6fbfc-9fcd-4e58-b070-1ace9ca93f2e1033.mspx?mfr=true,
 *      for a description of the <code>typeperf</code> command.
 * 
 * @see http://www.microsoft.com.nsatc.net/technet/archive/winntas/maintain/monitor/perform.mspx?mfr=true,
 *      for a list of counters and their descriptions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StatisticsCollectorForWindows extends AbstractStatisticsCollector {

    /**
     * 
     * @param interval
     *            Reporting interval in seconds.
     */
    public StatisticsCollectorForWindows(int interval) {

        super(interval);

        typeperf = new TypeperfCollector(interval);
        
    }

    /** reports on host performance counters (CPU, MEM, IO). */
    final protected TypeperfCollector typeperf;

    public void start() {

        log.info("starting collectors");
        
        super.start();
        
        typeperf.start();

    }

    public void stop() {

        log.info("stopping collectors");
        
        super.stop();
        
        typeperf.stop();

    }

    private boolean countersAdded = false;
    
    synchronized public CounterSet getCounters() {
        
        CounterSet root = super.getCounters();
        
        if( ! countersAdded ) {

            /*
             * These are per-host counters. We attach them under the fully
             * qualified hostname.
             */
            root.makePath(fullyQualifiedHostName).attach( typeperf.getCounters() );
         
            countersAdded = true;
            
        }
        
        return root;
        
    }

}

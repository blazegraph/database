package com.bigdata.counters.linux;

import java.util.UUID;

import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CounterSet;

/**
 * Collection of host performance data using the <code>sysstat</code> suite.
 * 
 * @see http://pagesperso-orange.fr/sebastien.godard/
 * 
 * @todo configuration parameters to locate the sysstat utilities (normally
 *       installed into /usr/bin).
 * 
 * <pre>
 *   vmstat
 *   procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu------
 *    r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 *    0  0  19088 1099996 210388 2130812    0    0    26    17    0    0  5  2 92  1  0
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StatisticsCollectorForLinux extends AbstractStatisticsCollector {

    /**
     * The process identifier for this process (the JVM).
     */
    static protected int pid;
    static {

        pid = PIDUtil.getLinuxPIDWithBash();

    }
    
    /**
     * The Linux {@link KernelVersion}.
     */
    static protected KernelVersion kernelVersion;
    static {

        kernelVersion = KernelVersion.get();

    }

    /**
     * The name of the process (or more typically its service {@link UUID})
     * whose per-process performance counters are to be collected.
     */
    protected final String processName;

    /** reports on the host CPU utilization. */
    protected SarCpuUtilizationCollector sar1;

    /** reports on process performance counters (CPU, MEM, IO). */
    protected PIDStatCollector pidstat;

    public void start() {

        log.info("starting collectors");
        
        super.start();
        
        sar1.start();

        pidstat.start();

    }

    public void stop() {

        log.info("stopping collectors");
        
        super.stop();
        
        sar1.stop();

        pidstat.stop();

    }
    
    private boolean countersAdded = false;
    
    synchronized public CounterSet getCounters() {
        
        CounterSet root = super.getCounters();
        
        if( ! countersAdded ) {

            /*
             * These are per-host counters. We attach them under the fully
             * qualified hostname.
             */
            root.makePath(fullyQualifiedHostName).attach( sar1.getCounters() );

            /*
             * These are per-process counters. We attach them under a path
             * described by the fully qualified hostname followed by the process
             * name.
             */
            root.makePath(fullyQualifiedHostName+ps+processName).attach( pidstat.getCounters() );
            
            countersAdded = true;
            
        }
        
        return root;
        
    }
    
    /**
     * 
     * @param interval
     *            The interval at which the performance counters will be
     *            collected in seconds.
     * @param processName
     *            The name of the process (or more typically its service
     *            {@link UUID}) whose per-process performance counters are to
     *            be collected.
     */
    public StatisticsCollectorForLinux(int interval, String processName) {

        super(interval);

        if (processName == null)
            throw new IllegalArgumentException();
        
        this.processName = processName;
        
        // host wide collection
        sar1 = new SarCpuUtilizationCollector(interval,kernelVersion);

        // process specific collection.
        pidstat = new PIDStatCollector(pid, interval, kernelVersion);

    }

}

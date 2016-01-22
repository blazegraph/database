package com.bigdata.counters.osx;

import java.util.UUID;

import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.IHostCounters;
import com.bigdata.counters.IRequiredHostCounters;
import com.bigdata.counters.PIDUtil;

/**
 * Collection of host performance data using OSX native utilities (curent
 * vm_stat and iostat).
 * <p>
 * Note: Unfortunately, the OSX command line reporting utilities do NOT provide
 * access to IO Wait and do NOT break down IO into reads and writes. This means
 * that we can not provide reporting for the following counters:
 * <ul>
 * <li> {@link IRequiredHostCounters#PhysicalDisk_BytesReadPerSec}</li>
 * <li> {@link IRequiredHostCounters#PhysicalDisk_BytesWrittenPerSec}</li>
 * <li> {@link IHostCounters#CPU_PercentIOWait}</li>
 * </ul>
 * However, we are collecting the following which may be substituted to some
 * extent:
 * <ul>
 * <li> {@link IHostCounters#PhysicalDisk_BytesPerSec}</li>
 * <li> {@link IHostCounters#PhysicalDisk_TransfersPerSec}</li>
 * </ul>
 * However, there is no substitute for IO Wait.
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/225
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: StatisticsCollectorForLinux.java 4290 2011-03-11 15:12:26Z
 *          thompsonbry $
 */
public class StatisticsCollectorForOSX extends AbstractStatisticsCollector {

    /**
     * The process identifier for this process (the JVM).
     */
    static protected int pid;
    static {

        pid = PIDUtil.getLinuxPIDWithBash();

    }
    
//    /**
//     * The Linux {@link KernelVersion}.
//     */
//    static protected KernelVersion kernelVersion;
//    static {
//
//        kernelVersion = KernelVersion.get();
//
//    }

//    /**
//     * The name of the process (or more typically its service {@link UUID})
//     * whose per-process performance counters are to be collected.
//     */
//    private final String processName;

	/**
	 * Reports on the host disk and CPU utilization (but not IOWait and does not
	 * break out disk read versus write activity).
	 */
    private IOStatCollector iostat;

	/**
	 * <code>vm_stat</code> reports on the host page faults, buffers, etc., but
	 * it does NOT report on either CPU stats or IO Wait.
	 */
    private VMStatCollector vmstat;

	/**
	 * TODO Under OSX, <code>top</code> provides more full featured reporting of
	 * OS performance counters, including some IO counters and process specific
	 * counters. Unfortunately, it does NOT provide IO Wait and it does NOT
	 * break out IO by reads and writes. It is also more work to parse its
	 * output.
	 * <p>
	 * <code>-l 0</code> turns on "logging" mode. The ZERO (0) indicates
	 * infinite samples, rather than some finite number of samples.
	 * <p>
	 * <code>-s [delay]</code> sets the delay between reports.
	 * <p>
	 * <code>-S</code> adds reporting on swap space into the headers.
	 * 
	 * <pre>
	 * Swap: 545M + 479M free.
	 * Purgeable: 31M 21336(0) pages purged.
	 * </pre>
	 * 
	 * Here is an example where the pid is NOT specified.
	 * 
	 * <pre>
	 * top -l 0 -s 60 -S
	 * Processes: 107 total, 3 running, 104 sleeping, 587 threads 
	 * 2011/07/13 20:17:36
	 * Load Avg: 2.24, 1.77, 1.53 
	 * CPU usage: 63.15% user, 36.84% sys, 0.0% idle 
	 * SharedLibs: 5292K resident, 6092K data, 0B linkedit.
	 * MemRegions: 18245 total, 4813M resident, 30M private, 429M shared.
	 * PhysMem: 1027M wired, 5472M active, 671M inactive, 7171M used, 1019M free.
	 * VM: 263G vsize, 1042M framework vsize, 2911706(0) pageins, 2092534(0) pageouts.
	 * Swap: 545M + 479M free.
	 * Purgeable: 31M 21336(0) pages purged.
	 * Networks: packets: 20768691/24G in, 22255115/23G out.
	 * Disks: 3335765/133G read, 8511536/2051G written.
	 * 
	 * PID    COMMAND          %CPU TIME     #TH #WQ #PORTS #MREGS RPRVT  RSHRD RSIZE  VPRVT VSIZE  PGRP  PPID STATE    UID FAULTS     COW   MSGSENT   MSGRECV  SYSBSD    SYSMACH   CSW       PAGEINS USER      
	 * 94188  mdworker         0.0  00:06.04 3   1   48+    60+    836K+  16M+  2488K+ 31M+  2408M+ 94188 1    sleeping 89  32402+     147+  115066+   37438+   263505+   79593+    55853+    42+     _spotlight
	 * 79486  Image Capture Ex 0.0  00:01.19 3   1   100+   73+    3052K+ 17M+  7004K+ 40M+  2672M+ 79486 865  sleeping 501 11662+     174+  126963+   63971+   26666+    71795+    16995+    55+     bryan     
	 * 79438  Preview          0.0  00:31.59 3   1   207+   453+   30M+   67M+  65M+   77M+  2851M+ 79438 865  sleeping 501 96609+     1466+ 330566+   162122+  709661+   553728+   129001+   801+    bryan     
	 * 79114  TextEdit         0.0  00:04.80 3   2   121+   193+   6616K+ 28M+  18M+   44M+  2751M+ 79114 865  sleeping 501 24588+     870+  211100+   104015+  52253+    142782+   43400+    238+    bryan     
	 * 66520- Skype            0.0  89:18.16 21  2   538+   658+   161M+  78M+  208M+  270M+ 1266M+ 66520 865  sleeping 501 115403971+ 2498+ 12493857+ 6119421+ 10995806+ 28285072+ 17521056+ 3683+   bryan
	 * </pre>
	 * 
	 * Top can be used to go after per-process statistics by specifying
	 * <code>-pid [pid]</code>. It is also possible to request all processes
	 * owned by a user with <code>-user [user]</code>. Here is an example where
	 * the pid is specified.
	 * 
	 * <pre>
	 * top -l 0 -s 60 -S -pid 26866
	 * Processes: 107 total, 3 running, 104 sleeping, 649 threads 
	 * 2011/07/13 20:19:29
	 * Load Avg: 1.14, 1.49, 1.45 
	 * CPU usage: 56.25% user, 43.75% sys, 0.0% idle 
	 * SharedLibs: 5292K resident, 6060K data, 0B linkedit.
	 * MemRegions: 18439 total, 4806M resident, 30M private, 404M shared.
	 * PhysMem: 969M wired, 4768M active, 1366M inactive, 7102M used, 1089M free.
	 * VM: 263G vsize, 1042M framework vsize, 2911717(0) pageins, 2092534(0) pageouts.
	 * Swap: 545M + 479M free.
	 * Purgeable: 380K 22126(0) pages purged.
	 * Networks: packets: 20876610/24G in, 22362893/23G out.
	 * Disks: 3336157/133G read, 8512107/2051G written.
	 * 
	 * PID    COMMAND %CPU TIME     #TH   #WQ #PORTS #MREGS RPRVT  RSHRD  RSIZE  VPRVT  VSIZE  PGRP  PPID  STATE   UID FAULTS   COW   MSGSENT  MSGRECV SYSBSD   SYSMACH   CSW       PAGEINS USER 
	 * 26866  java    0.0  20:29.90 137/1 1   5890+  703+   2823M+ 7944K+ 2863M+ 3281M+ 7450M+ 21051 21052 running 501 1975814+ 2352+ 9530469+ 138939+ 5625664+ 25326440+ 10828748+ 8030+   bryan
	 * </pre>
	 * 
	 */
//    private TOPCollector pidstat;

    public void start() {
        
        if (log.isInfoEnabled())
            log.info("starting collectors");
        
        super.start();
        
        if (iostat != null)
			try {
				iostat.start();
			} catch (Throwable t) {
				log.error(t, t);
			}

        if (vmstat != null)
			try {
				vmstat.start();
			} catch (Throwable t) {
				log.error(t, t);
			}
        
//        if (pidstat != null)
//			try {
//				pidstat.start();
//			} catch (Throwable t) {
//				log.error(t, t);
//			}

    }

    public void stop() {

        if (log.isInfoEnabled())
            log.info("stopping collectors");
        
        super.stop();

        if (iostat != null)
			try {
				iostat.stop();
			} catch (Throwable t) {
				log.error(t, t);
			}

        if (vmstat != null)
			try {
				vmstat.stop();
			} catch (Throwable t) {
				log.error(t, t);
			}

//        if (pidstat != null)
//			try {
//				pidstat.stop();
//			} catch (Throwable t) {
//				log.error(t, t);
//			}

    }
    
    @Override
    public CounterSet getCounters() {
        
		final CounterSet root = super.getCounters();

		if (iostat != null) {

			/*
			 * These are per-host counters. We attach them under the fully
			 * qualified hostname.
			 */

			root.makePath(fullyQualifiedHostName).attach(iostat.getCounters());

		}

		if (vmstat != null) {

			/*
			 * These are per-host counters. We attach them under the fully
			 * qualified hostname.
			 */

			root.makePath(fullyQualifiedHostName).attach(vmstat.getCounters());

		}
            
//            if (pidstat != null) {
//            
//                /*
//                 * These are per-process counters. We attach them under a path
//                 * described by the fully qualified hostname followed by the process
//                 * name.
//                 */
//
//                root.makePath(fullyQualifiedHostName + ps + processName)
//                        .attach(pidstat.getCounters());
//
//            }
        
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
    public StatisticsCollectorForOSX(final int interval,
            final String processName) {

        super(interval, processName);

//        if (processName == null)
//            throw new IllegalArgumentException();
//        
//        this.processName = processName;
        
		// host wide collection
		iostat = new IOStatCollector(interval, true/* cpuStats */);
        
        // Enable supported collectors.
        vmstat = new VMStatCollector(interval);
        
//        // process specific collection.
//        pidstat = new PIDStatCollector(pid, interval, kernelVersion);

    }

}

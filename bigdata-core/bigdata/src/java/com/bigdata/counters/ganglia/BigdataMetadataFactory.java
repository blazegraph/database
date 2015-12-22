/*
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.counters.ganglia;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


import com.bigdata.counters.ICounter;
import com.bigdata.counters.IHostCounters;
import com.bigdata.counters.IProcessCounters;
import com.bigdata.ganglia.GangliaCoreMetricDecls;
import com.bigdata.ganglia.GangliaMessageTypeEnum;
import com.bigdata.ganglia.GangliaMetadataMessage;
import com.bigdata.ganglia.GangliaMunge;
import com.bigdata.ganglia.GangliaSlopeEnum;
import com.bigdata.ganglia.IGangliaMetadataFactory;
import com.bigdata.ganglia.IGangliaMetadataMessage;

/**
 * A factory which integrates the bigdata hierarchical counter set model with
 * the ganglia metric model.
 * <p>
 * The bigdata counter set model is hierarchical and collects both per-host and
 * per-service metrics. This is illustrated below.
 * 
 * <pre>
 * /192.168.1.10/CPU/% IO Wait=0.0
 * /192.168.1.10/CPU/% Processor Time=0.24
 * /192.168.1.10/CPU/% System Time=0.08
 * /192.168.1.10/CPU/% User Time=0.16
 * /192.168.1.10/Info/Architecture=amd64
 * /192.168.1.10/Info/Number of Processors=4
 * /192.168.1.10/Info/Operating System Name=Linux
 * /192.168.1.10/Info/Operating System Version=2.6.38-11-server
 * /192.168.1.10/Info/Processor Info=Intel(R) Core(TM) i7-2620M CPU @ 2.70GHz Family 6 Model 42 Stepping 7, GenuineIntel
 * /192.168.1.10/Memory/Bytes Free=5.031514112E9
 * /192.168.1.10/Memory/Major Page Faults Per Second=0.0
 * /192.168.1.10/Memory/Swap Bytes Used=0.0
 * /192.168.1.10/PhysicalDisk/Bytes Read Per Second=0.0
 * /192.168.1.10/PhysicalDisk/Bytes Written Per Second=6144.0
 * /192.168.1.10/test-context-7/CPU/% Processor Time=0.0
 * /192.168.1.10/test-context-7/CPU/% System Time=0.0
 * /192.168.1.10/test-context-7/CPU/% User Time=0.0020
 * /192.168.1.10/test-context-7/Memory/Major Faults per Second=0.0
 * /192.168.1.10/test-context-7/Memory/Minor Faults per Second=0.0
 * /192.168.1.10/test-context-7/Memory/Percent Memory Size=0.0091
 * /192.168.1.10/test-context-7/Memory/Resident Set Size=76304384
 * /192.168.1.10/test-context-7/Memory/Virtual Size=2449391616
 * /192.168.1.10/test-context-7/PhysicalDisk/Bytes Read per Second=0.0
 * /192.168.1.10/test-context-7/PhysicalDisk/Bytes Written per Second=0.0
 * </pre>
 * 
 * In order to map this information onto ganglia, the host and service names
 * must be identified in the counter path. This allows us to recognize the local
 * name of the metric, which may itself include some path components. For
 * example, <code>Memory/Resident Set Size</code> is the local name of the
 * metric whose value is the resident set size of the service process. Some
 * metrics appear in both the per-host and per-service sections of the counter
 * set hierarchy. For example, <code>CPU/% Processor Time</code> is reported
 * both for the host and for the service (when using <code>pidstat</code> to
 * monitor the service).
 * <p>
 * For the per-host metrics, bigdata and ganglia sometimes collect what is
 * essentially the same metric. For example, <code>CPU/% User Time</code> is
 * essentially the same as <code>cpu_user</code>. However, sometimes the units
 * in which the metric is collected differ. For example, bigdata collects
 * <code>Memory/Bytes Free</code> in <code>byte</code>s using a
 * <code>double</code> counter while ganglia collects <code>mem_free</code> in
 * <code>KB</code>s using a <code>float</code> metric. In these cases, we need
 * to scale the metric and also change the name of the metric in order to report
 * it as if it were a core ganglia metric.
 * <p>
 * Another difference is that bigdata tends to put the units into the name of
 * the metric while ganglia has explicit metadata about the units. For metrics
 * such as <code>CPU/% System Time</code> we need to register the appropriate
 * units (note that ganglia also can not handle the <code>%</code> in the name
 * of the metric).
 * 
 * TODO Service name, service iface, service UUID.
 * 
 * TODO Right now metrics which we are not aligning with Ganglia are being
 * reported out in other top-level groups (Bytes Free and Swap Bytes Used are
 * two good examples). [Bytes Free has been fixed.]
 * 
 * TODO Rather than munging a "%" to a "Percent", it would be better to rename
 * the metric and specify "%" as the units.
 */
public class BigdataMetadataFactory extends GangliaCoreMetricDecls implements
		IGangliaMetadataFactory {

	protected final String serviceName;

	/**
	 * Maps the local name of a host counter onto the metric declaration.
	 * <p>
	 * Note: The local name is not just {@link ICounter#getName()}. It is
	 * everything after the host component or (for a service counter) the host
	 * plus the service component.
	 */
	private final Map<String/* localName */, IGangliaMetadataMessage/* decl */> hostDecls;
	
	/**
	 * Maps the local name of a service counter onto the metric declaration.
	 * <p>
	 * Note: The local name is not just {@link ICounter#getName()}. It is
	 * everything after the host component or (for a service counter) the host
	 * plus the service component.
	 */
	private final Map<String/* localName */, IGangliaMetadataMessage/* decl */> serviceDecls;
	
	/**
	 * Map for resolving metric names to rich {@link IGangliaMetadataMessage}
	 * implementations which allow for value translation, etc.
	 */
	private final Map<String/* localName */, IGangliaMetadataMessage/* decl */> resolveDecls;

	/**
	 * Register a per-host metric.
	 * 
	 * @param metricName
	 *            The name of the metric.
	 * @param decl
	 *            The declaration for the metric.
	 */
	protected void addHostCounter(final String metricName,
			final IGangliaMetadataMessage decl) {

		addHostCounter(metricName, decl, false/* prefer */);

	}

	protected void addHostCounter(final String metricName,
			final IGangliaMetadataMessage decl, boolean prefer) {

		if (prefer)
			resolveDecls.put(decl.getMetricName(), decl);

		hostDecls.put(GangliaMunge.munge(metricName.replace('/', '.')), decl);

	}

	/**
	 * Register a per-service metric.
	 * 
	 * @param metricName
	 *            The name of the metric.
	 * @param decl
	 *            The declaration for the metric.
	 */
	protected void addServiceCounter(final String metricName,
			final IGangliaMetadataMessage decl) {

		addServiceCounter(metricName, decl, false/* prefer */);

	}

	protected void addServiceCounter(final String metricName,
			final IGangliaMetadataMessage decl, final boolean prefer) {

		if (prefer)
			resolveDecls.put(decl.getMetricName(), decl);

		serviceDecls.put(GangliaMunge.munge(metricName.replace('/', '.')), decl);

	}
	
	/**
	 * {@inheritDoc}
	 * <p>
	 * Attempts to resolve a host counter and then a service counter based on
	 * the localName.
	 * 
	 * @return The declaration if one exists for that localName.
	 */
	@Override
	public IGangliaMetadataMessage newDecl(final String hostName,
			final String metricName, final Object value) {

		IGangliaMetadataMessage decl = null;

		// FIXME Base class does not implement interface yet.
//		if ((decl = super.newDecl(hostName, metricName, value)) != null) {
//
//			return decl;
//
//		}
		
		if((decl = hostDecls.get(metricName))!=null) {
			
			return decl;
			
		}

		if((decl = serviceDecls.get(metricName))!=null) {
			
			return decl;

		}

		return decl;
		
	}

	/**
	 * If this class has a replacement declaration for the metric, then it is
	 * returned. Otherwise the caller's argument is returned.
	 * 
	 * TODO We could actually meld the data generating a new declaration having
	 * the desired behavior but the same data as the caller's argument. That
	 * would require a new constructor (or factory method) on a rich metadata
	 * class.
	 */
	@Override
	public IGangliaMetadataMessage resolve(final IGangliaMetadataMessage decl) {

		final IGangliaMetadataMessage newDecl = resolveDecls.get(decl);
		
		if(newDecl != null) {
			
			return newDecl;
			
		}

		return decl;
		
	}
	
	/**
	 * 
	 * @param hostName
	 *            The name of this host.
	 * @param serviceName
	 *            The name of the service running on this host.
	 * @param slope
	 *            The default value to use in the declarations.
	 * @param tmax
	 *            The value of tmax to use in the declarations.
	 * @param dmax
	 *            The value of dmax to use in the declarations.
	 * @param heartbeatInteval
	 *            The heartbeat interval in seconds -or- ZERO (0) if we will not
	 *            be sending out the ganglia host heartbeat.
	 */
	public BigdataMetadataFactory(final String hostName,
			final String serviceName, final GangliaSlopeEnum slope,
			final int tmax, final int dmax, final int heartbeatInterval) {

		super(hostName, slope, tmax, dmax, heartbeatInterval);
		
		if(serviceName == null)
			throw new IllegalArgumentException();

		this.serviceName = serviceName;
		
		this.hostDecls = new ConcurrentHashMap<String, IGangliaMetadataMessage>();

		this.serviceDecls = new ConcurrentHashMap<String, IGangliaMetadataMessage>();

		this.resolveDecls = new ConcurrentHashMap<String, IGangliaMetadataMessage>();

		// Declare host metrics.
		addHostMetrics();
		
		// Declare service metrics.
		addServiceCPUMetrics();
		
	}

	/**
	 * Hook to declare host metrics.
	 * 
	 * @see IHostCounters
	 */
	protected void addHostMetrics() {

		addHostInfoMetrics();

		addHostCPUMetrics();

		addHostMemoryMetrics();
		
		addHostDiskMetrics();
		
	}

	protected void addHostInfoMetrics() {

		/*
		 * Align some counters which correspond directly to ganglia core
		 * metrics.
		 */
		addHostCounter(IHostCounters.Info_NumProcessors, cpu_num());
		addHostCounter(IHostCounters.Info_OperatingSystemName, os_name());
		addHostCounter(IHostCounters.Info_OperatingSystemVersion, os_release());

	}
	
	protected void addHostCPUMetrics() {
		/*
		 * TODO Ganglia does not directly report this metric. To conform with
		 * Ganglia we need to report 1-cpu_idle. So, not just a linear scaling
		 * factor. This could be done by sending a formula rather than just a
		 * scaling factor. Or we can just let gmond report this and we can
		 * report %processor time ourselves (which is what happens if we do not
		 * intercept the host metric).
		 */
//		addHostCounter(IRequiredHostCounters.CPU_PercentProcessorTime, cpu_idle());
		addHostCounter(IHostCounters.CPU_PercentSystemTime, cpu_system());
		addHostCounter(IHostCounters.CPU_PercentUserTime, cpu_user());
		addHostCounter(IHostCounters.CPU_PercentIOWait, cpu_wio());
	}

	protected void addHostMemoryMetrics() {
		
		/*
		 * Note: Since this metric has the same name as a ganglia core metric it
		 * MUST have a consistent declaration.
		 */
		addHostCounter(
				IHostCounters.Memory_Bytes_Free,
				new GangliaMetadataMessage(
				// metric_id
						hostName,// host
						"mem_free", // metric name
						false,// spoof
						// metadata record
						GangliaMessageTypeEnum.FLOAT,// metricType
						"mem_free",// metric name (again)
						"KB", // units
						slope,//
						tmax,//
						dmax,//
						getMap(GROUP_MEMORY, "Free Memory",
								"Amount of available memory")) {
					/**
					 * Convert units from bytes to KB.
					 */
					@Override
					public Object translateValue(final Object value) {
						return ((Number) value).doubleValue() / 1024;
					}
				});

		addHostCounter(
				IHostCounters.Memory_majorFaultsPerSecond,
				new GangliaMetadataMessage(
				// metric_id
						hostName,// host
						"mem_faults_major", // metric name
						false,// spoof
						// metadata record
						GangliaMessageTypeEnum.DOUBLE,// metricType
						"mem_faults_major",// metric name (again)
						"faults/sec", // units
						slope,//
						tmax,//
						dmax,//
						getMap(GROUP_MEMORY, "Major page faults",
								"Faults which required loading a page from disk.")));

		addHostCounter(
				IHostCounters.Memory_MinorFaultsPerSec,
				new GangliaMetadataMessage(
				// metric_id
						hostName,// host
						"mem_faults_minor", // metric name
						false,// spoof
						// metadata record
						GangliaMessageTypeEnum.DOUBLE,// metricType
						"mem_faults_minor",// metric name (again)
						"faults/sec", // units
						slope,//
						tmax,//
						dmax,//
						getMap(GROUP_MEMORY, "Minor page faults",
								"Faults that did not require loading a page from disk.")));

		addHostCounter(IHostCounters.Memory_SwapBytesAvailable, swap_total());
		
		// TODO swap_free := swap_total - swap_used
//		addHostCounter(IHostCounters.Memory_SwapBytesUsed, swap_free());

	}
	
	protected void addHostDiskMetrics() {

		/*
		 * IOs/Second (aka IOPs)
		 */
		{

			addHostCounter(
					IHostCounters.PhysicalDisk_ReadsPerSec,
					new GangliaMetadataMessage(
					// metric_id
							hostName,// host
							"disk_reads", // metric name
							false,// spoof
							// metadata record
							GangliaMessageTypeEnum.DOUBLE,// metricType
							"disk_reads",// metric name (again)
							"reads/sec", // units
							slope,//
							tmax,//
							dmax,//
							getMap(GROUP_DISK, "Disk Reads Per Second",
									"The #of disk read operations per second.")));

			addHostCounter(
					IHostCounters.PhysicalDisk_WritesPerSec,
					new GangliaMetadataMessage(
					// metric_id
							hostName,// host
							"disk_writes", // metric name
							false,// spoof
							// metadata record
							GangliaMessageTypeEnum.DOUBLE,// metricType
							"disk_writes",// metric name (again)
							"writes/sec", // units
							slope,//
							tmax,//
							dmax,//
							getMap(GROUP_DISK, "Disk Writes Per Second",
									"The #of disk write operations per second.")));

			addHostCounter(
					IHostCounters.PhysicalDisk_TransfersPerSec,
					new GangliaMetadataMessage(
					// metric_id
							hostName,// host
							"disk_xfers", // metric name
							false,// spoof
							// metadata record
							GangliaMessageTypeEnum.DOUBLE,// metricType
							"disk_xfers",// metric name (again)
							"xfers/sec", // units
							slope,//
							tmax,//
							dmax,//
							getMap(GROUP_DISK,
									"Disk Transfers Per Second",
									"Disk operations per second for the host (total of disk reads per second and disk writes per second)")));

		}

		/*
		 * Bytes/Second
		 */
		{

			addHostCounter(
					IHostCounters.PhysicalDisk_BytesReadPerSec,
					new GangliaMetadataMessage(
					// metric_id
							hostName,// host
							"disk_bytes_read", // metric name
							false,// spoof
							// metadata record
							GangliaMessageTypeEnum.DOUBLE,// metricType
							"disk_bytes_read",// metric name (again)
							"bytes/sec", // units
							slope,//
							tmax,//
							dmax,//
							getMap(GROUP_DISK, "Disk Bytes Read Per Second",
									"Disk bytes read per second for the host")));

			addHostCounter(
					IHostCounters.PhysicalDisk_BytesWrittenPerSec,
					new GangliaMetadataMessage(
					// metric_id
							hostName,// host
							"disk_bytes_written", // metric name
							false,// spoof
							// metadata record
							GangliaMessageTypeEnum.DOUBLE,// metricType
							"disk_bytes_written",// metric name (again)
							"bytes/sec", // units
							slope,//
							tmax,//
							dmax,//
							getMap(GROUP_DISK, "Disk Bytes Written Per Second",
									"Disk bytes written per second for the host")));

			addHostCounter(
					IHostCounters.PhysicalDisk_BytesPerSec,
					new GangliaMetadataMessage(
					// metric_id
							hostName,// host
							"disk_bytes", // metric name
							false,// spoof
							// metadata record
							GangliaMessageTypeEnum.DOUBLE,// metricType
							"disk_bytes",// metric name (again)
							"bytes/sec", // units
							slope,//
							tmax,//
							dmax,//
							getMap(GROUP_DISK,
									"Disk Bytes Per Second",
									"Disk bytes per second for the host (total of bytes read and bytes written per second)")));

		}
		
	}

	/**
	 * Hook to declare service metrics.
	 * 
	 * @see IProcessCounters
	 */
	protected void addServiceMetrics() {

		addServiceInfoMetrics();
		
		addServiceCPUMetrics();
		
		addServiceMemoryMetrics();
		
		addServiceDiskMetrics();
		
		// TODO Are there other categories here?
	}

	protected String getServiceMetricName(final String group, final String label) {
		
		return group + "_" + label;
		
	}
	
	protected void addServiceInfoMetrics() {
		
		// TODO Service INFO metrics.
	}
	
	protected void addServiceCPUMetrics() {

		addServiceCounter(
				IProcessCounters.CPU_PercentProcessorTime,
				new GangliaMetadataMessage(
				// metric_id
						hostName,// host
						getServiceMetricName("cpu","cpu_total"), // metric name
						false,// spoof
						// metadata record
						GangliaMessageTypeEnum.DOUBLE,// metricType
						getServiceMetricName("cpu","cpu_total"),// metric name (again)
						"%", // units
						GangliaSlopeEnum.both,// slope,//
						90,// tmax
						dmax,// dmax,//
						getMap(GROUP_CPU, "CPU Total",
								"Percentage of the time the processor is not idle")));

		addServiceCounter(
				IProcessCounters.CPU_PercentSystemTime,
				new GangliaMetadataMessage(
				// metric_id
						hostName,// host
						getServiceMetricName("cpu","cpu_system"),//metric name
						false,// spoof
						// metadata record
						GangliaMessageTypeEnum.DOUBLE,// metricType
						getServiceMetricName("cpu","cpu_system"),//metric name
						"%", // units
						GangliaSlopeEnum.both,// slope,//
						90,// tmax
						dmax,// dmax,//
						getMap(GROUP_CPU,
								"System Time",
								"Percentage of the time the processor is not idle that it is executing at the system (aka kernel) level (normalized to 100% in single CPU and SMP environments)")));

		addServiceCounter(
				IProcessCounters.CPU_PercentUserTime,
				new GangliaMetadataMessage(
				// metric_id
						hostName,// host
						getServiceMetricName("cpu","cpu_user"),//metric name
						false,// spoof
						// metadata record
						GangliaMessageTypeEnum.DOUBLE,// metricType
						getServiceMetricName("cpu","cpu_user"),//metric name
						"%", // units
						GangliaSlopeEnum.both,// slope,//
						90,// tmax
						dmax,// dmax,//
						getMap(GROUP_CPU,
								"User Time",
								"Percentage of the time the processor is not idle that it is executing at the user level (normalized to 100% in single CPU and SMP environments)")));

	}

	protected void addServiceMemoryMetrics() {

		// TODO Service Memory metrics (JVM and GC related; direct buffers; etc).

		addHostCounter(
				IHostCounters.Memory_majorFaultsPerSecond,
				new GangliaMetadataMessage(
				// metric_id
						hostName,// host
						getServiceMetricName("memory","mem_faults_major"),//metric name
						false,// spoof
						// metadata record
						GangliaMessageTypeEnum.DOUBLE,// metricType
						getServiceMetricName("memory","mem_faults_major"),//metric name
						"faults/sec", // units
						slope,//
						tmax,//
						dmax,//
						getMap(GROUP_MEMORY, "Major Page Faults",
								"Faults which required loading a page from disk")));

		addHostCounter(
				IHostCounters.Memory_MinorFaultsPerSec,
				new GangliaMetadataMessage(
				// metric_id
						hostName,// host
						getServiceMetricName("memory","mem_faults_minor"),//metric name
						false,// spoof
						// metadata record
						GangliaMessageTypeEnum.DOUBLE,// metricType
						getServiceMetricName("memory","mem_faults_minor"),//metric name
						"faults/sec", // units
						slope,//
						tmax,//
						dmax,//
						getMap(GROUP_MEMORY, "Minor Page Faults",
								"Faults that did not require loading a page from disk")));

//	    /**
//	     * The virtual memory usage of the process in bytes.
//	     */
//	    String Memory_virtualSize = Memory + ps + "Virtual Size";
//
//	    /**
//	     * The non-swapped physical memory used by the process in bytes.
//	     */
//	    String Memory_residentSetSize = Memory + ps + "Resident Set Size";
//
//	    /**
//	     * The percentage of the phsyical memory used by the process.
//	     */
//	    String Memory_percentMemorySize = Memory + ps + "Percent Memory Size";
//
//	    /**
//	     * The value reported by {@link Runtime#maxMemory()} (the maximum amount
//	     * of memory that the JVM will attempt to use). This should be a
//	     * {@link OneShotInstrument}.
//	     */
//	    String Memory_runtimeMaxMemory = Memory + ps + "Runtime Max Memory";
//	    
//	    /**
//	     * The value reported by {@link Runtime#freeMemory()} (the amount of
//	     * free memory in the JVM)).
//	     */
//	    String Memory_runtimeFreeMemory = Memory + ps + "Runtime Free Memory";
//	    
//	    /**
//	     * The value reported by {@link Runtime#totalMemory()} (the amount of
//	     * total memory in the JVM, which may vary over time).
//	     */
//	    String Memory_runtimeTotalMemory = Memory + ps + "Runtime Total Memory";
		
	}

	protected void addServiceDiskMetrics() {

		// TODO Service Disk metrics.
	       
//	    /**
//	     * The rate at which the process is reading data from disk in bytes per
//	     * second.
//	     */
//	    String PhysicalDisk_BytesReadPerSec = PhysicalDisk + ps
//	            + "Bytes Read per Second";
//
//	    /**
//	     * The rate at which the process is writing data on the disk in bytes
//	     * per second (cached writes may be reported in this quantity).
//	     */
//	    String PhysicalDisk_BytesWrittenPerSec = PhysicalDisk + ps
//	            + "Bytes Written per Second";

	}

}

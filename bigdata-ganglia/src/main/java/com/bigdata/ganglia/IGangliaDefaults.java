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
package com.bigdata.ganglia;

/**
 * Well known configuration defaults and constants for Ganglia.
 */
public interface IGangliaDefaults {

	/**
	 * The well known multicast address for ganglia service groups.
	 */
	String DEFAULT_GROUP = "239.2.11.71";

	/** The well known port against which ganglia is deployed. */
	int DEFAULT_PORT = 8649;

	/** The maximum size of an XDR message (as per libgmond.c). */
	int BUFFER_SIZE = 1500; // TODO Versus max_udp_msg_len = 1472 ?

	/**
	 * The duration in seconds of the quiet period after a ganglia service
	 * start. During the quiet period a ganglia service will not reset the
	 * timestamp associated with its counters in response to a metadata request.
	 * This is done to avoid multicast storms when several services are
	 * restarted during a short interval.
	 */
	int QUIET_PERIOD = 60 * 10; // aka 10 minutes.

	/**
	 * The waiting period in seconds before the first reporting pass.
	 * <p>
	 * Note: The core ganglia metrics will be self-declared once this period has
	 * expired if they have not already been observed by listening to the
	 * ganglia network.
	 */
	int INITIAL_DELAY = 20;
	
    /**
     * The default heartbeat interval for ganglia hosts.
     * <p>
     * Note: Use ZERO (0) if you are running <code>gmond</code> on the same
     * host. That will prevent the {@link GangliaService} from transmitting a
     * different heartbeat, which would confuse <code>gmond</code> and
     * <code>gmetad</code>.
     * <p>
     * The {@link GangliaService} MUST NOT send out a heartbeat if
     * <code>gmond</code> is also running on the host since the two heartbeats
     * will be different (they are the start time of <code>gmond</code>) and
     * <code>gmond</code> and <code>gmetad</code> will both get confused.
     */
	int HEARTBEAT_INTERVAL = 20;
	
	/**
	 * The interval at which metrics which are collected on this node.
	 * <p>
	 * Each time an updated metric is examined, its current value is copied into
	 * the internal state maintained by this service. If the metric value has
	 * never been published (timestamp is ZERO (0)), if TMax might expire before
	 * we sample the metric again, or if the value of the metric has changed
	 * "sufficiently" then the metric will be sent to any configured
	 * listener(s).
	 * 
	 * @see IGangliaDefaults#MONITORING_INTERVAL
	 */
	int MONITORING_INTERVAL = 20; // 20 seconds.
	
	/**
	 * The default "slope" used to shape curves in RRDTOOL.
	 * 
	 * @see GangliaSlopeEnum
	 */
	GangliaSlopeEnum DEFAULT_SLOPE = GangliaSlopeEnum.both;

	/**
	 * The default time after which ganglia should expect a new value for a
	 * metric (TMAX). This value is only advisory. Metric data are not purged
	 * until DMAX is exceeded. Values which remain unchanged are not
	 * retransmitted unless TMAX might be exceeded before their next reporting
	 * period.
	 */
	int DEFAULT_TMAX = 180; // aka 3 minutes.

	/**
	 * Time in seconds after weak a counter which has not been reported will be
	 * purged -or- ZERO (0) to never purge a counter.
	 */
	int DEFAULT_DMAX = 3600; // aka one hour.

	/**
	 * The default units for reported metrics (ganglia allows metadata for a
	 * metric to indicate its base unit).
	 */
	String DEFAULT_UNITS = "";

}

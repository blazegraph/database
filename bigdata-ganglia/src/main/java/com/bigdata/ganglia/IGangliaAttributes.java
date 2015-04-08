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
 * Attributes for {@link IGangliaMetadataMessage} records.
 * <p>
 * Note: The attribute names, group names, titles, and descriptions for the core
 * metrics can be discovered from the output of
 * 
 * <pre>
 * telnet localhost 8649
 * </pre>
 * 
 * They are configured in <code>gmetad.conf</code>.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/ganglia/wiki/Gmond%203.1%20configuration">Gmond configuration</a>
 * @see <a href="http://www.mail-archive.com/ganglia-developers@lists.sourceforge.net/msg03294.html">Email in Ganglia developers list archive.</a> 
 * @see <a href="http://tobym.posterous.com/gmetric-track-and-group-arbitrary-metrics-wit">Track and Group Arbitrary Metrics</a>
 */
public interface IGangliaAttributes {

	/**
	 * The name of the optional attribute which is used to collect metrics into
	 * groups in the UI. The value of the attribute is the name of the metric
	 * group to which a metric belongs.
	 */
	String ATTR_GROUP = "GROUP";
	
	/**
	 * The name of the optional attribute whose value is a nice "title" for the
	 * metric.
	 */
	String ATTR_TITLE = "TITLE";
	
	/**
	 * The name of the optional attribute whose value is a description of the
	 * attribute.
	 * <p>
	 * Note: The maximum size of a packet for the ganglia protocol puts a
	 * realistic limit on how verbose a description can be.
	 */
	
	String ATTR_DESC = "DESC";
	
	/*
	 * The following are group names used by Ganglia.
	 */
	
	/**
	 * The name of the group for some "core" per-host metrics (boottime, etc).
	 */
	String GROUP_CORE = "core";
	
	/**
	 * The name of the group for per-host CPU metrics.
	 */
	String GROUP_CPU = "cpu";

	/**
	 * The name of the group for per-host memory metrics.
	 */
	String GROUP_MEMORY = "memory";

	/**
	 * The name of the group for per-host disk metrics.
	 */
	String GROUP_DISK = "disk";

	/**
	 * The name of the group for per-host network metrics.
	 */
	String GROUP_NETWORK = "network";

	/**
	 * The name of the group for per-host load metrics (load_one, etc).
	 */
	String GROUP_LOAD = "load";
	
	/**
	 * The name of the group for per-host process metrics (proc_run, proc_total).
	 */
	String GROUP_PROCESS = "process";
	
	/**
	 * The name of the group for per-host metadata metrics (os_name, etc).
	 */
	String GROUP_SYSTEM = "system";

}

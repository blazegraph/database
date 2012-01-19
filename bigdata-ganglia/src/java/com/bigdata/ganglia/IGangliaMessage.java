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
 * A ganglia message.
 */
public interface IGangliaMessage {

	/** The code for the type of message. */
	public GangliaMessageTypeEnum getRecordType();

	/**
	 * The name of the host for which the metric was reported.
	 * <p>
	 * Note: When {@link #isSpoof()} reports <code>true</code> this will be
	 * <code>ip:host</code>.
	 */
	public String getHostName();

	/**
	 * The name of the metric (this needs to be clean for use in a file system
	 * so the application name of the metric needs to be munged before it is
	 * saved here).
	 */
	public String getMetricName();

	/**
	 * Return <code>true</code> iff this message contains the metadata for a
	 * metric (the metric declaration).
	 * 
	 * @see GangliaMessageTypeEnum#METADATA
	 */
	public boolean isMetricMetadata();

	/**
	 * Return <code>true</code> iff this message is a request for a metric.
	 * 
	 * @see GangliaMessageTypeEnum#REQUEST
	 */
	public boolean isMetricRequest();

	/** Return <code>true</code> if this message represents a metric value. */
	public boolean isMetricValue();
	
	/**
	 * <code>true</code> iff this is a spoofed message.
	 * <p>
	 * Note: Spoof messages format {@link #getHostName()} as
	 * <code>ip:host</code> and provide a means to fake metrics for a host which
	 * is down (or which is not running ganglia).
	 * </p>
	 */
	public boolean isSpoof();

}
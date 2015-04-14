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
 * A ganglia request for a metric.
 * <p>
 * Ganglia sends out request messages when a service wants to obtain fresh
 * metrics. The gmond instances handle the request by clearing the timestamp
 * associated with the metric. This causes the metric to be retransmitted soon
 * thereafter (a ZERO (0) timestamp is interpreted as the metric having never
 * been transmitted).
 * <p>
 * In order to prevent multicast storms, a service will conditionally clear the
 * timestamp associated with a metric if it has been running for at least some
 * minimum period of time (e.g., 10 minutes).
 * <p>
 * In order to generate request messages, a service needs to notice the first
 * time any given metric name is declared (including metrics which it is
 * self-reporting) and then multicast a request for that metric in order to
 * obtain its value from all participating ganglia services. There is no means
 * for a service to request a metric which it is neither self-reported nor
 * observed.
 * <p>
 * It appears that ganglia actually tracks the age of metric groups as well as
 * individual metrics and will reset the timestamp on the group if a request is
 * received for any metric in that group.
 * <p>
 * The request record itself does not have any additional fields. It is just the
 * metric_id header with the appropriate byte code for the record type.
 */
public class GangliaRequestMessage extends AbstractGangliaMessage implements
		IGangliaRequestMessage {

	public GangliaRequestMessage(final String hostName,
			final String metricName, final boolean spoof) {

		super(GangliaMessageTypeEnum.REQUEST, hostName, metricName,
				spoof);

	}

	@Override
	public boolean isMetricValue() {
		return false;
	}

	@Override
	public boolean isMetricRequest() {
		return true;
	}

	@Override
	public boolean isMetricMetadata() {
		return false;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "{recordType=" + getRecordType()
				+ ", hostName=" + getHostName() + ", metricName="
				+ getMetricName() + ", spoof=" + isSpoof() + "}";
	}

	@Override
	public boolean equals(final Object o) {

		if (o == this)
			return true;

		final IGangliaRequestMessage t = (IGangliaRequestMessage) o;

		if (!getRecordType().equals(t.getRecordType()))
			return false;

		if (!getHostName().equals(t.getHostName()))
			return false;

		if (!getMetricName().equals(t.getMetricName()))
			return false;

		if (isSpoof() != t.isSpoof())
			return false;

		return true;

	}
	
	@Override
	public int hashCode() {

		return getMetricName().hashCode();
		
	}
	
}

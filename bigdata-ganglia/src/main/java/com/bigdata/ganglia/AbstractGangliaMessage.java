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
 * Base class for ganglia 3.1 wire format messages.
 */
public abstract class AbstractGangliaMessage implements IGangliaMessage {

	private final GangliaMessageTypeEnum recordType;
	private final String hostName;
	private final String metricName;
	private final boolean spoof;

	public AbstractGangliaMessage(final GangliaMessageTypeEnum recordType,
			final String hostName, final String metricName, final boolean spoof) {
		
		if (recordType == null)
			throw new IllegalArgumentException();
		
		if (hostName == null)
			throw new IllegalArgumentException();
		
		if (metricName == null)
			throw new IllegalArgumentException();
		
		this.recordType = recordType;
		
		this.hostName = hostName;
		
		this.metricName = metricName;
		
		this.spoof = spoof;
		
	}
	
	@Override
	public GangliaMessageTypeEnum getRecordType() {
		return recordType;
	}

	@Override
	public String getHostName() {
		return hostName;
	}

	@Override
	public String getMetricName() {
		return metricName;
	}

	@Override
	public boolean isSpoof() {
		return spoof;
	}

	/*
	 * Overridden to ensure that subclasses implement this.
	 * 
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	abstract public boolean equals(Object o);

	/*
	 * Overridden to ensure that subclasses implement this.
	 * 
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	abstract public int hashCode();

}

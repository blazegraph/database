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

import java.util.Map;

/**
 * A host report.
 */
public class HostReport implements IHostReport {

	private final String hostName;
	private final Map<String, IGangliaMetricMessage> metrics;

    public HostReport(final String hostName,
            final Map<String, IGangliaMetricMessage> metrics) {

		if(hostName == null)
			throw new IllegalArgumentException();

		if(metrics == null)
			throw new IllegalArgumentException();
		
		this.hostName = hostName;
		
		this.metrics = metrics;
		
	}
	
	@Override
	public String getHostName() {

		return hostName;
		
	}

	@Override
	public Map<String, IGangliaMetricMessage> getMetrics() {

		return metrics;
		
	}

	@Override
	public String toString() {
	    
        return getClass().getName() + "{hostName=" + hostName + ", metrics="
                + metrics + "}";

	}
}
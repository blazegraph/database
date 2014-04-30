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

import java.io.Serializable;
import java.util.Comparator;

/**
 * Orders {@link IHostReport}s. 
 */
public class HostReportComparator implements Comparator<IHostReport>,
        Serializable {

	/**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private final String metricName;
	private final boolean asc;

	/**
	 * 
	 * @param metricName
	 *            The name of the metric which is used to impose an order on
	 *            the {@link IHostReport}s.
	 * @param asc
	 *            <code>true</code> for an ascending sort and
	 *            <code>false</code> for a descending sort.
	 */
	public HostReportComparator(final String metricName,final boolean asc) {

		if (metricName == null)
			throw new IllegalArgumentException();

		this.metricName = metricName;
		
		this.asc = asc;

	}

	@Override
	public int compare(final IHostReport o1, final IHostReport o2) {

		final int ret = comp(o1, o2);

		if (asc)
			return ret;

		return -ret;

	}

	private int comp(final IHostReport o1, final IHostReport o2) {

		final IGangliaMetricMessage m1 = o1.getMetrics().get(metricName);

		final IGangliaMetricMessage m2 = o2.getMetrics().get(metricName);

		if (m1 == null && m2 == null)
			return 0;
		else if (m1 == null)
			return -1;
		else if (m2 == null)
			return -1;

		final double d1 = Double.parseDouble(m1.getStringValue());

		final double d2 = Double.parseDouble(m2.getStringValue());

		if (d1 < d2)
			return -1;
		else if (d2 > d1)
			return 1;
		
		/*
		 * Order by host name in case of a tie on the metric. This makes the
		 * results more stable. (We could also round the metric a little to
		 * improve stability. But that can be done in a custom comparator.)
		 */
	
		return o1.getHostName().compareTo(o2.getHostName());

	}

}
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

import java.util.Collections;
import java.util.Map;

/**
 * Default factory will always produce an {@link IGangliaMetadataMessage} for
 * a metric.
 */
public class DefaultMetadataFactory implements IGangliaMetadataFactory {

	private final String defaultUnits;
	private final GangliaSlopeEnum defaultSlope;
	private final int defaultTMax;
	private final int defaultDMax;

	public DefaultMetadataFactory(final String defaultUnits,
			final GangliaSlopeEnum defaultSlope, final int defaultTMax,
			final int defaultDMax) {
		
		if (defaultUnits == null)
			throw new IllegalArgumentException();
		if (defaultSlope == null)
			throw new IllegalArgumentException();
		if (defaultTMax < 0)
			throw new IllegalArgumentException();
		if (defaultDMax < 0)
			throw new IllegalArgumentException();

		this.defaultUnits = defaultUnits;

		this.defaultSlope = defaultSlope;
		
		this.defaultTMax = defaultTMax;
		
		this.defaultDMax = defaultDMax;

	}
	
	/**
	 * Note: If the metric name contains a <code>.</code> then the metric will
	 * be placed into a group named by everything in the metricName up to that
	 * <code>.</code> character.
	 */
	public IGangliaMetadataMessage newDecl(final String hostName,
			final String metricName, final Object value) {

		final int lastIndexOf = metricName.lastIndexOf('.');
		
		final String groupName;
		
		if(lastIndexOf == -1) {
			
			groupName = null;
			
		} else {
			
			groupName = metricName.substring(0, lastIndexOf);
			
		}
		
		/*
		 * Setup a metric declaration based on some defaults and the type of the
		 * observed value (gtype).
		 */

		final String units = defaultUnits;
		final GangliaSlopeEnum slope = defaultSlope;
		final int tmax = defaultTMax;
		final int dmax = defaultDMax;

		/*
		 * The group(s) in the web UI with which this metric will be associated.
		 */
		@SuppressWarnings("unchecked")
		final Map<String, String[]> extraValues = (Map<String, String[]>) (groupName == null ? Collections
				.emptyMap() : Collections.singletonMap(
				IGangliaAttributes.ATTR_GROUP, new String[] { groupName }));

		/*
		 * Figure out the best match ganglia data type to use for the metric
		 * declaration.
		 */
		final GangliaMessageTypeEnum metricType = GangliaMessageTypeEnum
				.forJavaValue(value);

		// Return the metric declaration.
		return new GangliaMetadataMessage(hostName, metricName,
				false/* spoof */, metricType, metricName, units, slope, tmax,
				dmax, extraValues);

	}

	/**
	 * Always returns the caller's argument.
	 */
	@Override
	public IGangliaMetadataMessage resolve(final IGangliaMetadataMessage decl) {

		return decl;

	}

}

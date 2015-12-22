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
 * A Ganglia 3.1 message representing the declaration of a metric.
 */
public interface IGangliaMetadataMessage extends IGangliaMessage {
	
	/**
	 * The data type declaration for the metric.
	 */
	public GangliaMessageTypeEnum getMetricType();

	/**
	 * The metric name (this is represented twice in the Ganglia 3.1 wire
	 * format: once in the metric_id header and once in the metadata record;
	 * both values SHOULD be the same).
	 * 
	 * @see #getMetricName()
	 */
	public String getMetricName2();

	/** The units in which the metric values are expressed. */
	public String getUnits();

	/** Hint for rrdtool visualization of metric values. */
	public GangliaSlopeEnum getSlope();

	/**
	 * The maximum advisory delay in seconds before a metric value would become
	 * stale. For the metric sending, this is also the maximum delay before it
	 * should retransmit a metric value which is unchanged from its last
	 * reported value.
	 */
	public int getTMax();

	/**
	 * The maximum age in seconds before a metric will be deleted (aka Delete
	 * Max).
	 */
	public int getDMax();
	
	/**
	 * Zero or more additional name-value[] pairs serialized with this message.
	 * This may be used to group metrics, add nice titles, provide in-band
	 * descriptions of metrics, etc.
	 */
	public Map<String, String[]> getExtraValues();

	/**
	 * Return the first value for the given key from the
	 * {@link #getExtraValues()}.
	 * 
	 * @param key
	 *            The key.
	 *            
	 * @return The first value for that key -or- <code>null</code> if there are
	 *         no values for that key.
	 */
	public String getFirstValue(String key);

	/**
	 * Return the value of the well-known attribute
	 * {@link IGangliaAttributes#ATTR_TITLE}.
	 * 
	 * @return The value of the TITLE attribute if present and otherwise
	 *         <code>null</code>.
	 */
	public String[] getGroups();

	/**
	 * Return the value of the well-known attribute
	 * {@link IGangliaAttributes#ATTR_TITLE}.
	 * 
	 * @return The value of the {@link IGangliaAttributes#ATTR_TITLE} attribute if
	 *         present and otherwise <code>null</code>.
	 */
	public String getTitle();

	/**
	 * Return the value of the well-known attribute
	 * {@link IGangliaAttributes#ATTR_DESC}.
	 * 
	 * @return The value of the {@link IGangliaAttributes#ATTR_DESC} attribute if
	 *         present and otherwise <code>null</code>.
	 */
	public String getDescription();

	/**
	 * Return a value which may have been scaled and/or offset in order to align
	 * the value with the metric declaration. Use cases for translation include
	 * changing the units in which a metric is collected (e.g., bytes versus
	 * kilobytes) to conform with ganglia or translating the metric from an
	 * offset (e.g., 1-foo).
	 * 
	 * @param value
	 *            The value.
	 * 
	 * @return The translated value.
	 */
	public Object translateValue(Object value);
	
	/**
	 * Return <code>true</code> iff the newValue differs significantly from the
	 * old value.
	 * 
	 * @param oldValue
	 *            The old value and never <code>null</code>.
	 * @param newValue
	 *            The new value and never <code>null</code>.
	 *            
	 * @return <code>true</code> iff there is a significant difference.
	 */
	public boolean isChanged(final Object oldValue, final Object newValue);
	
}

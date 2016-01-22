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

import java.util.Arrays;
import java.util.Map;

/**
 * A ganglia metric metadata (aka declaration) record.
 * <p>
 * This record must be sent if a service is reporting a metric for the first
 * time. In addition, ganglia resends the metadata records periodically by
 * tracking their age (but <code>send_metadata_interval = 0</code> in the
 * default <code>gmond.conf</code>).
 */
public class GangliaMetadataMessage extends AbstractGangliaMessage implements
		IGangliaMetadataMessage {

	private final GangliaMessageTypeEnum metricType;
	private final String metricName2;
	private final String units;
	private final GangliaSlopeEnum slope;
	private final int tmax;
	private final int dmax;
	private final Map<String, String[]> extraValues;

	public GangliaMetadataMessage(
			final String hostName, final String metricName,
			final boolean spoof, final GangliaMessageTypeEnum metricType,
			final String metricName2, final String units,
			final GangliaSlopeEnum slope, final int tmax, final int dmax,
			final Map<String, String[]> extraValues) {

		super(GangliaMessageTypeEnum.METADATA, hostName, metricName,
				spoof);
		
		if (metricType == null)
			throw new IllegalArgumentException();
		
		if (metricName2 == null)
			throw new IllegalArgumentException();

		if (!metricName.equals(metricName2))
			throw new IllegalArgumentException();
		
		if (units == null)
			throw new IllegalArgumentException();
		
		if (slope == null)
			throw new IllegalArgumentException();
		
		if (extraValues == null)
			throw new IllegalArgumentException();

		this.metricType = metricType;
		this.metricName2 = metricName2;
		this.units = units;
		this.slope = slope;
		this.tmax = tmax;
		this.dmax = dmax;
		this.extraValues = extraValues;
	}

	@Override
	public boolean isMetricValue() {
		return false;
	}

	@Override
	public boolean isMetricRequest() {
		return false;
	}

	@Override
	public boolean isMetricMetadata() {
		return true;
	}

	@Override
	public GangliaMessageTypeEnum getMetricType() {
		return metricType;
	}

	@Override
	public String getMetricName2() {
		return metricName2;
	}

	@Override
	public String getUnits() {
		return units;
	}

	@Override
	public GangliaSlopeEnum getSlope() {
		return slope;
	}

	@Override
	public int getTMax() {
		return tmax;
	}

	@Override
	public int getDMax() {
		return dmax;
	}

	@Override
	public Map<String, String[]> getExtraValues() {
		return extraValues;
	}
	
	public String[] getGroups() {
		return extraValues.get(IGangliaAttributes.ATTR_GROUP);
	}
	
	public String getTitle() {
		return getFirstValue(IGangliaAttributes.ATTR_TITLE);
	}
	
	public String getDescription() {
		return getFirstValue(IGangliaAttributes.ATTR_DESC);
	}
	
	public String getFirstValue(final String key) {
		final String[] a = extraValues.get(key);
		if (a == null || a.length == 0)
			return null;
		return a[0];
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder(128);
		sb.append(getClass().getSimpleName());
		sb.append("{ recordType=");
		sb.append(getRecordType());
		sb.append(", hostName=");
		sb.append(getHostName());
		sb.append(", metricName=");
		sb.append(getMetricName());
		sb.append(", spoof=");
		sb.append(isSpoof());
		sb.append(", metricType=");
		sb.append(getMetricType());
		sb.append(", metricName2=");
		sb.append(getMetricName2());
		sb.append(", units=");
		sb.append(getUnits());
		sb.append(", slope=");
		sb.append(getSlope());
		sb.append(", tmax=");
		sb.append(getTMax());
		sb.append(", dmax=");
		sb.append(getDMax());
		sb.append(", extraValues=");
		{
			sb.append("{");
			if (!extraValues.isEmpty()) {
				boolean first = true;
				for (Map.Entry<String, String[]> e : extraValues.entrySet()) {
					if (!first)
						sb.append(",");
					first = false;
					sb.append(e.getKey());
					sb.append("=");
					final String[] v = e.getValue();
					if (v.length == 0) {
						// Nothing given. Should not happen.
						sb.append("[]");
					} else if (v.length == 1) {
						sb.append('\"');
						sb.append(v[0]);
						sb.append('\"');
					} else {
						sb.append("[");
						for (int i = 0; i < v.length; i++) {
							if (i > 1)
								sb.append(",");
							sb.append('\"');
							sb.append(v[i]);
							sb.append('\"');
						}
						sb.append("]");
					}
				}
			} // if(!extraValues.isEmpty()) ...
			sb.append("}");
		}
		sb.append("}");
		return sb.toString();
	}

	@Override
	public boolean equals(final Object o) {

		if (o == this)
			return true;

		final IGangliaMetadataMessage t = (IGangliaMetadataMessage) o;

		if (!getRecordType().equals(t.getRecordType()))
			return false;

		if (!getHostName().equals(t.getHostName()))
			return false;

		if (!getMetricName().equals(t.getMetricName()))
			return false;

		if (isSpoof() != t.isSpoof())
			return false;

		if (!getMetricType().equals(t.getMetricType()))
			return false;

		if (!getMetricName2().equals(t.getMetricName2()))
			return false;

		if (!getUnits().equals(t.getUnits()))
			return false;

		if (!getSlope().equals(t.getSlope()))
			return false;

		if (getTMax() != t.getTMax())
			return false;

		if (getDMax() != t.getDMax())
			return false;

		final Map<String, String[]> e1 = getExtraValues();
		
		final Map<String, String[]> e2 = t.getExtraValues();

		if (e1.size() != e2.size())
			return false;

		for (Map.Entry<String, String[]> e : e1.entrySet()) {

			final String key = e.getKey();

			final String[] val1 = e.getValue();

			final String[] val2 = e2.get(key);

			if (!Arrays.equals(val1, val2))
				return false;

		}
		
		return true;

	}
	
	@Override
	public int hashCode() {

		return getMetricName().hashCode();
		
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * The default implementation is an identity transform. In order for this
	 * method to be applied the application MUST resolve metadata declarations
	 * received over the wire to local declarations in which this method has the
	 * desired behavior. This resolution step is necessary because the ganglia
	 * wire protocol is data-only and the translation steps can not be readily
	 * expressed as data (unless they are turned into a full formula evaluation
	 * model, adding substantial complexity to the ganglia integration).
	 * 
	 * TODO We could also add a method which return true iff two values differ
	 * sufficient for an updated metric record to be sent out.
	 */
	@Override
	public Object translateValue(final Object value) {

		return value;
		
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * The default implementation returns <code>true</code> iff the two values
	 * are not equals().
	 */
	@Override
	public boolean isChanged(final Object oldValue, final Object newValue) {

		final boolean changed = !oldValue.equals(newValue);

		return changed;

	}

}

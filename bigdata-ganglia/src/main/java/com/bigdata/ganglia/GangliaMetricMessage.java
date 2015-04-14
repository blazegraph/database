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
 * A ganglia 3.1 metric message.
 */
public class GangliaMetricMessage extends AbstractGangliaMessage implements
		IGangliaMetricMessage {

	private final String format;
	private final Object value;

	/**
	 * Constructor from data.
	 * 
	 * @param recordType
	 * @param hostName
	 * @param metricName
	 * @param spoof
	 * @param format
	 * @param value
	 */
	public GangliaMetricMessage(final GangliaMessageTypeEnum recordType,
			final String hostName, final String metricName, final boolean spoof,
			final String format, final Object value) {

		super(recordType, hostName, metricName, spoof);
		
		switch (recordType) {
		case DOUBLE:
		case FLOAT:
		case INT32:
		case INT16:
		case STRING:
		case UINT32:
		case UINT16:
			break;
		default:
			throw new IllegalArgumentException();
		}

		if (format == null)
			throw new IllegalArgumentException();
		
		if (value == null)
			throw new IllegalArgumentException();
		
		this.format = format;
		
		this.value = value;

	}

	@Override
	public boolean isMetricValue() {
		return true;
	}

	@Override
	public boolean isMetricRequest() {
		return false;
	}

	@Override
	public boolean isMetricMetadata() {
		return false;
	}

	@Override
	public String getFormat() {
		return format;
	}

	@Override
	public boolean isNumeric() {
		switch (getRecordType()) {
		case STRING:
			return false;
		default:
			return true;
		}
	}

	@Override
	public Object getValue() {
		return value;
	}

	@Override
	public String getStringValue() {
		if (value instanceof String) {
			return (String) value;
		}
		return "" + value;
	}

	@Override
	public Number getNumericValue() {
		if (value instanceof Number) {
			return (Number) value;
		}
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "{recordType=" + getRecordType()
				+ ", hostName=" + getHostName() + ", metricName="
				+ getMetricName() + ", spoof=" + isSpoof() + ", format="
				+ format + ", value=" + value
				+ (" (valueClass=" + value.getClass().getSimpleName() + ")")
				+ "}";
	}

	@Override
	public boolean equals(final Object o) {

		if (o == this)
			return true;

		final IGangliaMetricMessage t = (IGangliaMetricMessage) o;

		if (!getRecordType().equals(t.getRecordType()))
			return false;

		if (!getHostName().equals(t.getHostName()))
			return false;

		if (!getMetricName().equals(t.getMetricName()))
			return false;

		if (isSpoof() != t.isSpoof())
			return false;

		if (!getValue().equals(t.getValue()))
			return false;

		if (getValue().getClass() != t.getValue().getClass())
			return false;

		if (!getFormat().equals(t.getFormat()))
			return false;

		return true;

	}
	
	@Override
	public int hashCode() {

		return getMetricName().hashCode();
		
	}
	
}

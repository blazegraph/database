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

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A reported or observed metric value with a timestamp. This is used to report
 * metric values which are unchanged no more frequently than tmax seconds.
 * <p>
 * Note: This class deliberately does not use a generic for the data type. The
 * data type of the last sampled value remains fluid within instances of this
 * class. The data type will be converted to the
 * {@link IGangliaMetadataMessage#getMetricType()} when an
 * {@link IGangliaMetricMessage} is generated from the current value.
 */
public class TimestampMetricValue implements ITimestampMetricValue {

	/**
	 * The metric metadata declaration record.
	 */
	private final IGangliaMetadataMessage decl;

	/**
	 * The current value of the metric.
	 */
	private Object value;

    /**
     * The timestamp (in milliseconds) associated with the most recent value for
     * this metric and initially ZERO (0) if the metric is newly declared.
     */
    private volatile long timestamp = 0L;

	public TimestampMetricValue(final IGangliaMetadataMessage decl) {

		if (decl == null)
			throw new IllegalArgumentException();

		this.decl = decl;

	}

	@Override
    public IGangliaMetadataMessage getMetadata() {
		
		return decl;
		
	}

	@Override
    public long getTimestamp() {
		
		return timestamp;
		
	}
	
	/**
	 * Reset the timestamp. The next time the metric is updated it will
	 * self-report that it's value needs to be sent to ganglia listeners.
	 */
	public void resetTimestamp() {
		
		timestamp = 0;
		
	}
	
	@Override
    public int getAge() {
		
		if (timestamp == 0L) {
		
			/*
			 * Ensure a positive age (no truncation) if we have never reported
			 * this metric.
			 */

			return Integer.MAX_VALUE;
			
		}
		
		return (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()
				- timestamp);
		
	}

	@Override
    synchronized public Object getValue() {
		
		return value;
		
	}
	
	/**
	 * Update the value associated with the metric.
	 * <p>
	 * This method handles any translation which must be applied to the value to
	 * bring it into line with the {@link IGangliaMetadataMessage} for the
	 * metric.
	 * <p>
	 * The as stored data type is not important as long as it remains either
	 * {@link String} or a {@link Number}. The {@link IGangliaMessageEncoder} is
	 * responsible for encoding the value according to the ganglia data type
	 * declaration in the {@link IGangliaMetadataMessage}.
	 * 
	 * @param newValue
	 *            The new value.
	 * 
	 * @return <code>true</code> if the value changed enough to be worth
	 *         reporting or if tmax has expired so we need to report the value
	 *         anyway.
	 * 
	 * @throws IllegalArgumentException
	 *             if <i>newValue</i> is <code>null</code>.
	 * @throws IllegalArgumentException
	 *             if the metricType is numeric and <i>newValue</i> is not a
	 *             {@link Number}.
	 */
	synchronized public boolean setValue(final Object newValue) {

		if (newValue == null) {
			/*
			 * The given value MUST NOT be null.
			 */
			throw new IllegalArgumentException();
			
		}
		
		final boolean isNumeric = decl.getMetricType().isNumeric();
		
		if (isNumeric && !(newValue instanceof Number)) {
			/*
			 * The given value MUST be a Number.
			 */
			throw new IllegalArgumentException("Not a number: " + newValue
					+ " (" + newValue.getClass() + ")");
		}

		/*
		 * Optionally translate the newValue.
		 */
		final Object translatedValue = decl.translateValue(newValue);

		if (translatedValue == null) {
			/*
			 * The translated value MUST NOT be null.
			 */
			throw new IllegalArgumentException();

		}

		if (isNumeric && !(translatedValue instanceof Number)) {
			/*
			 * The translated value MUST be a Number.
			 */
			throw new IllegalArgumentException("Not a number: "
					+ translatedValue + " (" + translatedValue.getClass() + ")");
		}

		/*
		 * Update the value.
		 */

		// Save a copy of the oldValue
		final Object oldValue = value;

		if(isNumeric) {

			/*
			 * Numeric.
			 */
			this.value = translatedValue;
			
		} else {

			/*
			 * Non-numeric.
			 */
			this.value = translatedValue.toString();

		}

		final boolean changed = oldValue == null
				|| decl.isChanged(oldValue, newValue);

		if (changed || getAge() >= decl.getTMax() / 2) {

			/*
			 * Either the value has changed or the last reported value is old
			 * enough that we need to retransmit it now.
			 * 
			 * Note: Tmax is divided by two to help avoid timeouts in which
			 * metrics which we have on hand would otherwise appear as stale to
			 * other ganglia clients.
			 */
			
			return true;
			
		}

		// Do not retransmit this metric.
		return false;
		
	}

	/**
	 * Update the timestamp.
	 */
	public void update() {
	
		timestamp = System.currentTimeMillis();
		
	}
	
	/**
	 * Return the priority. The priority orders metrics which have never been
	 * reported first (timestamp is ZERO) followed by metrics in descending
	 * <code>tmax - age</code>, where age is <code>now - timestamp</code>.
	 * <p>
	 * The highest priority is associated with the most negative value. The
	 * lowest priority is associated with the most positive value. These ar the
	 * semantics used by a {@link PriorityBlockingQueue}.
	 */
	public int priority() {
		
		if (timestamp == 0) {
			/*
			 * Anything which has never been reported has top priority.
			 * 
			 * Since a new declaration has top priority, we need to be careful
			 * that it does not make it onto the priority queue until it has a
			 * bound value, otherwise we will try to report a [null] value.
			 */
			return Integer.MIN_VALUE;
		}
		
		// The age (in seconds).
		final int age = getAge();

		// The priority is based on how soon TMax would expire.
		final int priority = decl.getTMax() - age;
		
		return priority;
		
	}

	/** Human readable representation. */
	@Override
	public String toString() {

		return getClass().getName() + "{age=" + getAge() + ",value="
				+ getValue() + ",decl=" + getMetadata() + "}";

	}
	
}

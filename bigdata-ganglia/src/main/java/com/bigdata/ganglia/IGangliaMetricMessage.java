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
 * A ganglia message representing a metric value.
 */
public interface IGangliaMetricMessage extends IGangliaMessage {

	/**
	 * The printf format string associated with the metric value.
	 */
	public String getFormat();

	/**
	 * Return <code>true</code> if the metric value is a numeric, in which case
	 * it can be cast to a {@link Number}.
	 */
	public boolean isNumeric();

	/**
	 * Return the metric value (non-<code>null</code>).
	 */
	public Object getValue();

	/**
	 * Return a String representation of the metric value (always succeeds).
	 */
	public String getStringValue();

	/**
	 * Return the {@link Number} for the metric value.
	 * 
	 * @throws UnsupportedOperationException
	 *             if the metric value is not numeric.
	 */
	public Number getNumericValue();

}

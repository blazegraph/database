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
 * Factory associates the {@link IGangliaMetadataMessage} with the
 * {@link IGangliaMetricMessage}.
 */
public class RichMetricFactory {

	/**
	 * Method creates an {@link IGangliaMetricMessage} which is consistent with
	 * the supplied {@link IGangliaMetadataMessage}.
	 * 
	 * @param hostName
	 *            The host reporting this metric value (should be ip:host if
	 *            this is a spoof record).
	 * @param decl
	 *            The metric declaration.
	 * @param spoof
	 *            if this is a spoof record.
	 * @param value
	 *            The value.
	 * 
	 * @throws IllegalArgumentException
	 *             if the hostName is <code>null</code>.
	 * @throws IllegalArgumentException
	 *             if the declaration is <code>null</code>.
	 * @throws IllegalArgumentException
	 *             if the value is <code>null</code>.
	 */
	public RichMetricMessage newMetricMessage(final String hostName,
			final IGangliaMetadataMessage decl, final boolean spoof,
			final Object value) {

		if (hostName == null)
			throw new IllegalArgumentException();

		if (decl == null)
			throw new IllegalArgumentException();

		if (value == null)
			throw new IllegalArgumentException();

		/*
		 * Note: Use the printf format specified by the metadata for this
		 * metric. That will govern how the metric record gets generated from
		 * the caller's value.
		 */

		final String format = decl.getMetricType().getFormat();

		return new RichMetricMessage(hostName, decl, spoof, format, value);

	}

}

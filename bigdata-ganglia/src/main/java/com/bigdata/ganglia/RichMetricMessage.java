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
 * Class bundles together an {@link IGangliaMetricMessage} with the
 * {@link IGangliaMetadataMessage} which provides its declaration.
 */
public class RichMetricMessage extends GangliaMetricMessage {

	/** The declaration. */
	private final IGangliaMetadataMessage decl;

	/**
	 * The declaration and never <code>null</code>.
	 */
	public IGangliaMetadataMessage getMetadata() {
		
		return decl;
		
	}
	
	/**
	 * Constructor produces an {@link IGangliaMetricMessage} which is consistent
	 * with the {@link IGangliaMetadataMessage}.
	 * 
	 * @param hostName
	 *            The host reporting this metric value (should be ip:host if
	 *            this is a spoof record).
	 * @param decl
	 *            The metric declaration.
	 * @param spoof
	 *            if this is a spoof record.
	 * @param format
	 *            The printf format to be used.
	 * @param value
	 *            The value.
	 * 
	 * @throws IllegalArgumentException
	 *             if the hostName is <code>null</code>.
	 * @throws NullPointerException
	 *             if the declaration is <code>null</code>.
	 * @throws IllegalArgumentException
	 *             if the format is <code>null</code>.
	 * @throws IllegalArgumentException
	 *             if the value is <code>null</code>.
	 */
	public RichMetricMessage(final String hostName,
			final IGangliaMetadataMessage decl, final boolean spoof,
			final String format, final Object value) {

		super(decl.getMetricType(), hostName, decl.getMetricName(), spoof,
				format, value);

		this.decl = decl;

	}

}

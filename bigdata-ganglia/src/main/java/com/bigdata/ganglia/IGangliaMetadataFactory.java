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
 * A factory for registering application metrics dynamically.
 */
public interface IGangliaMetadataFactory {
	
	/**
	 * Factory for new declarations.
	 * 
	 * @param hostName
	 *            The name of this host.
	 * @param metricName
	 *            The ganglia metric name (suitably munged if necessary for use
	 *            by ganglia).
	 * @param value
	 *            The metric value (NOT <code>null</code>).
	 * 
	 * @return The metric declaration -or- <code>null</code> if this factory
	 *         does not know how to declare this metric.
	 */
	public IGangliaMetadataMessage newDecl(final String hostName,
			final String metricName, final Object value);

	/**
	 * Resolve a declaration received over the wire into a declaration with a
	 * richer object behavior. This allows the application to substitute its own
	 * versions of an {@link IGangliaMetadataMessage}.
	 * 
	 * @param decl
	 *            A declaration (typically received over the wire).
	 * 
	 * @return The declaration to be used.
	 */
	public IGangliaMetadataMessage resolve(final IGangliaMetadataMessage decl);

}

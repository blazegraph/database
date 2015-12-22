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

import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * A factory which can be extended by an application.
 */
public class GangliaMetadataFactory implements IGangliaMetadataFactory {

	private final CopyOnWriteArraySet<IGangliaMetadataFactory> list = new CopyOnWriteArraySet<IGangliaMetadataFactory>();

	private final IGangliaMetadataFactory defaultFactory;
	
	public GangliaMetadataFactory(final DefaultMetadataFactory defaultFactory) {

		if (defaultFactory == null)
			throw new IllegalArgumentException();

		this.defaultFactory = defaultFactory;
		
	}

	public void add(final IGangliaMetadataFactory factory) {

		if (factory == null)
			throw new IllegalArgumentException();

		list.add(factory);

	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to try each registered delegate first and then the fallback
	 * delegate specified in the constructor. The first declaration returned by
	 * a delegate is returned to the caller.
	 */
	@Override
	public IGangliaMetadataMessage newDecl(final String hostName,
			final String metricName, final Object value) {
		
		final Iterator<IGangliaMetadataFactory> itr = list.iterator();
		
		IGangliaMetadataMessage decl = null;
		
		while(itr.hasNext() && decl == null) {
		
			final IGangliaMetadataFactory impl = itr.next();
			
			decl = impl.newDecl(hostName, metricName, value);
			
		}

		if (decl == null) {

			decl = defaultFactory.newDecl(hostName, metricName, value);

		}

		return decl;

	}

	/**
	 * Always returns the caller's argument.
	 */
	@Override
	public IGangliaMetadataMessage resolve(final IGangliaMetadataMessage decl) {

		return decl;

	}

}

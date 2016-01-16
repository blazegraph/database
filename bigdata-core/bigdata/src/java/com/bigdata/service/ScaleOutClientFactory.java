/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package com.bigdata.service;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.HAStatusServletUtilProxy;

/**
 * 
 * Convenience class to break out bigdata-jini dependency. See BLZG-1370.
 * 
 * @author beebs
 * 
 */
public class ScaleOutClientFactory {

	public static final String DEFAULT_PROVIDER = "com.bigdata.service.jini.JiniClient";

	public static AbstractScaleOutClient<?> getJiniClient(
			final String[] propertyFiles) {
		return getJiniClient(DEFAULT_PROVIDER, propertyFiles);
	}

	public static AbstractScaleOutClient<?> getJiniClient(
			final String provider, final String[] propertyFiles) {

		try {
			final Class<?> c = Class.forName(provider);
			final Constructor<?> cons = c.getConstructor();
			final Object object = cons.newInstance(propertyFiles);
			final AbstractScaleOutClient proxy = (AbstractScaleOutClient) object;
			return proxy;
		} catch (ClassNotFoundException | NoSuchMethodException
				| SecurityException | InstantiationException
				| IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {

			throw new RuntimeException(e);
		}

	}
}

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
/**
 * 
 * This servlet implements a proxy pattern to allow for separation of the 
 * bigdata-blueprints package.
 * 
 */
package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

public class BlueprintsServletProxy extends BigdataRDFServlet {
	
	private static final String DEFAULT_PROVIDER = "com.bigdata.blueprints.webapp.BlueprintsServlet";
	
    static private final transient Logger log = Logger.getLogger(BlueprintsServletProxy.class);

	/**
	 * Flag to signify a blueprints operation.
	 */
	public static final transient String ATTR_BLUEPRINTS = "blueprints";


	/**
	 * 
	 */
	private static final long serialVersionUID = 6037374358611489293L;
	
	public void doPostRequest(HttpServletRequest req, HttpServletResponse resp) throws IOException
	{
		throw new RuntimeException("Running without the Blueprints package.");
	}
	
	public static String getDefaultProvider() {
		
		return DEFAULT_PROVIDER;
		
	}
	
	public static class BlueprintsServletFactory {
		
		public static BlueprintsServletProxy getInstance() {
			return getInstance(DEFAULT_PROVIDER);
		}
		
		public static BlueprintsServletProxy getInstance(final String provider) {
			
			try {
				final Class<?> c = Class.forName(provider);
				final Constructor<?> cons = c.getConstructor();
				final Object object = cons.newInstance();
				final BlueprintsServletProxy proxy = (BlueprintsServletProxy) object;
				return proxy;
			} catch (ClassNotFoundException | NoSuchMethodException
					| SecurityException | InstantiationException
					| IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				if (log.isDebugEnabled()) {
					log.debug(e.toString());
				}
				//If we're running without the blueprint package, just return a proxy.
				return new BlueprintsServletProxy();
				
			}

		}
	}

}

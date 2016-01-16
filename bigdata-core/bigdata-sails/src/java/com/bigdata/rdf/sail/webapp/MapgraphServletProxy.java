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
 * bigdata-gpu package.
 * 
 */
package com.bigdata.rdf.sail.webapp;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.util.ClassPathUtil;

public class MapgraphServletProxy extends BigdataRDFServlet {
	
	private static final String DEFAULT_PROVIDER = "com.blazegraph.gpu.webapp.MapgraphServlet";
	
    static private final transient Logger log = Logger.getLogger(MapgraphServletProxy.class);

	/**
	 * Flag to signify a mapgraph operation.
	 */
	public static final transient String ATTR_MAPGRAPH = "mapgraph";

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public void doPostRequest(HttpServletRequest req, HttpServletResponse resp) throws IOException
	{
		throw new RuntimeException("Running without the Mapgraph package.");
	}
	
	public static String getDefaultProvider() {
		
		return DEFAULT_PROVIDER;
		
	}
	
	/**
     * Factory pattern for a servlet that is discovered at runtime. Either the
     * real servlet or its base class (the proxy servlet) will be returned by
     * the factory. This is used by the {@link RESTServlet} to discover,
     * initialize, delegate, and destroy such servlets using a runtime discovery
     * pattern.
     * 
     * @author beebs
     * @author bryan
     */
	public static class MapgraphServletFactory {
		
		public MapgraphServletProxy getInstance() {
			return getInstance(DEFAULT_PROVIDER);
		}
		
		public MapgraphServletProxy getInstance(final String provider) {

		    return ClassPathUtil.classForName(//
		                provider, // preferredClassName,
		                MapgraphServletProxy.class, // defaultClass,
		                MapgraphServletProxy.class, // sharedInterface,
		                getClass().getClassLoader() // classLoader
		        );

//			try {
//				final Class<?> c = Class.forName(provider);
//				final Constructor<?> cons = c.getConstructor();
//				final Object object = cons.newInstance();
//				final MapgraphServletProxy proxy = (MapgraphServletProxy) object;
//				return proxy;
//			} catch (ClassNotFoundException | NoSuchMethodException
//					| SecurityException | InstantiationException
//					| IllegalAccessException | IllegalArgumentException
//					| InvocationTargetException e) {
//				if (log.isDebugEnabled()) {
//					log.debug(e.toString());
//				}
//				//If we're running without the mapgraph package, just return a proxy.
//				return new MapgraphServletProxy();
//				
//			}

		}
	}

}

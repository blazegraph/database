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
package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeoutException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.journal.IIndexManager;
import com.bigdata.quorum.AsynchronousQuorumCloseException;

/**
 * Proxy class / factory pattern to break bigdata-jini dependency.  See BLZG-1370.
 * 
 * @author beebs
 *
 */
public class HAStatusServletUtilProxy {
	
	private static final String DEFAULT_PROVIDER = "com.bigdata.rdf.sail.webapp.HAStatusServletUtil";
	
	private static final String WITHOUT_JINI_MSG = "Running without the bigdata-jini package.  See BLZG-1370.";
	
    static private final transient Logger log = Logger.getLogger(HAStatusServletUtilProxy.class);
    
    protected HAStatusServletUtilProxy(IIndexManager indexManager) {
    } 
	
	/**
	 * Show the interesting things about the quorum.
	 * <ol>
	 * <li>QuorumState</li>
	 * <li>Who is the leader, who is a follower.</li>
	 * <li>What is the SPARQL end point for each leader and follower.</li>
	 * <li>Dump of the zookeeper state related to the quorum.</li>
	 * <li>listServices (into pre element).</li>
	 * </ol>
	 * 
	 * @throws IOException
	 */
	public void doGet(HttpServletRequest req,
			HttpServletResponse resp, XMLBuilder.Node current)
			throws IOException {
    	if(log.isInfoEnabled()) {
    		log.info(WITHOUT_JINI_MSG);
    	}
	}

	/**
	 * Special reporting request for HA status.
	 * 
	 * @param req
	 * @param resp
	 * @throws TimeoutException
	 * @throws InterruptedException
	 * @throws AsynchronousQuorumCloseException
	 * @throws IOException
	 */
	public void doHAStatus(HttpServletRequest req,
			HttpServletResponse resp) throws IOException {
    	if(log.isInfoEnabled()) {
    		log.info(WITHOUT_JINI_MSG);
    	}
	}

	/**
	 * Basic server health info
	 * 
	 * @param req
	 * @param resp
	 * @throws TimeoutException
	 * @throws InterruptedException
	 * @throws AsynchronousQuorumCloseException
	 * @throws IOException
	 */
	public void doHealthStatus(HttpServletRequest req,
			HttpServletResponse resp) throws IOException {
    	if(log.isInfoEnabled()) {
    		log.info(WITHOUT_JINI_MSG);
    	}
	}

	
	public static String getDefaultProvider() {
		
		return DEFAULT_PROVIDER;
		
	}
	
	public static class HAStatusServletUtilFactory {
		
		public /*static*/ HAStatusServletUtilProxy getInstance(IIndexManager indexManager) {
			return getInstance(DEFAULT_PROVIDER, indexManager);
		}

		/*
         * TODO This uses an unknown classloader . I suggest changing things (as
         * I have) to make the getInstance() method non-static and use the
         * ClassPathUtil as indicated so it uses the class loader of the caller.
         * Rather than initialize the proxy servlet with the IIndexManager in
         * the constructor, the caller can just attach this as a servlet request
         * attribute and pass it through that way.
         */
		public HAStatusServletUtilProxy getInstance(final String provider, IIndexManager indexManager) {
			
//            return ClassPathUtil.classForName(//
//                    provider, // preferredClassName,
//                    HAStatusServletUtilProxy.class, // defaultClass,
//                    HAStatusServletUtilProxy.class, // sharedInterface,
//                    getClass().getClassLoader() // classLoader
//            );
            
			try {
				final Class<?> c = Class.forName(provider);
				final Constructor<?> cons = c.getConstructor(IIndexManager.class);
				final Object object = cons.newInstance(indexManager);
				final HAStatusServletUtilProxy proxy = (HAStatusServletUtilProxy) object;
				return proxy;
			} catch (ClassNotFoundException | NoSuchMethodException
					| SecurityException | InstantiationException
					| IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				if (log.isDebugEnabled()) {
					log.debug(e.toString());
				}
				//If we're running without the bigdata-jini package, just return a proxy.
				return new HAStatusServletUtilProxy(indexManager);
				
			}

		}
	}

}

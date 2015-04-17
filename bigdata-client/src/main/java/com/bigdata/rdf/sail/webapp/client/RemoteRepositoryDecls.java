/**
Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
package com.bigdata.rdf.sail.webapp.client;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

/**
 * Various declarations used by the client API.
 * <p>
 * Note: Some of these fields are replicated from the com.bigdata.rdf.store.BD
 * interface in order to avoid dragging in other aspects of the bigdata code
 * base.
 */
public class RemoteRepositoryDecls {

   /**
    * The namespace used for bigdata specific extensions.
    */
   private static final String BD_NAMESPACE = "http://www.bigdata.com/rdf#";

   protected static final URI BD_NULL_GRAPH = new URIImpl(BD_NAMESPACE + "nullGraph");
   
   /**
    * The name of the <code>UTF-8</code> character encoding.
    */
   static protected final String UTF8 = "UTF-8";

   /**
    * The name of the system property that may be used to specify the default
    * HTTP method (GET or POST) for a SPARQL QUERY or other indempotent
    * request. 
    * 
    * @see #DEFAULT_QUERY_METHOD
    * 
    * @see <a href="http://trac.blazegraph.com/ticket/854"> Allow overrride of
    *      maximum length before converting an HTTP GET to an HTTP POST </a>
    */
   static public final String QUERY_METHOD = RemoteRepository.class
           .getName() + ".queryMethod";
   
   /**
    * Note: The default is {@value #DEFAULT_QUERY_METHOD}. This supports use
    * cases where the end points are read/write databases and http caching must
    * be defeated in order to gain access to the most recent committed state of
    * the end point.
    * 
    * @see #getQueryMethod()
    * @see #setQueryMethod(String)
    */
   static public final String DEFAULT_QUERY_METHOD = "POST";

   /**
    * The name of the system property that may be used to specify the maximum
    * length (in characters) for a requestURL associated with an HTTP GET
    * before it is automatically converted to an HTTP POST.
    * 
    * @see <a href="http://trac.blazegraph.com/ticket/854"> Allow overrride of
    *      maximum length before converting an HTTP GET to an HTTP POST </a>
    */
   static public final String MAX_REQUEST_URL_LENGTH = RemoteRepository.class
           .getName() + ".maxRequestURLLength";
   
   /**
    * The default maximum limit on a requestURL before the request is converted
    * into a POST using a <code>application/x-www-form-urlencoded</code>
    * request entity.
    * <p>
    * Note: I suspect that 2000 might be a better default limit. If the limit
    * is 4096 bytes on the target, then, even with UTF encoding, most queries
    * having a request URL that is 2000 characters long should go through with
    * a GET. 1000 is a safe value but it could reduce http caching.
    */
   static public final int DEFAULT_MAX_REQUEST_URL_LENGTH = 1000;
   
   /**
    * The name of the property whose value is the namespace of the KB to be
    * created.
    * <p>
    * Note: This string is identicial to one defined by the BigdataSail
    * options, but the client API must not include a dependency on the Sail so
    * it is given by value again here in a package local scope.
    * 
    * @see #DEFAULT_NAMESPACE
    */
   public static final String OPTION_CREATE_KB_NAMESPACE = "com.bigdata.rdf.sail.namespace";

   /**
    * The name of the default namespace.
    */
   public static final String DEFAULT_NAMESPACE = "kb";

   /**
    * HTTP header may be used to specify the timeout for a query.
    * 
    * @see http://trac.blazegraph.com/ticket/914 (Set timeout on remote query)
    */
   static protected final String HTTP_HEADER_BIGDATA_MAX_QUERY_MILLIS = "X-BIGDATA-MAX-QUERY-MILLIS";
      
}

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
package com.bigdata.rdf.sail.webapp.client;

import java.util.UUID;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.TupleQueryResult;

/**
 * Interface for the Java API to the NanoSparqlServer. See 
 * <a href="https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=NanoSparqlServer"
 * this page</a> for more information on the HTTP API.  The instance will be
 * constructed with the endpoint URL for the NSS and the namespace for the kb.  
 * It will then issue a query to learn some basic metadata about the database
 * (e.g. what mode it's in).
 */
public interface IRemoteRepository {

	/**
	 * Prepare a tuple (select) query.
	 * 
	 * @param query
	 * 			the query string
	 * @return
	 * 			the {@link TupleQuery}
	 */
	TupleQuery prepareTupleQuery(String query) throws Exception;
	
	/**
	 * Prepare a graph query.
	 * 
	 * @param query
	 * 			the query string
	 * @return
	 * 			the {@link GraphQuery}
	 */
	GraphQuery prepareGraphQuery(String query) throws Exception;
	
	/**
	 * Prepare a boolean (ask) query.
	 * 
	 * @param query
	 * 			the query string
	 * @return
	 * 			the {@link BooleanQuery}
	 */
	BooleanQuery prepareBooleanQuery(String query) throws Exception;

	/**
	 * Cancel a query running remotely on the server.
	 * 
	 * @param queryID
	 * 			the UUID of the query to cancel
	 */
	void cancel(UUID queryID) throws Exception;

	/**
	 * Perform a fast range count on the statement indices for a given
	 * triple (quad) pattern.
	 * 
	 * @param s
	 * 			the subject (can be null)
	 * @param p
	 * 			the predicate (can be null)
	 * @param o
	 * 			the object (can be null)
	 * @param c
	 * 			the context (can be null)
	 * @return
	 * 			the range count
	 */
	long rangeCount(URI s, URI p, Value o, URI c) throws Exception;
	
	/**
	 * A prepared query will hold metadata for a particular query instance.
	 * <p>
	 * Right now, the only metadata is the query ID.
	 */
	public static interface Query {

		/**
		 * Return the query ID.  Can be used in conjunction with 
		 * {@link IRemoteRepository#cancel(UUID)}. 
		 * 
		 * @return
		 * 			the query ID
		 */
		UUID getQueryId();
		
	}
	
	public static interface TupleQuery extends Query {
		
		/**
		 * Evaluate the query remotely on the server and parse the response
		 * into a <code>TupleQueryResult</code>.
		 */
		TupleQueryResult evalaute();
		
	}
	
	public static interface GraphQuery extends Query {
		
		/**
		 * Evaluate the query remotely on the server and parse the response
		 * into a <code>GraphQueryResult</code>.
		 */
		GraphQueryResult evaluate();
		
	}
	
	public static interface BooleanQuery extends Query {
		
		/**
		 * Evaluate the query remotely on the server and parse the response
		 * into a <code>boolean</code>.
		 */
		boolean evaluate();
		
	}
	
	/**
	 * Adds RDF data to the remote repository.
	 * 
	 * @param add
	 *        The RDF data to be added.
	 */
	void add(AddOp add)
		throws Exception;

	/**
	 * Removes RDF data from the remote repository.
	 * 
	 * @param remove
	 *        The RDF data to be removed.
	 */
	void remove(RemoveOp remove)
		throws Exception;

	/**
	 * Perform an ACID update (delete+insert) per the semantics of
	 * <a href="https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=NanoSparqlServer#UPDATE_.28DELETE_.2B_INSERT.29">
	 * the NanoSparqlServer.
	 * </a> 
	 * 
	 * @param remove
	 *        The RDF data to be removed.
	 * @param add
	 * 		  The RDF data to be added.        
	 */
	void update(RemoveOp remove, AddOp add)
		throws Exception;

	
	/**
	 * This class will have a number of different ctors:
	 * <ul>
	 * <li><code>AddOp(InputStream in, String baseURI, RDFFormat format)</code></li>
	 * <li><code>AddOp(Iterable<Statement> stmts)</code></li>
	 * <li><code>AddOp(URL url, String baseURI, RDFFormat format)</code></li>
	 * </ul>  
	 */
	public static interface AddOp {
	}
	
	/**
	 * This class will have a number of different ctors:
	 * <ul>
	 * <li><code>RemoveOp(InputStream in, String baseURI, RDFFormat format)</code></li>
	 * <li><code>RemoveOp(Iterable<Statement> stmts)</code></li>
	 * <li><code>RemoveOp(URI s, URI p, Value o, URI c)</code></li>
	 * <li><code>RemoveOp(String sparqlQuery)</code></li>
	 * </ul>  
	 */
	public static interface RemoveOp {
	}
	
}

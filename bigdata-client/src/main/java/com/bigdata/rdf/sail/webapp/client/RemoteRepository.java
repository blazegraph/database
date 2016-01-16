/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2014.  All rights reserved.

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

import info.aduna.io.IOUtil;

import java.io.Closeable;
import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.mime.FormBodyPart;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.ByteArrayBody;
import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepository;

/**
 * Java API to the Nano Sparql Server.
 * <p>
 * Note: The {@link RemoteRepository} object SHOULD be reused for multiple
 * operations against the same end point.
 * 
 * @see <a href=
 *      "https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=NanoSparqlServer"
 *      > NanoSparqlServer REST API </a>
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/628" > Create
 *      a bigdata-client jar for the NSS REST API </a>
 */
public class RemoteRepository extends RemoteRepositoryBase {

    private static final transient Logger log = Logger
            .getLogger(RemoteRepository.class);

   /**
    * The {@link RemoteRepositoryManager} object use to manage all access to the
    * service backing the {@link #sparqlEndpointURL}.
    */
    private final RemoteRepositoryManager mgr;
    
    /**
     * The service end point for the default data set.
     */
    private final String sparqlEndpointURL;

    /**
    * When non-<code>null</code> the operations against the
    * {@link #sparqlEndpointURL} will be isolated by the transaction.
    */
    private final IRemoteTx tx;
    
    /**
     * Return the SPARQL end point.
     */
    public String getSparqlEndPoint() {
        
        return sparqlEndpointURL;
        
    }
        
    @Override
    public String toString() {

      return super.toString() + "{sparqlEndpoint=" + sparqlEndpointURL
            + ", mgr=" + getRemoteRepositoryManager()
            + (tx == null ? "" : ", tx=" + tx) + "}";

    }

    /**
     * The {@link RemoteRepositoryManager} object use to manage all access to the
     * service backing the {@link #sparqlEndpointURL}.
     */
    public RemoteRepositoryManager getRemoteRepositoryManager() {

       return mgr;
       
    }

    /**
     * Create a connection to a remote repository. This can be used with any
     * SPARQL end point as long as you restrict yourself to SPARQL QUERY or
     * SPARQL UPDATE.  The other methods can only be used with a blazegraph
     * backend.
     * 
     * @param mgr
     * @param sparqlEndpointURL
     *            The SPARQL end point.
     * 
     * @see RemoteRepositoryManager
     * @see HttpClientConfigurator
     * @see <a href="http://wiki.blazegraph.com/wiki/index.php/HALoadBalancer">
     *      HALoadBalancer </a>
     */
   RemoteRepository(final RemoteRepositoryManager mgr,
         final String sparqlEndpointURL, final IRemoteTx tx) {

       if (mgr == null)
          throw new IllegalArgumentException();

        if (sparqlEndpointURL == null)
            throw new IllegalArgumentException();

        this.mgr = mgr;
        
        this.sparqlEndpointURL = sparqlEndpointURL;
        
        this.tx = tx;
        
    }

   /**
    * Flyweight method returns a wrapper for the sparql end point associated
    * with this instance.
    */
   public BigdataSailRemoteRepository getBigdataSailRemoteRepository() {

      return new BigdataSailRemoteRepository(this);

   }
    
    /**
     * Post a GraphML file to the blueprints layer of the remote bigdata instance.
     */
    public long postGraphML(final String path) throws Exception {
        
        // TODO Allow client to specify UUID for postGraphML. See #1254.
        final UUID uuid = UUID.randomUUID();
        
        final ConnectOptions opts = mgr.newConnectOptions(sparqlEndpointURL, uuid, tx);

        opts.addRequestParam("blueprints");

        JettyResponseListener response = null;
        try {

            final File file = new File(path);
            
            if (!file.exists()) {
                throw new RuntimeException("cannot locate file: " + file.getAbsolutePath());
            }
            
            final byte[] data = IOUtil.readBytes(file);
            
            final ByteArrayEntity entity = new ByteArrayEntity(data);

            entity.setContentType(ConnectOptions.MIME_GRAPH_ML);
            
            opts.entity = entity;
            
            opts.setAcceptHeader(ConnectOptions.MIME_APPLICATION_XML);

            checkResponseCode(response = doConnect(opts));

            final MutationResult result = mutationResults(response);

            return result.mutationCount;

        } finally {

        	if (response != null)
        		response.abort();
            
        }
        
    }
    
    /**
     * Return the SPARQL 1.1 Service Description for the end point.
     */
    public GraphQueryResult getServiceDescription() throws Exception {

        return getServiceDescription(UUID.randomUUID());
        
    }
    
    public GraphQueryResult getServiceDescription(final UUID uuid)
            throws Exception {

        // TODO Unit test when isolated by a transaction. The server is already
        // creating a tx for this so it might hit a fence post.
        
        final ConnectOptions opts = mgr.newConnectOptions(sparqlEndpointURL, uuid, tx);

        opts.method = "GET";

        opts.setAcceptHeader(ConnectOptions.DEFAULT_GRAPH_ACCEPT_HEADER);

        return mgr
                .graphResults(opts, uuid, null/* preparedListener */);

    }

    /**
     * Prepare a tuple (select) query.
     * 
     * @param query
     *            the query string
     * 
     * @return The {@link IPreparedTupleQuery}.
     */
    public IPreparedTupleQuery prepareTupleQuery(final String query)
            throws Exception {

        return prepareTupleQuery(query, UUID.randomUUID());

    }

    /**
     * Prepare a tuple (select) query.
     * 
     * @param query
     *            the query string
     * 
     * @param uuid
     *            The {@link UUID} used to identify this query.
     * 
     * @return The {@link IPreparedTupleQuery}.
     */
    public IPreparedTupleQuery prepareTupleQuery(final String query,
            final UUID uuid) throws Exception {

        return new TupleQuery(
                mgr.newQueryConnectOptions(sparqlEndpointURL, uuid, tx), uuid, query);

    }

    /**
     * Prepare a graph query.
     * 
     * @param query
     *            the query string
     * 
     * @return The {@link IPreparedGraphQuery}
     */
    public IPreparedGraphQuery prepareGraphQuery(final String query)
            throws Exception {

        return prepareGraphQuery(query, UUID.randomUUID());

    }

    /**
     * Prepare a graph query.
     * 
     * @param query
     *            the query string
     * @param uuid
     *            The {@link UUID} used to identify this query.
     * 
     * @return The {@link IPreparedGraphQuery}
     */
    public IPreparedGraphQuery prepareGraphQuery(final String query,
            final UUID uuid) throws Exception {

        return new GraphQuery(
                mgr.newQueryConnectOptions(sparqlEndpointURL, uuid, tx), uuid, query);

    }

    /**
     * Prepare a boolean (ask) query.
     * 
     * @param query
     *            the query string
     * 
     * @return the {@link IPreparedBooleanQuery}
     */
    public IPreparedBooleanQuery prepareBooleanQuery(final String query)
            throws Exception {

        return prepareBooleanQuery(query, UUID.randomUUID());

    }

    /**
     * Prepare a boolean (ask) query.
     * 
     * @param query
     *            the query string
     * 
     * @param uuid
     *            The {@link UUID} used to identify this query.
     *            
     * @return the {@link IPreparedBooleanQuery}
     */
    public IPreparedBooleanQuery prepareBooleanQuery(final String query, final UUID uuid)
            throws Exception {

        return new BooleanQuery(mgr.newQueryConnectOptions(sparqlEndpointURL, uuid, tx), uuid, query);

    }

    /**
     * Prepare a SPARQL UPDATE request.
     * 
     * @param updateStr
     *            The SPARQL UPDATE request.
     * 
     * @return The {@link SparqlUpdate} operation.
     * 
     * @throws Exception
     */
    public IPreparedSparqlUpdate prepareUpdate(final String updateStr)
            throws Exception {

        return prepareUpdate(updateStr, UUID.randomUUID());

    }

    /**
     * Prepare a SPARQL UPDATE request.
     * 
     * @param updateStr
     *            The SPARQL UPDATE request.
     * @param uuid
     *            The {@link UUID} used to identify this query.
     * 
     * @return The {@link SparqlUpdate} operation.
     * 
     * @throws Exception
     */
    public IPreparedSparqlUpdate prepareUpdate(final String updateStr, final UUID uuid)
            throws Exception {

        return new SparqlUpdate(mgr.newUpdateConnectOptions(sparqlEndpointURL, uuid,
                tx), uuid, updateStr);

    }

    /**
     * Return all matching statements.
     * 
     * @param subj
     * @param pred
     * @param obj
     * @param includeInferred
     * @param contexts
     * @return
     * @throws Exception
     */
    public GraphQueryResult getStatements(final Resource subj, final URI pred,
            final Value obj, final boolean includeInferred,
            final Resource... contexts) throws Exception {

    	if (contexts == null) {
            // Note: May not be a null Resource[] reference.
            // MAY be Resource[null], which is the openrdf nullGraph.
            // See #1177
            throw new IllegalArgumentException();
         }
         
            final UUID uuid = UUID.randomUUID();
            final ConnectOptions opts = mgr.newQueryConnectOptions(sparqlEndpointURL, uuid, tx);

            opts.addRequestParam("GETSTMTS");
            opts.addRequestParam(RemoteRepositoryDecls.INCLUDE_INFERRED,
                  Boolean.toString(includeInferred));
            if (subj != null) {
               opts.addRequestParam("s", EncodeDecodeValue.encodeValue(subj));
            }
            if (pred != null) {
               opts.addRequestParam("p", EncodeDecodeValue.encodeValue(pred));
            }
            if (obj != null) {
               opts.addRequestParam("o", EncodeDecodeValue.encodeValue(obj));
            }
           	opts.addRequestParam("c", EncodeDecodeValue.encodeContexts(contexts));

           	opts.setAcceptHeader(ConnectOptions.DEFAULT_GRAPH_ACCEPT_HEADER);

            JettyResponseListener resp = null;
            try {

               checkResponseCode(resp = doConnect(opts));
               
              GraphQueryResult result = mgr.graphResults(opts, null, null);
              
              return result;

            } finally {

               if (resp != null)
                  resp.abort();

            }

    }
    
   /**
    * Method to line up with the Sesame interface.
    * 
    * @param s
    *           The subject (optional).
    * @param p
    *           The predicate (optional).
    * @param o
    *           The value (optional).
    * @param includeInferred
    *           when <code>true</code> inferred statements will also be
    *           considered.
    * @param c
    *           The contexts (optional, BUT may not be a null Resource[]).
    *           
    * @return <code>true</code> iff a statement exists that matches the request.
    * 
    * @throws Exception
    * 
    * @see <a href="http://trac.bigdata.com/ticket/1109" >hasStatements can
    *      overestimate and ignores includeInferred (REST API) </a>
    * @see <a href="http://trac.bigdata.com/ticket/1177"> Resource... contexts
    *      not encoded/decoded according to openrdf semantics (REST API) </a>
    */
   public boolean hasStatement(final Resource s, final URI p, final Value o,
         final boolean includeInferred, final Resource... c) throws Exception {
      if (c == null) {
         // Note: May not be a null Resource[] reference.
         // MAY be Resource[null], which is the openrdf nullGraph.
         // See #1177
         throw new IllegalArgumentException();
      }
      final UUID uuid = UUID.randomUUID();
      final ConnectOptions opts = mgr.newQueryConnectOptions(sparqlEndpointURL, uuid, tx);

      opts.addRequestParam("HASSTMT");
      opts.addRequestParam(RemoteRepositoryDecls.INCLUDE_INFERRED,
            Boolean.toString(includeInferred));
      if (s != null) {
         opts.addRequestParam("s", EncodeDecodeValue.encodeValue(s));
      }
      if (p != null) {
         opts.addRequestParam("p", EncodeDecodeValue.encodeValue(p));
      }
      if (o != null) {
         opts.addRequestParam("o", EncodeDecodeValue.encodeValue(o));
      }
      opts.addRequestParam("c", EncodeDecodeValue.encodeContexts(c));

      JettyResponseListener resp = null;
      try {

         opts.setAcceptHeader(ConnectOptions.MIME_APPLICATION_XML);

         checkResponseCode(resp = doConnect(opts));

         final BooleanResult result = RemoteRepositoryManager.booleanResults(resp);

         return result.result;

      } finally {

         if (resp != null)
            resp.abort();

      }

   }
    
    private String asConstOrVar(final AST2SPARQLUtil util, final String var,
            final Value val) {

        if (val == null)
            return var;

        return util.toExternal(val);
        
    }
    
    /**
     * Cancel a query running remotely on the server.
     * 
     * @param queryID
     *             the UUID of the query to cancel
     */
    public void cancel(final UUID queryId) throws Exception {
    
       mgr.cancel(queryId);
            
    }
    
    /**
    * Perform a fast range count on the statement indices for a given triple
    * (quad) pattern.
    * 
    * @param s
    *           the subject (can be null)
    * @param p
    *           the predicate (can be null)
    * @param o
    *           the object (can be null)
    * @param c
    *           the context (can be null)
    *           
    * @return the range count
    * 
    * @see <a href="http://trac.bigdata.com/ticket/1127"> Add REST API method
    *      for exact range counts </a>
    */
   public long rangeCount(final Resource s, final URI p, final Value o,
         final Resource... c) throws Exception {

      return rangeCount(false/* exact */, s, p, o, c);
       
    }

   /**
    * Perform a range count on the statement indices for a given triple (quad)
    * pattern.
    * <p>
    * Note: fast range counts are *fast*. They require two key probes into the
    * indices. Exact range counts are only fast when the indices do not support
    * isolation or fused views. Isolation is used if the namespace supports full
    * read/write transactions. Fused views are used in scale-out to model shards
    * and are also used in full read/write transaction support.
    * 
    * @param exact
    *           if <code>true</code> then an exact range count is requested,
    *           otherwise a fast range count is requested.
    * @param s
    *           the subject (can be null)
    * @param p
    *           the predicate (can be null)
    * @param o
    *           the object (can be null)
    * @param c
    *           the context (can be null)
    * 
    * @return the range count
    * 
    * @see <a href="http://trac.bigdata.com/ticket/1127"> Add REST API method
    *      for exact range counts </a>
    * @see <a href="http://trac.bigdata.com/ticket/1177"> Resource... contexts
    *      not encoded/decoded according to openrdf semantics (REST API) </a>
    */
   public long rangeCount(final boolean exact, final Resource s, final URI p,
         final Value o, final Resource... c) throws Exception {

      if (c == null) {
         // Note: May not be a null Resource[] reference.
         // MAY be Resource[null], which is the openrdf nullGraph.
         // See #1177
         throw new IllegalArgumentException();
      }

      // TODO Allow client to specify UUID for ESTCARD. See #1254.
      final UUID uuid = UUID.randomUUID();
      final ConnectOptions opts = mgr.newQueryConnectOptions(sparqlEndpointURL, uuid, tx);

        opts.addRequestParam("ESTCARD");
        if (exact) {
           opts.addRequestParam("exact", "true");
        }
        if (s != null) {
            opts.addRequestParam("s", EncodeDecodeValue.encodeValue(s));
        }
        if (p != null) {
            opts.addRequestParam("p", EncodeDecodeValue.encodeValue(p));
        }
        if (o != null) {
            opts.addRequestParam("o", EncodeDecodeValue.encodeValue(o));
        }
        opts.addRequestParam("c", EncodeDecodeValue.encodeContexts(c));
        
        JettyResponseListener resp = null;
        try {
            
            opts.setAcceptHeader(ConnectOptions.MIME_APPLICATION_XML);
            
            checkResponseCode(resp = doConnect(opts));
            
            final RangeCountResult result = rangeCountResults(resp);
            
            return result.rangeCount;
            
        } finally {
            
        	if (resp != null)
        		resp.abort();
                     
        }

    }
    
   /**
    * Perform a fast range count on the statement indices. This reports an
    * estimate of the number of statements in the namespace. That estimate is
    * exact unless the namespace is provisioned for full read/write transactions
    * or the endpoint is scale-out.
    * 
    * @return the range count (#of statements in the database).
    */
   public long size() throws Exception {

      return rangeCount(/* s */null, /* p */null, /* o */null);

   }
    
    /**
    * Return a list of contexts in use in a remote quads database.
    * 
    * FIXME This should be a streaming response for scalability. That will
    * require us to change the return type for the method. E.g., to something
    * that implements {@link Closeable}. Callers will then have to invoke
    * {@link Closeable#close()} to avoid leaking resources. (This change could
    * be made when making the CONTEXTS an operation that can be given a UUID
    * for cancellation by the client.)
    */
    public Collection<Resource> getContexts() throws Exception {
    	
        // TODO Allow client to specify UUID for CONTEXTS. See #1254.
        final UUID uuid = UUID.randomUUID();

        final ConnectOptions opts = mgr.newQueryConnectOptions(sparqlEndpointURL, uuid, tx);

        opts.addRequestParam("CONTEXTS");

        JettyResponseListener resp = null;
        try {
            
            opts.setAcceptHeader(ConnectOptions.MIME_APPLICATION_XML);
            
            checkResponseCode(resp = doConnect(opts));
            
            final ContextsResult result = contextsResults(resp);
            
            return result.contexts;
            
        } finally {
            
        	if (resp != null)
        		resp.abort();
                      
        }
    	
    }
    

    /**
     * Adds RDF data to the remote repository.
     * 
     * @param add
     *            The RDF data to be added.
     * 
     * @return The mutation count.
     */
    public long add(final AddOp add) throws Exception {
        
        return add(add, UUID.randomUUID()/*queryId*/);
        
    }
    
    /**
     * Adds RDF data to the remote repository.
     * 
     * @param add
     *            The RDF data to be added.
     * @param uuid
     *            The {@link UUID} used to identify this query.
     *            
     * @return The mutation count.
     * 
     * @see See #1254 / BLZG-1259
     */
    public long add(final AddOp add, final UUID uuid) throws Exception {

        if (add == null)
            throw new IllegalArgumentException();

        final ConnectOptions opts = mgr.newUpdateConnectOptions(
                sparqlEndpointURL, uuid, tx);
        
        add.prepareForWire();
        
        if (add.format != null) {
            
            final ByteArrayEntity entity = new ByteArrayEntity(add.data);

            entity.setContentType(add.format.getDefaultMIMEType());
            
            opts.entity = entity;
            
        }
  
        if (add.uris != null) {
            // set the resource(s) to load
            opts.addRequestParam("uri", add.uris.toArray(new String[0]));
        }
        
        if (add.context != null && add.context.length > 0) {
            // set the default context.
            opts.addRequestParam("context-uri", toStrings(add.context));
        }
        
        opts.setAcceptHeader(ConnectOptions.MIME_APPLICATION_XML);
        
        JettyResponseListener response = null;
        boolean ok = false;
        try {
            
            checkResponseCode(response = doConnect(opts));
            
            final MutationResult result = mutationResults(response);
            
            ok = true;
            
            return result.mutationCount;
            
        } finally {

            if (response != null) {
                // Abort the http response handling.
                response.abort();
                if (!ok) {
                    try {
                        /*
                         * POST back to the server to cancel the request in case
                         * it is still running on the server.
                         */
                        cancel(uuid);
                    } catch (Exception ex) {
                        log.warn(ex);
                    }
                }
            }
        }
        
    }
            
    /**
    * Removes RDF data from the remote repository.
    * 
    * @param remove
    *           The RDF data to be removed.
    * 
    * @return The mutation count.
    * 
    * @see <a href="http://trac.bigdata.com/ticket/1177"> Resource... contexts
    *      not encoded/decoded according to openrdf semantics (REST API) </a>
    */
    public long remove(final RemoveOp remove) throws Exception {
        
        return remove(remove, UUID.randomUUID());

    }
    
    /**
     * Removes RDF data from the remote repository.
     * 
     * @param remove
     *            The RDF data to be removed.
     * 
     * @return The mutation count.
     * 
     * @see <a href="http://trac.bigdata.com/ticket/1177"> Resource... contexts
     *      not encoded/decoded according to openrdf semantics (REST API) </a>
     * @see #1254 / BLZG-1259
     */
    public long remove(final RemoveOp remove, final UUID uuid) throws Exception {

        if (remove == null)
            throw new IllegalArgumentException();

        final ConnectOptions opts = mgr.newUpdateConnectOptions(sparqlEndpointURL, uuid, tx);
        
        remove.prepareForWire();
            
        if (remove.format != null) {
            
            opts.method = "POST";
            opts.addRequestParam("delete");
            
            final ByteArrayEntity entity = new ByteArrayEntity(remove.data);

            entity.setContentType(remove.format.getDefaultMIMEType());
            
            opts.entity = entity;
            
            if (remove.context != null && remove.context.length > 0) {
                // set the default context.
                opts.addRequestParam("context-uri", toStrings(remove.context));
            }
            
        } else {
            
            opts.method = "DELETE";
        
	        if (remove.query != null) {
	            opts.addRequestParam("query", remove.query);
	        }
	        
	        if (remove.s != null) {
	            opts.addRequestParam("s", EncodeDecodeValue.encodeValue(remove.s));
	        }
	        
	        if (remove.p != null) {
	            opts.addRequestParam("p", EncodeDecodeValue.encodeValue(remove.p));
	        }
	        
	        if (remove.o != null) {
	            opts.addRequestParam("o", EncodeDecodeValue.encodeValue(remove.o));
	        }
	     
         if (remove.c != null) {
            /*
             * Note: Due to the way in which the RemoveOp declares [c] even when
             * it is not a "delete-by-accesspath" request, we have to check for
             * [c!=null] here.
             * 
             * TODO This could be fixed if we had a factory for RemoveOp such
             * that the concrete instance only declared [c] when it was a
             * delete-by-access-path request. See #1177
             */
            opts.addRequestParam("c",
                  EncodeDecodeValue.encodeContexts(remove.c));
         }
        
        }
        
        opts.setAcceptHeader(ConnectOptions.MIME_APPLICATION_XML);

        JettyResponseListener response = null;
        boolean ok = false;
        try {
            
            checkResponseCode(response = doConnect(opts));
            
            final MutationResult result = mutationResults(response);
            
            ok = true;
            
            return result.mutationCount;
            
        } finally {
            
            if (response != null) {
                // Abort the http response handling.
                response.abort();
                if (!ok) {
                    try {
                        /*
                         * POST back to the server to cancel the request in case
                         * it is still running on the server.
                         */
                        cancel(uuid);
                    } catch (Exception ex) {
                        log.warn(ex);
                    }
                }
            }
                        
        }
        
    }

    /**
     * Perform an ACID update (delete+insert) per the semantics of <a href=
     * "https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=NanoSparqlServer#UPDATE_.28DELETE_.2B_INSERT.29"
     * > the NanoSparqlServer. </a>
     * <p>
     * Currently, the only combination supported is delete by query with add by
     * post (Iterable<Statement> and File). You can embed statements you want to
     * delete inside a construct query without a where clause.
     * 
     * @param remove
     *            The RDF data to be removed.
     * @param add
     *            The RDF data to be added.
     *            
     * @return The mutation count.
     */
    public long update(final RemoveOp remove, final AddOp add) throws Exception {

        return update(remove, add, UUID.randomUUID());

    }

    /**
     * Perform an ACID update
     * <p>
     * There are two different patterns which are supported:
     * <dl>
     * <dt>UPDATE (DELETE statements selected by a QUERY plus INSERT statements
     * from Request Body using PUT)</dt>
     * <dd>Where query is a CONSTRUCT or DESCRIBE query. <br>
     * Note: The QUERY + DELETE operation is ACID. <br>
     * Note: You MAY specify a CONSTRUCT query with an empty WHERE clause in
     * order to specify a set of statements to be removed without reference to
     * statements already existing in the database.</dd>
     * <dt>UPDATE (POST with Multi-Part Request Body)</dt>
     * <dd>You can specify two sets of serialized statements - one to be removed
     * and one to be added..</dd>
     * </dl>
     * 
     * @param remove
     *            The RDF data to be removed (either a collection of statements
     *            or a CONSTRUCT or DESCRIBE QUERY identifying the data to be
     *            deleted).
     * @param add
     *            The RDF data to be added (must be a collection of statements).
     * 
     * @return The mutation count.
     * @see http
     *      ://wiki.blazegraph.com/wiki/index.php/NanoSparqlServer#UPDATE_.28D
     *      ELETE_.2B_INSERT.29
     */
    public long update(final RemoveOp remove, final AddOp add, final UUID uuid)
            throws Exception {

        if(remove == null)
            throw new IllegalArgumentException();

        if(add == null)
            throw new IllegalArgumentException();

        remove.prepareForWire();
        
        add.prepareForWire();

        final ConnectOptions opts = mgr.newUpdateConnectOptions(
                sparqlEndpointURL, uuid, tx);

        if (remove.format != null) {

            // Code path when caller specifies data to be removed.
            opts.method = "POST";
            opts.addRequestParam("update");

            // Note: Multi-part MIME request entity.
            final MultipartEntity entity = new MultipartEntity();

            // The data to be removed.
            entity.addPart(new FormBodyPart("remove", new ByteArrayBody(
                    remove.data, remove.format.getDefaultMIMEType(), "remove")));

            // The data to be added.
            entity.addPart(new FormBodyPart("add", new ByteArrayBody(add.data,
                    add.format.getDefaultMIMEType(), "add")));

            // The multi-part request entity.
            opts.entity = entity;

        } else {

            // Code path when caller specifies CONSTRUCT or DESCRIBE query
            // identifying the data to be removed.
            opts.method = "PUT";
            
            // QUERY specifying the data to be removed.
            opts.addRequestParam("query", remove.query);

            // The data to be added.
            final ByteArrayEntity entity = new ByteArrayEntity(add.data);
            entity.setContentType(add.format.getDefaultMIMEType());
            opts.entity = entity;

        }

        if (add.context != null) {
            // set the default context for insert.
            opts.addRequestParam("context-uri-insert", toStrings(add.context));
        }
        
        if (remove.context != null) {
            // set the default context for delete.
            opts.addRequestParam("context-uri-delete", toStrings(remove.context));
        }
        
        opts.setAcceptHeader(ConnectOptions.MIME_APPLICATION_XML);
        
        JettyResponseListener response = null;
        boolean ok = false;
        try {

            checkResponseCode(response = doConnect(opts));
            
            final MutationResult result = mutationResults(response);
            
            ok = true;
            
            return result.mutationCount;
            
        } finally {
            
            if (response != null) {
                // Abort the http response handling.
                response.abort();
                if (!ok) {
                    try {
                        /*
                         * POST back to the server to cancel the request in case
                         * it is still running on the server.
                         */
                        cancel(uuid);
                    } catch (Exception ex) {
                        log.warn(ex);
                    }
                }
            }
            
        }
        
    }
    
    /**
     * A prepared query will hold metadata for a particular query instance.
     * <p>
     * Right now, the only metadata is the query ID.
     */
    protected abstract class QueryOrUpdate implements IPreparedOperation, IPreparedQuery {
        
        protected final ConnectOptions opts;
        
        private final UUID uuid;

        protected final String query;

        private final boolean update;

        public QueryOrUpdate(final ConnectOptions opts, final UUID id,
                final String query) {

            this(opts, id, query, false/* update */);

        }

        /**
         * 
         * @param id
         *            The query id.
         * @param query
         *            The SPARQL query or update string.
         * @param update
         *            <code>true</code> iff this is a SPARQL update.
         */
        public QueryOrUpdate(final ConnectOptions opts, final UUID uuid,
                final String query, final boolean update) {

            if (opts == null)
                throw new IllegalArgumentException();
            
            if (query == null)
                throw new IllegalArgumentException();
            
            if (uuid == null)
                throw new IllegalArgumentException();
            
            this.opts = opts;
            this.uuid = uuid;
            this.query = query;
            this.update = update;
            
        }

        @Override
        final public UUID getQueryId() {
            
            return uuid;
            
        }
        
        @Override
        public final boolean isUpdate() {
            
            return update;
            
        }
        
    	public void addRequestParam(String name, String... val) {
    		opts.addRequestParam(name, val);
    	}

        /**
         * Setup the connection options.
         */
        protected void setupConnectOptions() {
            
            opts.method = mgr.getQueryMethod();

            if(update) {
            
                opts.addRequestParam("update", query);
                
            } else {
                
                opts.addRequestParam("query", query);
                
            }

            final UUID queryId = getQueryId();

            if (queryId != null)
                opts.addRequestParam(QUERYID, queryId.toString());

        }
        
        @Override
        public void setAcceptHeader(final String value) {
            
            opts.setAcceptHeader(value);
            
        }
        
        @Override
        public void setHeader(final String name, final String value) {

            opts.setHeader(name, value);
            
        }
        
        @Override
        public void setMaxQueryMillis(final long timeout) {
            
            opts.setHeader(HTTP_HEADER_BIGDATA_MAX_QUERY_MILLIS,
                    Long.toString(timeout));
            
        }

        /**
         * {@inheritDoc}
         * <p>
         * Note: <code>-1L</code> is returned if the http header is not
         * specified.
         */
        @Override
        public long getMaxQueryMillis() {

            final String s = opts
                    .getHeader(HTTP_HEADER_BIGDATA_MAX_QUERY_MILLIS);

            if (s == null) {

                return -1L;

            }

            return StringUtil.toLong(s);
            
        }
        
        @Override
        public String getHeader(final String name) {
            
            return opts.getHeader(name);
            
        }
        
    }

    private final class TupleQuery extends QueryOrUpdate implements IPreparedTupleQuery {
        
        public TupleQuery(final ConnectOptions opts, final UUID id,
                final String query) {

            super(opts, id, query);

        }

        @Override
        protected void setupConnectOptions() {

            super.setupConnectOptions();

            if (opts.getAcceptHeader() == null)
                opts.setAcceptHeader(ConnectOptions.DEFAULT_SOLUTIONS_ACCEPT_HEADER);

        }

        @Override
        public TupleQueryResult evaluate() throws Exception {
            
            return evaluate(null);
                
        }
        
        @Override
        public TupleQueryResult evaluate(final IPreparedQueryListener listener) 
                throws Exception {
            
            setupConnectOptions();

            return mgr.tupleResults(opts, getQueryId(), listener);
                
        }
        
    }

    private final class GraphQuery extends QueryOrUpdate implements IPreparedGraphQuery {

        public GraphQuery(final ConnectOptions opts, final UUID id,
                final String query) {

            super(opts, id, query);

        }

        @Override
        protected void setupConnectOptions() {

            super.setupConnectOptions();

            if (opts.getAcceptHeader() == null)
                opts.setAcceptHeader(ConnectOptions.DEFAULT_GRAPH_ACCEPT_HEADER);

        }

        @Override
        public GraphQueryResult evaluate() throws Exception {

            return evaluate(null);

        }

        @Override
        public GraphQueryResult evaluate(final IPreparedQueryListener listener) 
                throws Exception {

            setupConnectOptions();
            
            return mgr.graphResults(opts, getQueryId(), listener);

        }

    }

    private final class BooleanQuery extends QueryOrUpdate implements
            IPreparedBooleanQuery {
        
        public BooleanQuery(final ConnectOptions opts, final UUID id,
                final String query) {
        
            super(opts, id, query);
            
        }
        

        @Override
        protected void setupConnectOptions() {

            super.setupConnectOptions();

            if (opts.getAcceptHeader() == null)
                opts.setAcceptHeader(ConnectOptions.DEFAULT_BOOLEAN_ACCEPT_HEADER);

        }
        
        @Override
        public boolean evaluate() throws Exception {
           
            return evaluate(null);
            
        }

        @Override
        public boolean evaluate(final IPreparedQueryListener listener) 
                throws Exception {
           
            setupConnectOptions();
            
            return mgr.booleanResults(opts, getQueryId(), listener);

        }
        
    }

    private final class SparqlUpdate extends QueryOrUpdate implements
            IPreparedSparqlUpdate {
        
        public SparqlUpdate(final ConnectOptions opts, final UUID uuid,
                final String updateStr) {

            super(opts, uuid, updateStr, true/*update*/);

        }
        
        @Override
        public void evaluate() throws Exception {
            
            evaluate(null);
            
        }
         
        @Override
        public void evaluate(final IPreparedQueryListener listener) 
                throws Exception {
         
            setupConnectOptions();

            mgr.sparqlUpdateResults(opts, getQueryId(), listener);
            
        }
        
    }
   
    /**
     * Add by URI, statements, or file.
     */
    public static class AddOp {

        private List<String> uris;
        private Iterable<? extends Statement> stmts;
        private byte[] data;
        private File file;
        private InputStream is;
        private Reader reader;
        private RDFFormat format;
        private Resource[] context;
        
        public AddOp(final String uri) {
            if (uri == null)
                throw new IllegalArgumentException();
            this.uris = Collections.singletonList(uri);
        }

        public AddOp(final Collection<String> uris) {
            if (uris == null)
                throw new IllegalArgumentException();
            if (uris.isEmpty())
                throw new IllegalArgumentException();
            this.uris = new LinkedList<String>(uris);
        }

        public AddOp(final Iterable<? extends Statement> stmts) {
            this.stmts = stmts;
        }
        
        public AddOp(final File file, final RDFFormat format) {
            this.file = file;
            this.format = format;
        }
        
        public AddOp(final InputStream is, final RDFFormat format) {
            this.is = is;
            this.format = format;
        }
        
        public AddOp(final Reader reader, final RDFFormat format) {
            this.reader = reader;
            this.format = format;
        }
        
        /**
         * This ctor is for the test cases.
         */
        public AddOp(final byte[] data, final RDFFormat format) {
            this.data = data;
            this.format = format;
        }
        
        public void setContext(final Resource... context) {
            this.context = context;
        }
        
        private void prepareForWire() throws Exception {
            
            if (file != null) {

                // set the data
                data = IOUtil.readBytes(file);
                
            } else if (is != null) {

                // set the data
                data = IOUtil.readBytes(is);
                
            } else if (reader != null) {

                // set the data
                data = IOUtil.readString(reader).getBytes();
                
            } else if (stmts != null) {
                
                // set the data and content type (TRIG by default)
                format = RDFFormat.TRIG;
                data = serialize(stmts, format);

            }
            
        }
        
    }
    
    /**
     * Remove by query, access path, statements, or file.
     */
    public static class RemoveOp {
        
        private String query;
        
        private Iterable<? extends Statement> stmts;
        
        private Value s, p, o;
        
        private Resource[] c;
        
        private byte[] data;
        
        private File file;
        
        private RDFFormat format;
        
        private Resource[] context;
        
        public RemoveOp(final String query) {
            this.query = query;
        }
        
        public RemoveOp(final Iterable<? extends Statement> stmts) {
            this.stmts = stmts;
        }

      /**
       * 
       * @param s
       *           The subject (optional).
       * @param p
       *           The predicate (optional).
       * @param o
       *           The value (optional).
       * @param c
       *           The contexts (optional, BUT may not be a null Resource[]).
       */
        public RemoveOp(final Resource s, final URI p, final Value o, final Resource... c) {
            if (c == null)
               throw new IllegalArgumentException();
            this.s = s;
            this.p = p;
            this.o = o;
            this.c = c;
        }

        public RemoveOp(final File file, final RDFFormat format) {
            this.file = file;
            this.format = format;
        }
        
        /**
         * This ctor is for the test cases.
         */
        public RemoveOp(final byte[] data, final RDFFormat format) {
            this.data = data;
            this.format = format;
        }
                    
        public void setContext(final Resource... context) {
            this.context = context;
        }
        
        private void prepareForWire() throws Exception {
            
            if (file != null) {

                // set the data
                data = IOUtil.readBytes(file);
                
            } else if (stmts != null) {
                
                // set the data and content type (TRIG by default)
                format = RDFFormat.TRIG;
                data = serialize(stmts, format);

            }
            
        }
        
    }
    
    /**
     * Connect to a SPARQL end point (GET or POST query only).
     * 
     * @param opts
     *            The connection options.
     * 
     * @return The connection.
     */
    private JettyResponseListener doConnect(final ConnectOptions opts) throws Exception {
    
       return mgr.doConnect(opts);
       
    }
        
}

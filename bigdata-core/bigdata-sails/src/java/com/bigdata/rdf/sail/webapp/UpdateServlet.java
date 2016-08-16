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
import java.io.InputStream;
import java.io.PipedOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;

import com.bigdata.journal.ITx;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext.AbstractQueryTask;
import com.bigdata.rdf.sail.webapp.DeleteServlet.BufferStatementHandler;
import com.bigdata.rdf.sail.webapp.DeleteServlet.RemoveStatementHandler;
import com.bigdata.rdf.sail.webapp.InsertServlet.AddStatementHandler;
import com.bigdata.rdf.sail.webapp.client.MiniMime;
import com.bigdata.rdf.sparql.ast.ASTContainer;

/**
 * Handler for NanoSparqlServer REST API UPDATE operations (PUT, not SPARQL
 * UPDATE).
 * 
 * @author martyncutcher
 */
public class UpdateServlet extends BigdataRDFServlet {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    static private final transient Logger log = Logger
            .getLogger(UpdateServlet.class);

    /*
     * Note: includedInferred is false because inferences can
     * not be deleted (they are retracted by truth maintenance
     * when they can no longer be proven).
     */
    private static final boolean includeInferred = false;

    public UpdateServlet() {

    }

    @Override
    protected void doPut(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        if (!isWritable(getServletContext(), req, resp)) {
            // Service must be writable.
            return;
        }

        final String queryStr = req.getParameter(QueryServlet.ATTR_QUERY);

        final String contentType = req.getContentType();

        if (contentType == null) {

            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);

        }

        if (queryStr == null) {

            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);

        }

        doUpdateWithQuery(req, resp);

    }

    /**
	 * Delete all statements materialized by a DESCRIBE or CONSTRUCT query and
	 * then insert all statements in the request body.
	 * <p>
	 * Note: To avoid materializing the statements, this runs the query against
	 * the last commit time and uses a pipe to connect the query directly to the
	 * process deleting the statements. This is done while it is holding the
	 * unisolated connection which prevents concurrent modifications. Therefore
	 * the entire <code>SELECT + DELETE</code> operation is ACID.
	 */
    private void doUpdateWithQuery(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        final String baseURI = req.getRequestURL().toString();

        final String namespace = getNamespace(req);

        final String queryStr = req.getParameter(QueryServlet.ATTR_QUERY);
        
        final boolean suppressTruthMaintenance = getBooleanValue(req, QueryServlet.ATTR_TRUTH_MAINTENANCE, false);
        
        if (queryStr == null)
            buildAndCommitResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                    "Required parameter not found: " + QueryServlet.ATTR_QUERY);

        final Map<String, Value> bindings = parseBindings(req, resp);
        if (bindings == null) { // invalid bindings definition generated error response 400 while parsing
            return;
        }
        
        final String contentType = req.getContentType();

        if (log.isInfoEnabled())
            log.info("Request body: " + contentType);

        /**
         * <a href="https://sourceforge.net/apps/trac/bigdata/ticket/620">
         * UpdateServlet fails to parse MIMEType when doing conneg. </a>
         */

        final RDFFormat requestBodyFormat = RDFFormat.forMIMEType(new MiniMime(
                contentType).getMimeType());

        if (requestBodyFormat == null) {

            buildAndCommitResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                    "Content-Type not recognized as RDF: " + contentType);

            return;

        }

        final RDFParserFactory rdfParserFactory = RDFParserRegistry
                .getInstance().get(requestBodyFormat);

        if (rdfParserFactory == null) {

            buildAndCommitResponse(resp, HTTP_INTERNALERROR, MIME_TEXT_PLAIN,
                    "Parser factory not found: Content-Type="
                            + contentType + ", format=" + requestBodyFormat);
            
            return;

        }

        /*
         * Allow the caller to specify the default context for insert.
         */
        final Resource[] defaultContextInsert;
        {
            final String[] s = req.getParameterValues("context-uri-insert");
            if (s != null && s.length > 0) {
                try {
                    defaultContextInsert = toURIs(s);
                } catch (IllegalArgumentException ex) {
                    buildAndCommitResponse(resp, HTTP_INTERNALERROR, MIME_TEXT_PLAIN,
                            ex.getLocalizedMessage());
                    return;
                }
            } else {
                defaultContextInsert = null;
            }
        }

        /*
         * Allow the caller to specify the default context for delete.
         */
        final Resource[] defaultContextDelete;
        {
            final String[] s = req.getParameterValues("context-uri-delete");
            if (s != null && s.length > 0) {
                try {
                	defaultContextDelete = toURIs(s);
                } catch (IllegalArgumentException ex) {
                    buildAndCommitResponse(resp, HTTP_INTERNALERROR, MIME_TEXT_PLAIN,
                            ex.getLocalizedMessage());
                    return;
                }
            } else {
            	defaultContextDelete = null;
            }
        }
        
		try {

		   if(getIndexManager().isGroupCommit()) {

		      // compatible with group commit serializability semantics.
		      submitApiTask(
					new UpdateWithQueryMaterializedTask(req, resp, namespace,
							ITx.UNISOLATED, //
							queryStr,//
							baseURI,//
							suppressTruthMaintenance, //
							bindings,//
							rdfParserFactory,//
							defaultContextDelete,//
							defaultContextInsert//
					)).get();

		   } else {

		      // streaming implementation. not compatible with group commit.
		      submitApiTask(
	               new UpdateWithQueryStreamingTask(req, resp, namespace,
	                     ITx.UNISOLATED, //
	                     queryStr,//
	                     baseURI,//
	                     suppressTruthMaintenance, //
	                     bindings,//
	                     rdfParserFactory,//
	                     defaultContextDelete,//
	                     defaultContextInsert//
	               )).get();
		      
		   }
		   
		} catch (Throwable t) {

			launderThrowable(
					t,
					resp,
					"UPDATE-WITH-QUERY"
							+ ": queryStr="
							+ queryStr
							+ ", baseURI="
							+ baseURI
							+ (defaultContextInsert == null ? ""
									: ",context-uri-insert="
											+ Arrays.toString(defaultContextInsert))
							+ (defaultContextDelete == null ? ""
									: ",context-uri-delete="
											+ Arrays.toString(defaultContextDelete)));

		}

    }

    /**
    * Streaming version is more scalable but is not compatible with group
    * commit. The underlying issue is the serializability of the mutation
    * operations. This task reads from the last commit time and writes on the
    * unisolated indices. It holds the lock on the unisolated indices while
    * doing this. However, if group commit is enabled, then it might not observe
    * mutations which may have been applied since the last commit which breaks
    * the serializability guarantee.
    * 
    * @author bryan
    * 
    */
    private static class UpdateWithQueryStreamingTask extends AbstractRestApiTask<Void> {

    	private final String queryStr;
        private final String baseURI;
        private final boolean suppressTruthMaintenance;
        private final RDFParserFactory parserFactory;
        private final Resource[] defaultContextDelete;
        private final Resource[] defaultContextInsert;
        private final Map<String, Value> bindings;

        /**
         * 
         * @param namespace
         *            The namespace of the target KB instance.
         * @param timestamp
         *            The timestamp used to obtain a mutable connection.
         * @param baseURI
         *            The base URI for the operation.
         * @param defaultContextDelete
         *            When removing statements, the context(s) for triples
         *            without an explicit named graph when the KB instance is
         *            operating in a quads mode.
         * @param defaultContextInsert
         *            When inserting statements, the context(s) for triples
         *            without an explicit named graph when the KB instance is
         *            operating in a quads mode.
         */
        public UpdateWithQueryStreamingTask(final HttpServletRequest req,
                final HttpServletResponse resp,
                final String namespace, final long timestamp,
                final String queryStr,//
                final String baseURI,
                final boolean suppressTruthMaintenance,//
                final Map<String, Value> bindings,
                final RDFParserFactory parserFactory,
                final Resource[] defaultContextDelete,//
                final Resource[] defaultContextInsert//
                ) {
            super(req, resp, namespace, timestamp);
            this.queryStr = queryStr;
            this.baseURI = baseURI;
            this.suppressTruthMaintenance = suppressTruthMaintenance;
            this.bindings = bindings;
            this.parserFactory = parserFactory;
            this.defaultContextDelete = defaultContextDelete;
            this.defaultContextInsert = defaultContextInsert;
        }
        
        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public Void call() throws Exception {

            final long begin = System.currentTimeMillis();
            
            final AtomicLong nmodified = new AtomicLong(0L);

            /*
             * Parse the query before obtaining the connection object.
             * 
             * @see BLZG-2039 SPARQL QUERY and SPARQL UPDATE should be parsed
             * before obtaining the connection
             */
            
            // Setup the baseURI for this request. 
            final String baseURI = BigdataRDFContext.getBaseURI(req, resp);

            // Parse the query.
            final ASTContainer astContainer = new Bigdata2ASTSPARQLParser().parseQuery2(queryStr, baseURI);

            BigdataSailRepositoryConnection repoConn = null;
            BigdataSailConnection conn = null;
            boolean success = false;
            try {
            	
            	repoConn = getConnection();
        
				conn = repoConn.getSailConnection();
				
				boolean truthMaintenance = conn.getTruthMaintenance();
				
				if(truthMaintenance && suppressTruthMaintenance){
					
					conn.setTruthMaintenance(false);
					
				}

				{

					if (log.isInfoEnabled())
						log.info("update with query: " + queryStr);

					final BigdataRDFContext context = BigdataServlet
							.getBigdataRDFContext(req.getServletContext());

					/*
					 * Note: pipe is drained by this thread to consume the query
					 * results, which are the statements to be deleted.
					 */
					final PipedOutputStream os = new PipedOutputStream();

					// The read-only connection for the query.
					BigdataSailRepositoryConnection roconn = null;
					try {
						
						final long readOnlyTimestamp = ITx.READ_COMMITTED;

						roconn = getQueryConnection(namespace,
								readOnlyTimestamp);

						// Use this format for the query results.
						final RDFFormat deleteQueryFormat = RDFFormat.NTRIPLES;

						final AbstractQueryTask queryTask = context
								.getQueryTask(roconn, namespace,
										readOnlyTimestamp, queryStr, baseURI, astContainer, includeInferred, bindings,
										deleteQueryFormat.getDefaultMIMEType(),
										req, resp, os);

						switch (queryTask.queryType) {
						case DESCRIBE:
						case CONSTRUCT:
							break;
						default:
							throw new MalformedQueryException(
									"Must be DESCRIBE or CONSTRUCT query");
						}

						// Run DELETE
						{

							final RDFParserFactory factory = RDFParserRegistry
									.getInstance().get(deleteQueryFormat);

							final RDFParser rdfParser = factory.getParser();

							rdfParser.setValueFactory(conn.getTripleStore()
									.getValueFactory());

							rdfParser.setVerifyData(false);

							rdfParser.setStopAtFirstError(true);

							rdfParser
									.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

							rdfParser.setRDFHandler(new RemoveStatementHandler(conn,
									nmodified, defaultContextDelete));

							// Wrap as Future.
							final FutureTask<Void> ft = new FutureTask<Void>(
									queryTask);

							// Submit query for evaluation.
							context.queryService.execute(ft);

							// Reads on the statements produced by the query.
							final InputStream is = newPipedInputStream(os);

							// Run parser : visited statements will be deleted.
							rdfParser.parse(is, baseURI);

							// Await the Future (of the Query)
							ft.get();
							
						}

						// Run INSERT
						{

							/*
							 * There is a request body, so let's try and parse
							 * it.
							 */

							final RDFParser rdfParser = parserFactory
									.getParser();

							rdfParser.setValueFactory(conn.getTripleStore()
									.getValueFactory());

							rdfParser.setVerifyData(true);

							rdfParser.setStopAtFirstError(true);

							rdfParser
									.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

							rdfParser.setRDFHandler(new AddStatementHandler(
									conn, nmodified,
									defaultContextInsert));

							/*
							 * Run the parser, which will cause statements to be
							 * inserted.
							 */
							rdfParser.parse(req.getInputStream(), baseURI);

						}

					} finally {
				
						if (roconn != null) {
							// close the read-only connection for the query.
							roconn.rollback();
						}
						
					}

				}
				
				if (truthMaintenance && suppressTruthMaintenance) {
					
					conn.setTruthMaintenance(true);

				}

                conn.commit();

                success = true;
                
                final long elapsed = System.currentTimeMillis() - begin;

                reportModifiedCount(nmodified.get(), elapsed);

                return null;
                
            } finally {
                
                if (conn != null) {

                    if (!success)
                        conn.rollback();

                    conn.close();

                }
                
                if (repoConn != null) {

                   repoConn.close();

                }
                
            }

        }

    } // class UpdateWithQueryStreamingTask

    /**
    * This version runs the query against the unisolated connection, fully
    * buffers the statements to be removed, and then removes them. Since both
    * the read and the write are on the unisolated connection, it see all
    * mutations that have been applied since the last group commit and preserves
    * the serializability guarantee.
    * 
    * @author bryan
    * 
    */
   private static class UpdateWithQueryMaterializedTask extends
         AbstractRestApiTask<Void> {

      private final String queryStr;
      private final String baseURI;
      private final boolean suppressTruthMaintenance;
      private final RDFParserFactory parserFactory;
      private final Resource[] defaultContextDelete;
      private final Resource[] defaultContextInsert;
      private final Map<String, Value> bindings;

      /**
       * 
       * @param namespace
       *           The namespace of the target KB instance.
       * @param timestamp
       *           The timestamp used to obtain a mutable connection.
       * @param baseURI
       *           The base URI for the operation.
       * @param bindings 
       * @param defaultContextDelete
       *           When removing statements, the context(s) for triples without
       *           an explicit named graph when the KB instance is operating in
       *           a quads mode.
       * @param defaultContextInsert
       *           When inserting statements, the context(s) for triples without
       *           an explicit named graph when the KB instance is operating in
       *           a quads mode.
       */
      public UpdateWithQueryMaterializedTask(final HttpServletRequest req,
            final HttpServletResponse resp, final String namespace,
            final long timestamp,
            final String queryStr,//
            final String baseURI,
            final boolean suppressTruthMaintenance, //
            final Map<String, Value> bindings,
            final RDFParserFactory parserFactory,
            final Resource[] defaultContextDelete,//
            final Resource[] defaultContextInsert//
      ) {
         super(req, resp, namespace, timestamp);
         this.queryStr = queryStr;
         this.baseURI = baseURI;
         this.suppressTruthMaintenance = suppressTruthMaintenance;
         this.bindings = bindings;
         this.parserFactory = parserFactory;
         this.defaultContextDelete = defaultContextDelete;
         this.defaultContextInsert = defaultContextInsert;
      }

      @Override
      public boolean isReadOnly() {
         return false;
      }

      @Override
      public Void call() throws Exception {

         final long begin = System.currentTimeMillis();

         final AtomicLong nmodified = new AtomicLong(0L);

         /*
          * Parse the query before obtaining the connection object.
          * 
          * @see BLZG-2039 SPARQL QUERY and SPARQL UPDATE should be parsed
          * before obtaining the connection
          */
         
         // Setup the baseURI for this request. 
         final String baseURI = BigdataRDFContext.getBaseURI(req, resp);

         // Parse the query.
         final ASTContainer astContainer = new Bigdata2ASTSPARQLParser().parseQuery2(queryStr, baseURI);

         BigdataSailRepositoryConnection repoConn = null;
         BigdataSailConnection conn = null;
         boolean success = false;
         try {
        	 
        	repoConn = getConnection(); 

            conn = repoConn.getSailConnection();
            
            boolean truthMaintenance = conn.getTruthMaintenance();
            
            if(truthMaintenance && suppressTruthMaintenance){
            	
            	conn.setTruthMaintenance(false);
            	
            }
             
            BigdataSailRepositoryConnection roconn = null;
            try {
            	
            	final long readOnlyTimestamp = ITx.READ_COMMITTED;

				roconn = getQueryConnection(namespace,
						readOnlyTimestamp);

	            {
	            	
	               if (log.isInfoEnabled())
	                  log.info("update with query: " + queryStr);
	
	               final BigdataRDFContext context = BigdataServlet
	                     .getBigdataRDFContext(req.getServletContext());
	
	               /*
	                * Note: pipe is drained by this thread to consume the query
	                * results, which are the statements to be deleted.
	                */
	               final PipedOutputStream os = new PipedOutputStream();
	               
	                // Use this format for the query results.
	               final RDFFormat deleteQueryFormat = RDFFormat.NTRIPLES;
	
	               final AbstractQueryTask queryTask = context.getQueryTask(roconn,
	                     namespace, ITx.UNISOLATED, queryStr, baseURI, astContainer, includeInferred, bindings,
	                     deleteQueryFormat.getDefaultMIMEType(), req, resp, os);
	
	               switch (queryTask.queryType) {
	               case DESCRIBE:
	               case CONSTRUCT:
	                  break;
	               default:
	                  throw new MalformedQueryException(
	                        "Must be DESCRIBE or CONSTRUCT query");
	               }
	
	               // Run DELETE
	               {
	
	                  final RDFParserFactory factory = RDFParserRegistry
	                        .getInstance().get(deleteQueryFormat);
	
	                  final RDFParser rdfParser = factory.getParser();
	
	                  rdfParser.setValueFactory(conn.getTripleStore()
	                        .getValueFactory());
	
	                  rdfParser.setVerifyData(false);
	
	                  rdfParser.setStopAtFirstError(true);
	
	                  rdfParser
	                        .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);
	
	//                  rdfParser.setRDFHandler(new BufferStatementHandler(conn
	//                        .getSailConnection(), nmodified, defaultContextDelete));
	
	                  final BufferStatementHandler buffer = new BufferStatementHandler(
	                        conn, nmodified, defaultContextDelete);
	                  
	                  rdfParser.setRDFHandler(buffer);
	
	                  // Wrap as Future.
	                  final FutureTask<Void> ft = new FutureTask<Void>(queryTask);
	
	                  // Submit query for evaluation.
	                  context.queryService.execute(ft);
	
	                  // Reads on the statements produced by the query.
	                  final InputStream is = newPipedInputStream(os);
	
	                  // Run parser : visited statements will be buffered.
	                  rdfParser.parse(is, baseURI);
	
	                  // Await the Future (of the Query)
	                  ft.get();
	
	                  // Delete the buffered statements.
	                  buffer.removeAll();
	                  	
	               }
	
	               // Run INSERT
	               {
	
	                  /*
	                   * There is a request body, so let's try and parse it.
	                   */
	
	                  final RDFParser rdfParser = parserFactory.getParser();
	
	                  rdfParser.setValueFactory(conn.getTripleStore()
	                        .getValueFactory());
	
	                  rdfParser.setVerifyData(true);
	
	                  rdfParser.setStopAtFirstError(true);
	
	                  rdfParser
	                        .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);
	
	                  rdfParser.setRDFHandler(new AddStatementHandler(conn,
	                        nmodified, defaultContextInsert));
	
	                  /*
	                   * Run the parser, which will cause statements to be inserted.
	                   */
	                  rdfParser.parse(req.getInputStream(), baseURI);
	
	               }
	               
	            }

            }finally {
            	
            	if (roconn != null) {
					// close the read-only connection for the query.
					roconn.rollback();
				}
            	
            }
            
            if (truthMaintenance && suppressTruthMaintenance) {
            	
            	conn.setTruthMaintenance(true);
            	
            }
            
         	conn.commit();

            success = true;

            final long elapsed = System.currentTimeMillis() - begin;

            reportModifiedCount(nmodified.get(), elapsed);

            return null;

         } finally {

            if (conn != null) {

               if (!success)
                  conn.rollback();

               conn.close();

            }
            
            if (repoConn != null) {

               repoConn.close();

             }

         }

      }

   } // class UpdateWithQueryMaterializedTask

    @Override
    protected void doPost(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        if (!isWritable(getServletContext(), req, resp)) {
            // Service must be writable.
            return;
        }

        if (ServletFileUpload.isMultipartContent(req)) {
    		
            doUpdateWithBody(req, resp);
    		
        } else {

            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);

        }

    }

    /**
     * UPDATE request with a request body containing the statements to be
     * removed and added as a multi-part mime request.
     */
    private void doUpdateWithBody(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        final DiskFileItemFactory factory = new DiskFileItemFactory();
        
        final ServletFileUpload upload = new ServletFileUpload(factory);
        
        FileItem add = null, remove = null;
        
        try {
        
    	    final List<FileItem> items = upload.parseRequest(req);

            for (FileItem item : items) {

                if (item.getFieldName().equals("add")) {

                    if (!validateItem(resp, add = item)) {
                        return;
                    }

                } else if (item.getFieldName().equals("remove")) {

                    if (!validateItem(resp, remove = item)) {
                        return;
                    }

                }

            }

        } catch (FileUploadException ex) {

            throw new IOException(ex);

        }

        final String baseURI = req.getRequestURL().toString();
        
        final boolean suppressTruthMaintenance = getBooleanValue(req, QueryServlet.ATTR_TRUTH_MAINTENANCE, false);
     
        /*
         * Allow the caller to specify the default context for insert.
         */
        final Resource[] defaultContextInsert;
        {
            final String[] s = req.getParameterValues("context-uri-insert");
            if (s != null && s.length > 0) {
                try {
                    defaultContextInsert = toURIs(s);
                } catch (IllegalArgumentException ex) {
                    buildAndCommitResponse(resp, HTTP_INTERNALERROR, MIME_TEXT_PLAIN,
                            ex.getLocalizedMessage());
                    return;
                }
            } else {
                defaultContextInsert = null;
            }
        }

        /*
         * Allow the caller to specify the default context for delete.
         */
        final Resource[] defaultContextDelete;
        {
            final String[] s = req.getParameterValues("context-uri-delete");
            if (s != null && s.length > 0) {
                try {
                	defaultContextDelete = toURIs(s);
                } catch (IllegalArgumentException ex) {
                    buildAndCommitResponse(resp, HTTP_INTERNALERROR, MIME_TEXT_PLAIN,
                            ex.getLocalizedMessage());
                    return;
                }
            } else {
                defaultContextDelete = null;
            }
        }

        final String namespace = getNamespace(req);

        try {
        
            submitApiTask(
                    new UpdateWithBodyTask(req, resp, namespace,
                            ITx.UNISOLATED, //
                            baseURI,//
                            suppressTruthMaintenance, //
                            remove,//
                            defaultContextDelete,//
                            add,//
                            defaultContextInsert//
                            )).get();

        } catch (Throwable t) {

            launderThrowable(
                    t,
                    resp,
                    "UPDATE-WITH-BODY: baseURI="
                            + baseURI
                            + (add == null ? null
                                    : ", add="
                                            + add
                                            + (defaultContextInsert == null ? ""
                                                    : ",context-uri-insert="
                                                            + Arrays.toString(defaultContextInsert)))
                            + (remove == null ? null
                                    : ", remove="
                                            + remove
                                            + (defaultContextDelete == null ? ""
                                                    : ",context-uri-delete="
                                                            + Arrays.toString(defaultContextDelete))));

        }
        
    }

    private static class UpdateWithBodyTask extends AbstractRestApiTask<Void> {

        private final String baseURI;
        private final boolean suppressTruthMaintenance;
        private final FileItem remove;
        private final FileItem add;
        private final Resource[] defaultContextDelete;
        private final Resource[] defaultContextInsert;

        /**
         * 
         * @param namespace
         *            The namespace of the target KB instance.
         * @param timestamp
         *            The timestamp used to obtain a mutable connection.
         * @param baseURI
         *            The base URI for the operation.
         * @param defaultContextDelete
         *            When removing statements, the context(s) for triples
         *            without an explicit named graph when the KB instance is
         *            operating in a quads mode.
         * @param defaultContextInsert
         *            When inserting statements, the context(s) for triples
         *            without an explicit named graph when the KB instance is
         *            operating in a quads mode.
         */
        public UpdateWithBodyTask(final HttpServletRequest req,
                final HttpServletResponse resp,
                final String namespace, final long timestamp,
                final String baseURI,
                final boolean suppressTruthMaintenance,//
                final FileItem remove,
                final Resource[] defaultContextDelete,//
                final FileItem add,
                final Resource[] defaultContextInsert//
                ) {
            super(req, resp, namespace, timestamp);
            this.baseURI = baseURI;
            this.suppressTruthMaintenance = suppressTruthMaintenance;
            this.remove = remove;
            this.defaultContextDelete = defaultContextDelete;
            this.add = add;
            this.defaultContextInsert = defaultContextInsert;
        }
        
        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public Void call() throws Exception {

            final long begin = System.currentTimeMillis();
            
            final AtomicLong nmodified = new AtomicLong(0L);

            BigdataSailRepositoryConnection repoConn = null;
            BigdataSailConnection conn = null;
            boolean success = false;
            try {
        
                repoConn = getConnection();
            	
            	conn = repoConn.getSailConnection();
                
                boolean truthMaintenance = conn.getTruthMaintenance();
                
                if(truthMaintenance && suppressTruthMaintenance){
                	
                	conn.setTruthMaintenance(false);
                	
                }
                
                    if (remove != null) {

                    final String contentType = remove.getContentType();

                    final InputStream is = remove.getInputStream();

                    final RemoveStatementHandler handler = new RemoveStatementHandler(
                            conn, nmodified,
                            defaultContextDelete);

                    processData(conn, contentType, is, handler, baseURI);
                    
                }

                if (add != null) {

                    final String contentType = add.getContentType();

                    final InputStream is = add.getInputStream();

                    final RDFHandler handler = new AddStatementHandler(
                            conn, nmodified,
                            defaultContextInsert);

                    processData(conn, contentType, is, handler, baseURI);

                }
                
              	if (truthMaintenance && suppressTruthMaintenance) {
              		
              		conn.setTruthMaintenance(true);
              		
              	}

                conn.commit();
                
                success = true;
                
                final long elapsed = System.currentTimeMillis() - begin;

                reportModifiedCount(nmodified.get(), elapsed);

                return null;
                
            } finally {
                
                if (conn != null) {

                    if (!success)
                        conn.rollback();

                    conn.close();

                }
                
                if (repoConn != null) {

                   repoConn.close();

                }
                
            }

        }
        
        private void processData(final BigdataSailConnection conn, 
                final String contentType, 
                final InputStream is, 
                final RDFHandler handler,
                final String baseURI) 
                    throws Exception {
        
            /**
             * Note: The request was already validated.
             * 
             * <a href="https://sourceforge.net/apps/trac/bigdata/ticket/620">
             * UpdateServlet fails to parse MIMEType when doing conneg. </a>
             */

            final RDFFormat format = RDFFormat
                    .forMIMEType(new MiniMime(contentType).getMimeType());

            final RDFParserFactory rdfParserFactory = RDFParserRegistry
                    .getInstance().get(format);

            final RDFParser rdfParser = rdfParserFactory.getParser();

            rdfParser.setValueFactory(conn.getTripleStore()
                    .getValueFactory());

            rdfParser.setVerifyData(true);

            rdfParser.setStopAtFirstError(true);

            rdfParser
                    .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

            rdfParser.setRDFHandler(handler);

            /*
             * Run the parser, which will cause statements to be deleted.
             */
            rdfParser.parse(is, baseURI);

        }
        
    } // class UpdateWithBodyTask

	private boolean validateItem(
			final HttpServletResponse resp, final FileItem item) 
				throws IOException {
		
		final String contentType = item.getContentType();
		
	    if (contentType == null) {
	    	
	        buildAndCommitResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
	                "Content-Type not specified");
	
	        return false;
	        
	    }
	
        final RDFFormat format = RDFFormat
                .forMIMEType(new MiniMime(contentType).getMimeType());

	    if (format == null) {
	
	        buildAndCommitResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
	                "Content-Type not recognized as RDF: " + contentType);
	
	        return false;
	
	    }
	    
        final RDFParserFactory rdfParserFactory = RDFParserRegistry
		        .getInstance().get(format);
		
		if (rdfParserFactory == null) {
		
		    buildAndCommitResponse(resp, HTTP_INTERNALERROR, MIME_TEXT_PLAIN,
		            "Parser factory not found: Content-Type=" + contentType
		                    + ", format=" + format);
		
		    return false;
		
		}

	    if (item.getInputStream() == null) {
	    	
	        buildAndCommitResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
	                "No content");
	
	        return false;
	    	
	    }
	    
	    return true;
		
	}	

}

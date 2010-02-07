/**
Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
package com.bigdata.sail.rest;

import info.aduna.xml.XMLWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.Query;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.rdfxml.RDFXMLWriter;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;

/**
 * A RESTful web service implementation of the {@link GraphRepository} 
 * interface.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
@SuppressWarnings("serial")
public class GraphRepositoryServlet extends HttpServlet {

    /**
     * The logger.
     */
    protected static Logger log = Logger.getLogger(
            GraphRepositoryServlet.class);
    
    /**
     * Flag to log INFO level statements.
     */
    protected static boolean INFO = log.isInfoEnabled();

    /**
     * HTTP request parameter for range (selecting a subset of the graph, i.e.
     * queries).
     */
    static final String HTTP_RANGE = "RANGE";

    /**
     * HTTP request parameter for including inferred statements in reads.
     */
    static final String X_INCLUDE_INFERRED = "X-INCLUDE-INFERRED";

    /**
     * The location of the properties for the bigdata repository.
     */
    public static final String SAIL_PROPERTIES = "sail.properties";
    
    /**
     * The content type (and encoding) used for exchanging RDF/XML with the
     * server.
     */
    public static final String RDF_XML = "application/rdf+xml; charset=UTF-8";

    /**
     * Used to make writes atomic, especially PUT.
     */
    protected static final Object lock = new Object();
    
    /**
     * The bigdata repository.
     */
    private BigdataSailRepository repo;

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        
        try {
            // load properties file
            ServletContext context = config.getServletContext();
            String propsDir = context.getRealPath("/WEB-INF");
            String propsFileName = config.getInitParameter(SAIL_PROPERTIES);
            File propsFile = new File(propsDir, propsFileName);
            Properties sailProps = new Properties();
            sailProps.load(new FileInputStream(propsFile));
            BigdataSail sail = new BigdataSail(sailProps);
            repo = new BigdataSailRepository(sail);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new ServletException(ex);
        }
        
    }

    @Override
    public void destroy() {
        super.destroy();
        try {
            repo.shutDown();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Perform an HTTP GET, which corresponds to the basic CRUD operation "read"
     * according to the generic interaction semantics of HTTP REST.
     * <p>
     * <ul>
     * <li>BODY ignored for READ</li> 
     * <li>no RANGE retrieves the entire graph</li> 
     * <li>RANGE(&lt;query language&gt;[&lt;query&gt;]) performs a query</li>
     * </ul>
     */
    protected void doGet(HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
        
        String range = request.getHeader(HTTP_RANGE);
        boolean includeInferred = Boolean.valueOf(
                request.getHeader(X_INCLUDE_INFERRED));
        
        if (log.isInfoEnabled()) {
            log.info("doGet: " + range);
        }
        
        try {
            
            // set the content type and the _encoding_
            response.setContentType(RDF_XML);
            
            // obtain the writer -- it will use the specified encoding.
            PrintWriter out = response.getWriter();
            String query = null;
            QueryLanguage ql = null;
            int status = 0;
            if (range == null || range.length() == 0) {
                // proper semantics are to provide the entire graph, per above
                status = HttpStatus.SC_OK;
            } else {
                // sparql[select ...]
                final int i = range.indexOf('[');
                ql = QueryLanguage.valueOf(range.substring(0, i));
                if (ql == null) {
                    throw new RuntimeException(
                            "unrecognized query language: " + range);
                }
                query = range.substring(i + 1, range.length() - 1);
                /*
                 MetadataRepositoryHelper.doQuery(graph, query, ql,
                 includeInferred);
                 */
                status = HttpStatus.SC_PARTIAL_CONTENT;
            }

            String results = read(query, ql, includeInferred);
            out.print(results);
            out.flush();
            response.setStatus(status);
        
        } catch (Exception ex) {
            ex.printStackTrace();
            response.sendError(HttpStatus.SC_INTERNAL_SERVER_ERROR, 
                    ex.getMessage());
        }
        
    }

    /**
     * Perform an HTTP-POST, which corresponds to the basic CRUD operation
     * "create" according to the generic interaction semantics of HTTP REST.
     * <p>
     * BODY contains the new RDF/XML content
     * if ( body == null ) -> noop 
     * RANGE ignored for POST
     */
    protected void doPost(HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
        
        // CREATE
        String body = IOUtils.readString(request.getReader());
        try {
            if (body != null && body.length() > 0) {
                create(body);
            }
            response.setStatus(HttpStatus.SC_OK);
        } catch (Exception ex) {
            ex.printStackTrace();
            response.sendError(HttpStatus.SC_INTERNAL_SERVER_ERROR, 
                    ex.getMessage());
        }
        
    }

    /**
     * Perform an HTTP-PUT, which corresponds to the basic CRUD operation
     * "update" according to the generic interaction semantics of HTTP REST.
     * <p>
     * {@see {@link #doDelete(HttpServletRequest, HttpServletResponse)}}
     * {@see {@link #doPost(HttpServletRequest, HttpServletResponse)}}
     */
    protected void doPut(HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
        // PUT needs to get the lock around both writes
        synchronized (lock) {
            // UPDATE
            doDelete(request, response);
            doPost(request, response);
        }
    }

    /**
     * Perform an HTTP-DELETE, which corresponds to the basic CRUD operation
     * "delete" according to the generic interaction semantics of HTTP REST.
     * <p>
     * BODY ignored for DELETE 
     * RANGE of null clears the entire graph
     * RANGE(triples[<rdf/xml>]) deletes the selection specified by the serialized triples
     * RANGE(query[<query language>[<query string>]]) deletes the selection specified by the query
     */
    protected void doDelete(HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
        // DELETE
        String range = request.getHeader("RANGE");
        
        if (log.isInfoEnabled()) {
            log.info("range header: " + range);
        }
        
        try {
            if (range == null || range.length() == 0) {
                clear();
            } else if (range.startsWith("triples[")) {
                // chop off the "triples[" at the beginning
                // and the "]" at the end
                final String rdfXml = range.substring(8, range.length()-1);
                delete(rdfXml);
            } else if (range.startsWith("query[")) {
                // chop off the "query[" at the beginning
                // and the "]" at the end
                range = range.substring(6, range.length() - 1);
                final int i = range.indexOf('[');
                final QueryLanguage ql =
                        QueryLanguage.valueOf(range.substring(0, i));
                if (ql == null) {
                    throw new RuntimeException("unrecognized query language: "
                            + range);
                }
                final String query = range.substring(i + 1, range.length() - 1);
                deleteByQuery(query, ql);
            } else {
                response.sendError(
                                HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE,
                                "unrecognized subgraph selection scheme in range header");
                return;
            }
            response.setStatus(HttpStatus.SC_OK);
        } catch (Exception ex) {
            ex.printStackTrace();
            response.sendError(HttpStatus.SC_INTERNAL_SERVER_ERROR, ex
                    .getMessage());
        }
    }

    public String read(String queryString, QueryLanguage ql, 
            boolean includeInferred) throws Exception {
        
        RepositoryConnection cxn = repo.getQueryConnection();
        
        try {

            if (queryString != null && ql != null) {
                final Query query = cxn.prepareQuery(ql, queryString);
                if (query instanceof GraphQuery) {
                    return executeConstructQuery((GraphQuery) query, 
                            includeInferred);
                } else if (query instanceof TupleQuery) {
                    return executeSelectQuery((TupleQuery) query, 
                            includeInferred);
                }
            }
            
            return new String();

        } finally {
            // close the repository connection
            cxn.close();
        }
        
    }
    
    /**
     * Perform a construct query, including or excluding inferred statements 
     * per the supplied parameter.
     * 
     * @param query
     *            the construct query to execute
     * @param includeInferred
     *            determines whether evaluation results of this query should
     *            include inferred statements
     * @return rdf/xml serialization of the subgraph
     * @throws Exception
     */
    public String executeConstructQuery(GraphQuery query, 
            boolean includeInferred) throws Exception {

        final StringWriter sw = new StringWriter();
        final RDFXMLWriter writer = new RDFXMLWriter(sw);
        writer.startRDF();
        if (query != null) {
            // silly construct queries, can't guarantee distinct results
            final Set<Statement> results = new LinkedHashSet<Statement>();
            // if we set include inferred on the graph query, each dimension
            // in the join is filtered for explicit statements. what we want
            // is for the entire db to be considered in the query, then for
            // explicit statements to be filtered afterwards.
            // graphQuery.setIncludeInferred(includeInferred);
            query.evaluate(
                    new StatementCollector(results, includeInferred));
            for (Statement stmt : results) {
                writer.handleStatement(stmt);
            }
        }
        writer.endRDF();
        return sw.toString();

    }

    /**
     * Perform a select query, including or excluding inferred statements 
     * per the supplied parameter.
     * 
     * @param query
     *            the select query to execute
     * @param includeInferred
     *            determines whether evaluation results of this query should
     *            include inferred statements
     * @return xml serialization of the solutions
     * @throws Exception
     */
    public String executeSelectQuery(TupleQuery query, 
            boolean includeInferred) throws Exception {

        StringWriter writer = new StringWriter();
        query.evaluate(new SPARQLResultsXMLWriter(new XMLWriter(writer)));
        return writer.toString();
        
    }

    /**
     * Add the specified RDF statements to the graph.
     * 
     * @param rdfXml
     *            rdf/xml to add
     * @throws Exception
     */
    public void create(String rdfXml) throws Exception {
        synchronized (lock) {
            final RepositoryConnection cxn = repo.getConnection();
            cxn.setAutoCommit(false);
            try {
                cxn.add(new StringReader(rdfXml), "", RDFFormat.RDFXML);
                cxn.commit();
            } catch (Exception ex) {
                cxn.rollback();
                throw ex;
            } finally {
                cxn.close();
            }
        }
    }

    /**
     * Delete the specified RDF statements from the graph.
     * 
     * @param rdfXml
     *            rdf/xml to delete
     * @throws Exception
     */
    public void delete(String rdfXml) throws Exception {
        // deserialize the statements into a collection enforcing uniqueness
        Collection<Statement> stmts = IOUtils.rdfXmlToStatements(rdfXml);
        if (stmts.size() == 0) {
            return;
        }

        synchronized (lock) {
            final RepositoryConnection cxn = repo.getConnection();
            cxn.setAutoCommit(false);
            try {
                cxn.remove(stmts);
                cxn.commit();
            } catch (Exception ex) {
                cxn.rollback();
                throw ex;
            } finally {
                cxn.close();
            }
        }
    }

    /**
     * Delete the statements resulting from the specified query.
     * 
     * @param query
     *            the construct query
     * @param ql
     *            the query language of the supplied query
     * @throws Exception
     */
    public void deleteByQuery(String query, QueryLanguage ql) throws Exception {
        synchronized (lock) {
            RepositoryConnection cxn = repo.getConnection();
            cxn.setAutoCommit(false);
            
            // silly construct queries, can't guarantee distinct results
            final Set<Statement> results = new LinkedHashSet<Statement>();
            final GraphQuery graphQuery = cxn.prepareGraphQuery(ql, query);
            // if we set include inferred on the graph query, each dimension
            // in the join is filtered for explicit statements. what we want
            // is for the entire db to be considered in the query, then for
            // explicit statements to be filtered afterwards.
            // graphQuery.setIncludeInferred(includeInferred);
            graphQuery.evaluate(new StatementCollector(results, false));
            
            try {
                cxn.remove(results);
                cxn.commit();
            } catch (Exception ex) {
                cxn.rollback();
                throw ex;
            } finally {
                cxn.close();
            }
            
        }
    }
    
    /**
     * Clears the entire graph.
     */
    public void clear() throws Exception {
        synchronized(lock) {
            final RepositoryConnection cxn = repo.getConnection();
            cxn.setAutoCommit(false);
            try {
                cxn.clear();
                cxn.commit();
            } catch (Exception ex) {
                cxn.rollback();
                throw ex;
            } finally {
                cxn.close();
            }
        }
    }
    
    
    private static class StatementCollector extends
        org.openrdf.rio.helpers.StatementCollector {
        
        private boolean includeInferred = true;

        public StatementCollector(Collection<Statement> stmts) {
            this(stmts, true);
        }

        public StatementCollector(Collection<Statement> stmts,
                boolean includeInferred) {
            super(stmts);
            this.includeInferred = includeInferred;
        }

        @Override
        public void handleStatement(Statement stmt) {
            if (includeInferred) {
                super.handleStatement(stmt);
            } else {
                if (stmt instanceof BigdataStatement == false
                        || ((BigdataStatement) stmt).isExplicit()) {
                    super.handleStatement(stmt);
                }
            }
        }
    }
    
}

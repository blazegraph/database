package com.bigdata.rdf.sail;

import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.ParsedBooleanQuery;
import org.openrdf.query.parser.ParsedGraphQuery;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilder;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailBooleanQuery;
import org.openrdf.repository.sail.SailGraphQuery;
import org.openrdf.repository.sail.SailQuery;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.repository.sail.SailTupleQuery;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sail.bench.NanoSparqlClient;
import com.bigdata.rdf.sail.bench.NanoSparqlClient.QueryType;
import com.bigdata.rdf.sail.sparql.BaseDeclProcessor;
import com.bigdata.rdf.sail.sparql.PrefixDeclProcessor;
import com.bigdata.rdf.sail.sparql.StringEscapesProcessor;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;

public class BigdataSailRepositoryConnection extends SailRepositoryConnection {
   
    protected Logger log = Logger.getLogger(BigdataSailRepositoryConnection.class);
   
    public BigdataSailRepositoryConnection(BigdataSailRepository repository,
            SailConnection sailConnection) {
    
        super(repository, sailConnection);
        
    }
    
    /**
     * Overriden to capture query hints from SPARQL queries. Query hints are
     * embedded in query strings as namespaces.  
     * See {@link BD#QUERY_HINTS_NAMESPACE} for more information.
     */
    @Override
    public SailGraphQuery prepareGraphQuery(final QueryLanguage ql,
            final String qs, final String baseURI) 
            throws MalformedQueryException {
        final ParsedGraphQuery parsedQuery = QueryParserUtil.parseGraphQuery(
                ql, qs, baseURI);
        final Properties queryHints = parseQueryHints(ql, qs, baseURI);
        final boolean describe = 
            ql == QueryLanguage.SPARQL &&
            NanoSparqlClient.QueryType.fromQuery(qs) == QueryType.DESCRIBE;
        return new BigdataSailGraphQuery(parsedQuery, this, queryHints, describe);
    }

    /**
     * Overriden to capture query hints from SPARQL queries. Query hints are
     * embedded in query strings as namespaces.  
     * See {@link BD#QUERY_HINTS_NAMESPACE} for more information.
     */
    @Override
    public SailTupleQuery prepareTupleQuery(final QueryLanguage ql,
            final String queryString, final String baseURI) 
            throws MalformedQueryException {
        final ParsedTupleQuery parsedQuery = QueryParserUtil.parseTupleQuery(
                ql, queryString, baseURI);
        final Properties queryHints = parseQueryHints(ql, queryString, baseURI);
        return new BigdataSailTupleQuery(parsedQuery, this, queryHints);
    }

    /**
     * Overriden to capture query hints from SPARQL queries. Query hints are
     * embedded in query strings as namespaces.  
     * See {@link BD#QUERY_HINTS_NAMESPACE} for more information.
     */
    @Override
    public SailBooleanQuery prepareBooleanQuery(final QueryLanguage ql,
            final String queryString, final String baseURI) 
            throws MalformedQueryException {
        final ParsedBooleanQuery parsedQuery = QueryParserUtil
                .parseBooleanQuery(ql, queryString, baseURI);
        final Properties queryHints = parseQueryHints(ql, queryString, baseURI);
        return new BigdataSailBooleanQuery(parsedQuery, this, queryHints);
    }

    /**
     * Overriden to capture query hints from SPARQL queries. Query hints are
     * embedded in query strings as namespaces.  
     * See {@link BD#QUERY_HINTS_NAMESPACE} for more information.
     */
    @Override
    public SailQuery prepareQuery(final QueryLanguage ql, final String qs,
            final String baseURI) 
            throws MalformedQueryException {
        final ParsedQuery parsedQuery = QueryParserUtil.parseQuery(ql, 
                qs, baseURI);
        final Properties queryHints = parseQueryHints(ql, qs, baseURI);
        if (parsedQuery instanceof ParsedTupleQuery) {
            return new BigdataSailTupleQuery(
                    (ParsedTupleQuery) parsedQuery, this, queryHints);
        } else if (parsedQuery instanceof ParsedGraphQuery) {
            final boolean describe = 
                ql == QueryLanguage.SPARQL &&
                NanoSparqlClient.QueryType.fromQuery(qs) == QueryType.DESCRIBE;
            return new BigdataSailGraphQuery(
                    (ParsedGraphQuery) parsedQuery, this, queryHints, describe);
        } else if (parsedQuery instanceof ParsedBooleanQuery) {
            return new BigdataSailBooleanQuery(
                    (ParsedBooleanQuery) parsedQuery, this, queryHints);
        } else {
            throw new RuntimeException("Unexpected query type: "
                    + parsedQuery.getClass());
        }
    }

    /**
     * Note: auto-commit is an EXTREMELY bad idea. Performance will be terrible.
     * The database will swell to an outrageous size. TURN OFF AUTO COMMIT.
     * 
     * @see BigdataSail.Options#ALLOW_AUTO_COMMIT
     */
    @Override
    public void commit() throws RepositoryException {
        
        // auto-commit is heinously inefficient
        if (isAutoCommit() && 
            !((BigdataSailConnection) getSailConnection()).getAllowAutoCommit()) {
            
            throw new RepositoryException("please set autoCommit to false");

        }
        
        super.commit();
        
    }

    /**
     * Flush the statement buffers. The {@link BigdataSailConnection} heavily
     * buffers assertions and retractions. Either a {@link #flush()} or a
     * {@link #commit()} is required before executing any operations directly
     * against the backing {@link AbstractTripleStore} so that the buffered
     * assertions or retractions will be written onto the KB and become visible
     * to other methods. This is not a transaction issue -- just a buffer issue.
     * The public methods on the {@link BigdataSailConnection} all flush the
     * buffers before performing any queries against the underlying
     * {@link AbstractTripleStore}.
     * 
     * @throws RepositoryException
     */
    public void flush() throws RepositoryException {

        try {

            ((BigdataSailConnection) getSailConnection()).flush();

        } catch (Exception ex) {

            throw new RepositoryException(ex);

        }

    }

    /**
     * Return the backing {@link AbstractTripleStore} object. Caution MUST be
     * used when accessing this object as the access goes around the SAIL API.
     */
    public AbstractTripleStore getTripleStore() {

        return ((BigdataSailConnection) getSailConnection()).database;

    }

    /**
     * Computes the closure of the triple store for RDF(S)+ entailments.
     * <p>
     * This computes the closure of the database. This can be used if you do
     * NOT enable truth maintenance and choose instead to load up all of
     * your data first and then compute the closure of the database. Note
     * that some rules may be computed by eager closure while others are
     * computed at query time.
     * <p>
     * Note: If there are already entailments in the database AND you have
     * retracted statements since the last time the closure was computed
     * then you MUST delete all entailments from the database before
     * re-computing the closure.
     * <p>
     * Note: This method does NOT commit the database. See
     * {@link ITripleStore#commit()} and {@link #getTripleStore()}.
     * 
     * @see #removeAllEntailments()
     */
    public void computeClosure() throws RepositoryException {

        try {

            ((BigdataSailConnection) getSailConnection()).computeClosure();

        } catch(Exception ex) {
            
            throw new RepositoryException(ex);
            
        }

    }

    /**
     * Removes all "inferred" statements from the database (does NOT commit the
     * database).
     * 
     * @throws RepositoryException
     */
    public void removeAllEntailments() throws SailException,
            RepositoryException {

        try {

            ((BigdataSailConnection) getSailConnection())
                    .removeAllEntailments();

        } catch (Exception ex) {

            throw new RepositoryException(ex);

        }

    }
    
    /**
     * Parse query hints from a query string.  Query hints are embedded in the 
     * query string via special namespaces.      
     * See {@link BD#QUERY_HINTS_NAMESPACE} for more information.
     */
    private Properties parseQueryHints(QueryLanguage ql, String queryString, 
            String baseURI) 
                throws MalformedQueryException {
        try {
            Properties queryHints = new Properties();
            // currently only supporting SPARQL
            if (ql == QueryLanguage.SPARQL) {
                // the next four lines were taken directly from
                // org.openrdf.query.parser.sparql.SPARQLParser.parseQuery(String queryStr, String baseURI)
                ASTQueryContainer qc = SyntaxTreeBuilder.parseQuery(queryString);
                StringEscapesProcessor.process(qc);
                BaseDeclProcessor.process(qc, baseURI);
                Map<String, String> prefixes = PrefixDeclProcessor.process(qc);
                // iterate the namespaces
                for (Map.Entry<String, String> prefix : prefixes.entrySet()) {
                    // if we see one that matches the magic namespace, try
                    // to parse it
                    if (prefix.getKey().equalsIgnoreCase(BD.QUERY_HINTS_NAMESPACE)) {
                        String hints = prefix.getValue();
                        // has to have a # and it can't be at the end
                        int i = hints.indexOf('#');
                        if (i < 0 || i == hints.length()-1) {
                            throw new MalformedQueryException("bad query hints: " + hints);
                        }
                        hints = hints.substring(i+1);
                        // properties are separated by &
                        StringTokenizer st = new StringTokenizer(hints, "&");
                        while (st.hasMoreTokens()) {
                            String hint = st.nextToken();
                            i = hint.indexOf('=');
                            if (i < 0 || i == hint.length()-1) {
                                throw new MalformedQueryException("bad query hint: " + hint);
                            }
                            String key = hint.substring(0, i);
                            String val = hint.substring(i+1);
                            queryHints.put(key, val);
                        }
                    }
                }
            }
            return queryHints;
        } catch (ParseException e) {
            throw new MalformedQueryException(e.getMessage(), e);
        }
    }

}

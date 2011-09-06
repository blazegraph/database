package com.bigdata.rdf.sail;

import java.util.Properties;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.ParsedBooleanQuery;
import org.openrdf.query.parser.ParsedGraphQuery;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailQuery;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.changesets.IChangeRecord;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sail.sparql.BigdataSPARQLParser;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Class with support for a variety of bigdata specific extensions,
 * <p>
 * {@inheritDoc}
 * 
 * @author mrpersonick
 */
public class BigdataSailRepositoryConnection extends SailRepositoryConnection {
   
//	private static transient final Logger log = Logger
//			.getLogger(BigdataSailRepositoryConnection.class);

	public String toString() {
		return getClass().getName() + "{timestamp="
				+ TimestampUtility.toString(getTripleStore().getTimestamp())
				+ "}";
	}
	
    public BigdataSailRepositoryConnection(BigdataSailRepository repository,
            SailConnection sailConnection) {
    
        super(repository, sailConnection);
        
    }
    
    @Override
    public BigdataSailRepository getRepository() {

    	return (BigdataSailRepository)super.getRepository();
    	
    }
    
    @Override
    public BigdataSailConnection getSailConnection() {

        return (BigdataSailConnection)super.getSailConnection();
        
    }

    @Override
    public BigdataValueFactory getValueFactory() {

        return (BigdataValueFactory) super.getValueFactory();
        
    }

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to capture query hints from SPARQL queries. Query hints are
	 * embedded in query strings as namespaces. See {@link QueryHints#PREFIX}
	 * for more information.
	 */
    @Override
    public BigdataSailGraphQuery prepareGraphQuery(final QueryLanguage ql,
            final String qs, final String baseURI) 
            throws MalformedQueryException {

        final SailQuery sailQuery = prepareQuery(ql, qs, baseURI);
        
        if(sailQuery.getParsedQuery() instanceof ParsedGraphQuery) {
            
            return (BigdataSailGraphQuery) sailQuery;
            
        }

        // Wrong type of query.
        throw new IllegalArgumentException();
        
//		final ParsedGraphQuery parsedQuery = QueryParserUtil.parseGraphQuery(
//				ql, qs, baseURI);
//
//        final Properties queryHints = QueryHintsUtility.parseQueryHints(ql, qs,
//                baseURI);
//
//		final boolean describe = ql == QueryLanguage.SPARQL
//				&& QueryType.fromQuery(qs) == QueryType.DESCRIBE;
//
//		return new BigdataSailGraphQuery(parsedQuery, this, queryHints,
//				describe);

    }

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to capture query hints from SPARQL queries. Query hints are
	 * embedded in query strings as namespaces. See {@link QueryHints#PREFIX}
	 * for more information.
	 */
    @Override
	public BigdataSailTupleQuery prepareTupleQuery(final QueryLanguage ql,
			final String qs, final String baseURI)
			throws MalformedQueryException {

        final SailQuery sailQuery = prepareQuery(ql, qs, baseURI);
        
        if(sailQuery.getParsedQuery() instanceof ParsedTupleQuery) {
            
            return (BigdataSailTupleQuery) sailQuery;
            
        }

        // Wrong type of query.
        throw new IllegalArgumentException();

//		final ParsedTupleQuery parsedQuery = QueryParserUtil.parseTupleQuery(
//				ql, queryString, baseURI);
//
//        final Properties queryHints = QueryHintsUtility.parseQueryHints(ql,
//                queryString, baseURI);
//
//		return new BigdataSailTupleQuery(parsedQuery, this, queryHints);

    }

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to capture query hints from SPARQL queries. Query hints are
	 * embedded in query strings as namespaces. See {@link QueryHints#PREFIX}
	 * for more information.
	 */
    @Override
	public BigdataSailBooleanQuery prepareBooleanQuery(final QueryLanguage ql,
			final String qs, final String baseURI)
			throws MalformedQueryException {

        final SailQuery sailQuery = prepareQuery(ql, qs, baseURI);
        
        if(sailQuery.getParsedQuery() instanceof ParsedBooleanQuery) {
            
            return (BigdataSailBooleanQuery) sailQuery;
            
        }

        // Wrong type of query.
        throw new IllegalArgumentException();

//		final ParsedBooleanQuery parsedQuery = QueryParserUtil
//				.parseBooleanQuery(ql, queryString, baseURI);
//
//        final Properties queryHints = QueryHintsUtility.parseQueryHints(ql,
//                queryString, baseURI);
//
//		return new BigdataSailBooleanQuery(parsedQuery, this, queryHints);

    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to capture query hints from SPARQL queries. Query hints are
     * embedded in query strings as namespaces. See {@link QueryHints#PREFIX}
     * for more information.
     * <p>
     * Note: In order to ensure that all code paths captures this information,
     * all the other "prepare query" methods on this class delegate to this
     * implementation.
     */
	@Override
	public SailQuery prepareQuery(final QueryLanguage ql, final String qs,
			final String baseURI) throws MalformedQueryException {

		final ParsedQuery parsedQuery;
		final Properties queryHints;
		final boolean describe;
		if(QueryLanguage.SPARQL == ql) {
		    /*
		     * Make sure that we go through the overridden SPARQL parser.
		     */
            parsedQuery = new BigdataSPARQLParser().parseQuery(qs, baseURI);
            queryHints = ((IBigdataParsedQuery) parsedQuery).getQueryHints();
            describe = QueryType.DESCRIBE == ((IBigdataParsedQuery) parsedQuery)
                    .getQueryType();
		} else {
		    /*
		     * Not a SPARQL query.
		     */
            parsedQuery = QueryParserUtil.parseQuery(ql, qs, baseURI);
            queryHints = new Properties();
            describe = false;
		}

		if (parsedQuery instanceof ParsedTupleQuery) {

			return new BigdataSailTupleQuery((ParsedTupleQuery) parsedQuery,
					this, queryHints);

		} else if (parsedQuery instanceof ParsedGraphQuery) {

			return new BigdataSailGraphQuery((ParsedGraphQuery) parsedQuery,
					this, queryHints, describe);

		} else if (parsedQuery instanceof ParsedBooleanQuery) {

			return new BigdataSailBooleanQuery(
					(ParsedBooleanQuery) parsedQuery, this, queryHints);

		} else {

			throw new RuntimeException("Unexpected query type: "
					+ parsedQuery.getClass());

		}

	}

    /**
     * Parse a SPARQL query
     * 
     * @param ql
     *            The {@link QueryLanguage}.
     * @param queryStr
     *            The query.
     * @param baseURI
     *            The base URI.
     * 
     * @return The bigdata AST model for that query.
     * 
     * @throws MalformedQueryException
     */
    public BigdataSailQuery prepareNativeSPARQLQuery(final QueryLanguage ql,
            final String queryStr, final String baseURI)
            throws MalformedQueryException {

        if (ql != QueryLanguage.SPARQL)
            throw new UnsupportedOperationException(ql.toString());

        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser(
                getTripleStore()).parseQuery2(queryStr, baseURI);

        switch (queryRoot.getQueryType()) {
        case SELECT:
            return new BigdataSailTupleQuery(queryRoot, this);
        case DESCRIBE:
        case CONSTRUCT:
            return new BigdataSailGraphQuery(queryRoot, this);
        case ASK: {
            return new BigdataSailBooleanQuery(queryRoot, this);
        }
        default:
            throw new RuntimeException("Unknown query type: "
                    + queryRoot.getQueryType());
        }

    }
	   
    /**
     * {@inheritDoc}
     * <p>
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

//    /**
//     * Commit any changes made in the connection, providing detailed feedback
//     * on the change set that occurred as a result of this commit.
//     * <p>
//     * Note: auto-commit is an EXTREMELY bad idea. Performance will be terrible.
//     * The database will swell to an outrageous size. TURN OFF AUTO COMMIT.
//     * 
//     * @see BigdataSail.Options#ALLOW_AUTO_COMMIT
//     */
//    public Iterator<IChangeRecord> commit2() throws RepositoryException {
//        
//        // auto-commit is heinously inefficient
//        if (isAutoCommit() && 
//            !((BigdataSailConnection) getSailConnection()).getAllowAutoCommit()) {
//            
//            throw new RepositoryException("please set autoCommit to false");
//
//        }
//        
//        try {
//            return getSailConnection().commit2();
//        }
//        catch (SailException e) {
//            throw new RepositoryException(e);
//        }
//        
//    }

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
     * Set the change log on the SAIL connection.  See {@link IChangeLog} and
     * {@link IChangeRecord}.
     * 
     * @param log
     *          the change log
     */
    public void setChangeLog(final IChangeLog log) {
        
        getSailConnection().setChangeLog(log);
        
    }

}

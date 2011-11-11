package com.bigdata.rdf.sail;

import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
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
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Class with support for a variety of bigdata specific extensions,
 * <p>
 * {@inheritDoc}
 * 
 * @author mrpersonick
 */
public class BigdataSailRepositoryConnection extends SailRepositoryConnection {
   
	private static transient final Logger log = Logger
			.getLogger(BigdataSailRepositoryConnection.class);

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

    @Override
    public BigdataSailGraphQuery prepareGraphQuery(final QueryLanguage ql,
            final String qs, final String baseURI) 
            throws MalformedQueryException {

        return (BigdataSailGraphQuery) prepareQuery(ql, qs, baseURI);
        
    }

    @Override
	public BigdataSailTupleQuery prepareTupleQuery(final QueryLanguage ql,
			final String qs, final String baseURI)
			throws MalformedQueryException {

        return (BigdataSailTupleQuery) prepareQuery(ql, qs, baseURI);

    }

    @Override
	public BigdataSailBooleanQuery prepareBooleanQuery(final QueryLanguage ql,
			final String qs, final String baseURI)
			throws MalformedQueryException {

        return (BigdataSailBooleanQuery) prepareQuery(ql, qs, baseURI);
        
    }

    @Override
    public SailQuery prepareQuery(final QueryLanguage ql, final String qs,
            final String baseURI) throws MalformedQueryException {

        if (ql == QueryLanguage.SPARQL) {

            return (SailQuery) prepareNativeSPARQLQuery(ql, qs, baseURI);

        }

        throw new MalformedQueryException("Unsupported language: " + ql);

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
     * @throws UnsupportedOperationException
     *             if the query language is not SPARQL.
     */
    public BigdataSailQuery prepareNativeSPARQLQuery(final QueryLanguage ql,
            final String queryStr, final String baseURI)
            throws MalformedQueryException {

        if (ql != QueryLanguage.SPARQL)
            throw new UnsupportedOperationException(ql.toString());

        if (log.isDebugEnabled()) {
        	log.debug(queryStr);
        }
        
        // Flush buffers since we are about to resolve Values to IVs.
        getSailConnection()
                .flushStatementBuffers(true/* assertions */, true/* retractions */);

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(
                getTripleStore()).parseQuery2(queryStr, baseURI);

        final QueryType queryType = astContainer.getOriginalAST()
                .getQueryType();
        
        switch (queryType) {
        case SELECT:
            return new BigdataSailTupleQuery(astContainer, this);
        case DESCRIBE:
        case CONSTRUCT:
            return new BigdataSailGraphQuery(astContainer, this);
        case ASK: {
            return new BigdataSailBooleanQuery(astContainer, this);
        }
        default:
            throw new RuntimeException("Unknown query type: "
                    + queryType);
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

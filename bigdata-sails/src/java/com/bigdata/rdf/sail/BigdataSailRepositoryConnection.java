package com.bigdata.rdf.sail;

import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.ParsedGraphQuery;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailGraphQuery;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.store.AbstractTripleStore;

public class BigdataSailRepositoryConnection extends SailRepositoryConnection {
   
    protected Logger log = Logger.getLogger(BigdataSailRepositoryConnection.class);
   
    public BigdataSailRepositoryConnection(BigdataSailRepository repository,
            SailConnection sailConnection) {
    
        super(repository, sailConnection);
        
    }
    
    @Override
    public SailGraphQuery prepareGraphQuery(QueryLanguage ql, String queryString, String baseURI)
        throws MalformedQueryException
    {
        
        final ParsedGraphQuery parsedQuery = QueryParserUtil.parseGraphQuery(
                ql, queryString, baseURI);
        
        return new BigdataSailGraphQuery(parsedQuery, this);
        
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

}

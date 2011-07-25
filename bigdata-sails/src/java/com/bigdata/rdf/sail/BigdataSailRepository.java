package com.bigdata.rdf.sail;

import java.io.IOException;

import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.store.AbstractTripleStore;

public class BigdataSailRepository extends SailRepository {
   
    public BigdataSailRepository(BigdataSail sail) {
    
        super(sail);
        
    }
    
    public AbstractTripleStore getDatabase() {
        
        return ((BigdataSail) getSail()).getDatabase();
        
    }

    @Override
    public BigdataSail getSail() {
        return (BigdataSail)super.getSail();
    }
    
//    private BigdataSail getBigdataSail() {
//        
//        return (BigdataSail) getSail();
//        
//    }

    @Override
    public BigdataSailRepositoryConnection getConnection() 
            throws RepositoryException {
        
        try {
        
            return new BigdataSailRepositoryConnection(this, 
                getSail().getConnection());
            
        } catch (SailException e) {
            
            throw new RepositoryException(e);
            
        }
        
    }

	/**
	 * Obtain a read-only connection to the database at the last commit point. A
	 * read-only connection should be used for all pure-readers, as the
	 * connection will not be blocked by concurrent writers.
	 * 
	 * @return a read-only connection to the database
	 * 
	 * @see BigdataSail#getReadOnlyConnection()
	 */
    public BigdataSailRepositoryConnection getReadOnlyConnection() 
        throws RepositoryException {
        
        return new BigdataSailRepositoryConnection(this, 
            getSail().getReadOnlyConnection());
    }

	/**
	 * Obtain a read-only connection to the database from a historical commit
	 * point. A read-only connection should be used for all pure-readers, as the
	 * connection will not be blocked by concurrent writers.
	 * 
	 * @return a read-only connection to the database
	 * 
	 * @see BigdataSail#getReadOnlyConnection(long)
	 */
    public BigdataSailRepositoryConnection getReadOnlyConnection(long timestamp) 
        throws RepositoryException {
        
        return new BigdataSailRepositoryConnection(this, 
            getSail().getReadOnlyConnection(timestamp));
        
    }

	/**
	 * Return a connection backed by a read-write transaction.
	 * 
	 * @see BigdataSail#getReadWriteConnection()
	 */
    public BigdataSailRepositoryConnection getReadWriteConnection() 
        throws RepositoryException {
        
        try {
            
            return new BigdataSailRepositoryConnection(this, 
                getSail().getReadWriteConnection());
            
        } catch (IOException e) {
            
            throw new RepositoryException(e);
            
        }
        
    }
    
    /**
     * Return an unisolated connection to the database.  Only one of these
     * allowed at a time.
     * 
     * @return unisolated connection to the database
     * 
     * @see BigdataSail#getUnisolatedConnection()
     */
    public BigdataSailRepositoryConnection getUnisolatedConnection() 
        throws RepositoryException {
        
        try {
            
            return new BigdataSailRepositoryConnection(this, 
                getSail().getUnisolatedConnection());
            
        } catch (InterruptedException e) {
            
            throw new RepositoryException(e);
            
        }
        
    }

}

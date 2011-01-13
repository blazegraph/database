package com.bigdata.rdf.sail;

import java.io.IOException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
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
     * Obtain a read-only connection to the database at the last commit point.
     * This connection should be used for all pure-readers, as the connection
     * will not be blocked by concurrent writers.
     * 
     * @return a read-only connection to the database
     */
    public BigdataSailRepositoryConnection getReadOnlyConnection() 
        throws RepositoryException {
        
        return new BigdataSailRepositoryConnection(this, 
            getSail().getReadOnlyConnection());
    }
    
    /**
     * Obtain a read-only connection to the database from a historical commit 
     * point. This connection should be used for all pure-readers, as the 
     * connection will not be blocked by concurrent writers.
     * 
     * @return a read-only connection to the database
     */
    public BigdataSailRepositoryConnection getReadOnlyConnection(long timestamp) 
        throws RepositoryException {
        
        return new BigdataSailRepositoryConnection(this, 
            getSail().getReadOnlyConnection(timestamp));
        
    }
    
    public BigdataSailRepositoryConnection getReadWriteConnection() 
        throws RepositoryException {
        
        try {
            
            return new BigdataSailRepositoryConnection(this, 
                getSail().getReadWriteConnection());
            
        } catch (IOException e) {
            
            throw new RepositoryException(e);
            
        }
        
    }
    
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

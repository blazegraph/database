package com.bigdata.rdf.sail;

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
    
    private BigdataSail getBigdataSail() {
        
        return (BigdataSail) getSail();
        
    }

    @Override
    public SailRepositoryConnection getConnection() throws RepositoryException {
        
        try {
        
            return new BigdataSailRepositoryConnection(this, 
                getSail().getConnection());
            
        } catch (SailException e) {
            
            throw new RepositoryException(e);
            
        }
        
    }
    
    public SailRepositoryConnection getQueryConnection() throws RepositoryException {
        
        try {
            
            return new BigdataSailRepositoryConnection(this, 
                getBigdataSail().getQueryConnection());
            
        } catch (SailException e) {
            
            throw new RepositoryException(e);
            
        }
        
    }
    
}

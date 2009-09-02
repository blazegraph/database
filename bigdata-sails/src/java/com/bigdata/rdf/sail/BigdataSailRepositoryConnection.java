package com.bigdata.rdf.sail;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.ParsedGraphQuery;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailGraphQuery;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.SailConnection;

import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;

public class BigdataSailRepositoryConnection extends SailRepositoryConnection {
   
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

    @Override
    public void commit() throws RepositoryException {
        
        // auto-commit is heinously inefficient
        if (isAutoCommit()) {
            
            throw new RuntimeException(
                    "auto-commit not supported, please setAutoCommit(false)");
            
        }
        
        super.commit();
    }
    
    public void computeClosure() throws RepositoryException {
        
        try {
        
            ((BigdataSailConnection)getSailConnection()).computeClosure();
            
        } catch(Exception ex) {
            
            throw new RepositoryException(ex);
            
        }
        
    }
    

    
}

package com.bigdata.rdf.sail;

import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.SailException;

public class BigdataSailRepository extends SailRepository {
    public BigdataSailRepository(BigdataSail sail) {
        super(sail);
    }

    @Override
    public SailRepositoryConnection getConnection() throws RepositoryException {
        try {
            return new BigdataSailRepositoryConnection(this, getSail()
                    .getConnection());
        } catch (SailException e) {
            throw new RepositoryException(e);
        }
    }
    
}

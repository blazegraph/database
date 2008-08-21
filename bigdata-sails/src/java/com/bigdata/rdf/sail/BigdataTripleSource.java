package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * TripleSource API - used by high-level query.
 * <p>
 * Note: We need to use a class for this since the behavior is parameterized
 * based on whether or not inferred statements are to be included in the query
 * results.
 */
public class BigdataTripleSource implements TripleSource {

    private final BigdataSailConnection conn;
    
    public final boolean includeInferred;
    
    BigdataTripleSource(BigdataSailConnection conn, boolean includeInferred) {
        
        this.conn = conn;
        
        this.includeInferred = includeInferred;
        
    }
    
    protected AbstractTripleStore getDatabase() {
        
        return conn.database;
        
    }
    
    /**
     * This wraps
     * {@link BigdataSailConnection#getStatements(Resource, URI, Value, boolean, Resource[])}.
     */
    public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(
            Resource s, URI p, Value o, Resource... contexts)
            throws QueryEvaluationException {

        /*
         * Note: we have to wrap the statement iterator due to conflicting
         * exception signatures.
         */
        
        final CloseableIteration<? extends Statement, SailException> src;
        
        try {
        
            src = conn
                    .getStatements(s, p, o, includeInferred, contexts);
            
        } catch (SailException ex) {
            
            throw new QueryEvaluationException(ex);
            
        }
        
        return new QueryEvaluationIterator<Statement>(src);

    }

    /**
     * The {@link BigdataValueFactoryImpl}.
     */
    public ValueFactory getValueFactory() {

        return conn.database.getValueFactory();
        
    }
    
}
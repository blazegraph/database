package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.Properties;

import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedBooleanQuery;
import org.openrdf.repository.sail.SailBooleanQuery;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;

public class BigdataSailBooleanQuery extends SailBooleanQuery 
        implements BigdataSailQuery {
    
    /**
     * Query hints are embedded in query strings as namespaces.  
     * See {@link QueryHints#PREFIX} for more information.
     */
    private final Properties queryHints;
    
    public Properties getQueryHints() {
    	
    	return queryHints;
    	
    }

    public BigdataSailBooleanQuery(ParsedBooleanQuery tupleQuery,
            SailRepositoryConnection con, Properties queryHints) {

    	super(tupleQuery, con);
    	
        this.queryHints = queryHints;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use query hints from SPARQL queries. Query hints are
     * embedded in query strings as namespaces.
     * 
     * @see QueryHints
     */
    @Override
    public boolean evaluate() throws QueryEvaluationException {

        final ParsedBooleanQuery parsedBooleanQuery = getParsedQuery();
        
        final TupleExpr tupleExpr = parsedBooleanQuery.getTupleExpr();
        
        Dataset dataset = getDataset();
        
        if (dataset == null) {
        
            // No external dataset specified, use query's own dataset (if any)
            dataset = parsedBooleanQuery.getDataset();
        
        }

        try {
            
            final BigdataSailConnection sailCon =
                (BigdataSailConnection) getConnection().getSailConnection();

            CloseableIteration<? extends BindingSet, QueryEvaluationException> bindingsIter;

            bindingsIter = sailCon.evaluate(tupleExpr, dataset, getBindings(),
                    null/* bindingSets */, getIncludeInferred(), queryHints);

            bindingsIter = enforceMaxQueryTime(bindingsIter);

            try {
                return bindingsIter.hasNext();
            }
            finally {
                bindingsIter.close();
            }
        }
        catch (SailException e) {
            throw new QueryEvaluationException(e.getMessage(), e);
        }
    }

    public TupleExpr getTupleExpr() throws QueryEvaluationException {
        
        TupleExpr tupleExpr = getParsedQuery().getTupleExpr();
        
        try {
            
            final BigdataSailConnection sailCon =
                (BigdataSailConnection) getConnection().getSailConnection();
            
            tupleExpr = sailCon.optimize(tupleExpr, getActiveDataset(), 
                    getBindings(), getIncludeInferred(), queryHints);
            
            return tupleExpr;

        } catch (SailException e) {
            
            throw new QueryEvaluationException(e.getMessage(), e);
            
        }
        
    }

//    synchronized public void setBindingSets(
//            final CloseableIteration<BindingSet, QueryEvaluationException> bindings) {
//
//        if (this.bindings != null)
//            throw new IllegalStateException();
//
//        this.bindings = bindings;
//
//    }
//
//    synchronized public CloseableIteration<BindingSet, QueryEvaluationException> getBindingSets() {
//
//        return bindings;
//
//    }
//
//    private CloseableIteration<BindingSet, QueryEvaluationException> bindings;

}

package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.ArrayList;
import java.util.Properties;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.TupleQueryResultImpl;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.repository.sail.SailTupleQuery;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;

public class BigdataSailTupleQuery extends SailTupleQuery 
        implements BigdataSailQuery {
    
    /**
     * Query hints are embedded in query strings as namespaces.  
     * See {@link QueryHints#PREFIX} for more information.
     */
    private final Properties queryHints;
    
    /**
     * Set lazily by {@link #getTupleExpr()}
     */
    private volatile TupleExpr tupleExpr;
    
    public Properties getQueryHints() {
    	
    	return queryHints;
    	
    }
    
	public BigdataSailTupleQuery(final ParsedTupleQuery tupleQuery,
			final SailRepositoryConnection con, final Properties queryHints) {

    	super(tupleQuery, con);
    	
        this.queryHints = queryHints;
        
    }

	/**
	 *{@inheritDoc}
	 * <p>
	 * Overridden to use query hints from SPARQL queries. Query hints are
	 * embedded in query strings as namespaces. See {@link QueryHints#PREFIX}
	 * for more information.
	 */
    @Override
    public TupleQueryResult evaluate() throws QueryEvaluationException {
        
    	final TupleExpr tupleExpr = getParsedQuery().getTupleExpr();

        try {
        
			CloseableIteration<? extends BindingSet, QueryEvaluationException> bindingsIter;

			final BigdataSailConnection sailCon = (BigdataSailConnection) getConnection()
					.getSailConnection();

            bindingsIter = sailCon.evaluate(tupleExpr, getActiveDataset(),
                    getBindings(), null/* bindingSets */, getIncludeInferred(),
                    queryHints);

			bindingsIter = enforceMaxQueryTime(bindingsIter);

			return new TupleQueryResultImpl(new ArrayList<String>(tupleExpr
					.getBindingNames()), bindingsIter);

		} catch (SailException e) {

			throw new QueryEvaluationException(e);

		}

    }

    public TupleExpr getTupleExpr() throws QueryEvaluationException {

        if (tupleExpr == null) {
            
            TupleExpr tupleExpr = getParsedQuery().getTupleExpr();

            try {

                final BigdataSailConnection sailCon = (BigdataSailConnection) getConnection()
                        .getSailConnection();

                tupleExpr = sailCon.optimize(tupleExpr, getActiveDataset(),
                        getBindings(), getIncludeInferred(), queryHints);

                this.tupleExpr = tupleExpr;

            } catch (SailException e) {

                throw new QueryEvaluationException(e.getMessage(), e);

            }
        }

        return tupleExpr;

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

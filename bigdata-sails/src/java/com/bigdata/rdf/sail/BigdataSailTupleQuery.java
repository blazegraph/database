package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.TupleQueryResultImpl;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.repository.sail.SailTupleQuery;
import org.openrdf.sail.SailException;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.PipelineOp;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.ASTEvalHelper;
import com.bigdata.rdf.store.AbstractTripleStore;

public class BigdataSailTupleQuery extends SailTupleQuery 
        implements BigdataSailQuery {

    private static final Logger log = Logger
            .getLogger(BigdataSailTupleQuery.class);
    
    /**
     * Query hints are embedded in query strings as namespaces.  
     * See {@link QueryHints#PREFIX} for more information.
     */
    private final Properties queryHints;
    
    /**
     * Set lazily by {@link #getTupleExpr()}
     */
    private volatile TupleExpr tupleExpr;
    
    private final QueryRoot queryRoot;
    
    public Properties getQueryHints() {
    	
    	return queryHints;
    	
    }
    
    public QueryRoot getQueryRoot() {
        
        return queryRoot;
        
    }

    public AbstractTripleStore getTripleStore() {

        return ((BigdataSailRepositoryConnection) getConnection())
                .getTripleStore();

    }
    
    public BigdataSailTupleQuery(final QueryRoot queryRoot,
            final BigdataSailRepositoryConnection con) {

        super(null/*tupleQuery*/, con); // TODO Might have to fake the TupleExpr with a Nop.

        this.queryHints = queryRoot.getQueryHints();

        this.queryRoot = queryRoot;
        
    }

    @Deprecated
	public BigdataSailTupleQuery(final ParsedTupleQuery tupleQuery,
			final SailRepositoryConnection con, final Properties queryHints) {

    	super(tupleQuery, con);
    	
        this.queryHints = queryHints;

        this.queryRoot = null;
        
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

        if (queryRoot != null) {
            
            if (getMaxQueryTime() > 0)
                queryRoot.setTimeout(TimeUnit.SECONDS
                        .toMillis(getMaxQueryTime()));
            
            queryRoot.setIncludeInferred(getIncludeInferred());
            
            final AbstractTripleStore store = getTripleStore();

            final AST2BOpContext context = new AST2BOpContext(queryRoot, store);

            if (log.isInfoEnabled())
                log.info("queryRoot:\n" + queryRoot);

            /*
             * Run the query optimizer first so we have access to the rewritten
             * query plan.
             */
            final QueryRoot optimizedQuery = context.optimizedQuery = (QueryRoot) context.optimizers
                    .optimize(context, queryRoot, null/* bindingSet[] */);

            if (log.isInfoEnabled())
                log.info("optimizedQuery:\n" + optimizedQuery);

            final PipelineOp queryPlan = AST2BOpUtility.convert(context);

            if (log.isInfoEnabled())
                log.info("queryPlan:\n" + queryPlan);

            final IVariable<?>[] projected = queryRoot.getProjection()
                    .getProjectionVars();

            final TupleQueryResult queryResult = ASTEvalHelper
                    .evaluateTupleQuery(store, queryPlan,
                            new QueryBindingSet(), context.queryEngine,
                            projected);
            
            return queryResult;
            
        }
        
        /*
         * FIXME The code below this point is going away.
         */
        
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

    @Deprecated
    public TupleExpr getTupleExpr() throws QueryEvaluationException {

        if (tupleExpr == null) {
            
            if (getParsedQuery() == null) {
                // native sparql evaluation.
                return null;
            }

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

}

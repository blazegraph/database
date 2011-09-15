package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.TupleQueryResultImpl;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.repository.sail.SailTupleQuery;
import org.openrdf.sail.SailException;

import com.bigdata.bop.IVariable;
import com.bigdata.bop.PipelineOp;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.ASTEvalHelper;
import com.bigdata.rdf.store.AbstractTripleStore;

public class BigdataSailTupleQuery extends SailTupleQuery 
        implements BigdataSailQuery {

//    private static final Logger log = Logger
//            .getLogger(BigdataSailTupleQuery.class);
    
    /**
     * Query hints are embedded in query strings as namespaces.  
     * See {@link QueryHints#PREFIX} for more information.
     */
    private final Properties queryHints;
    
    /**
     * Set lazily by {@link #getTupleExpr()}
     */
    private volatile TupleExpr tupleExpr;
    
    private final ASTContainer astContainer;
    
    public Properties getQueryHints() {
    	
    	return queryHints;
    	
    }
    
    public ASTContainer getASTContainer() {
        
        return astContainer;
        
    }

    @Override
    public void setDataset(final Dataset dataset) {
        
        if(astContainer == null) {
            
            super.setDataset(dataset);
            
        } else {

            /*
             * Batch resolve RDF Values to IVs and then set on the query model.
             */
            
            try {
                
                final Object[] tmp = new BigdataValueReplacer(getTripleStore())
                        .replaceValues(dataset, null/* tupleExpr */, null/* bindings */);
                
                astContainer.getOriginalAST().setDataset(new DatasetNode((Dataset) tmp[0]));
                
            } catch (SailException e) {
                
                throw new RuntimeException(e);
                
            }
            
        }
        
    }
    
    @Override
    public String toString() {

        if (astContainer == null)
            return super.toString();
        
        return astContainer.toString();
        
    }
    
    public AbstractTripleStore getTripleStore() {

        return ((BigdataSailRepositoryConnection) getConnection())
                .getTripleStore();

    }
    
    public BigdataSailTupleQuery(final ASTContainer astContainer,
            final BigdataSailRepositoryConnection con) {

        super(null/*tupleQuery*/, con);

        if(astContainer == null)
            throw new IllegalArgumentException();
        
        this.queryHints = astContainer.getOriginalAST().getQueryHints();

        this.astContainer = astContainer;
        
    }

    @Deprecated
	public BigdataSailTupleQuery(final ParsedTupleQuery tupleQuery,
			final SailRepositoryConnection con, final Properties queryHints) {

    	super(tupleQuery, con);
    	
        this.queryHints = queryHints;

        this.astContainer = null;
        
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

        if (astContainer != null) {
            
            astContainer.clearOptimizedAST();
            
            final QueryRoot originalQuery = astContainer.getOriginalAST();
            
            if (getMaxQueryTime() > 0)
                originalQuery.setTimeout(TimeUnit.SECONDS
                        .toMillis(getMaxQueryTime()));
            
            originalQuery.setIncludeInferred(getIncludeInferred());
            
            final AbstractTripleStore store = getTripleStore();

            final AST2BOpContext context = new AST2BOpContext(astContainer, store);

            final PipelineOp queryPlan = AST2BOpUtility.convert(context);

            final QueryRoot optimizedQuery = astContainer.getOptimizedAST();
            
            final IVariable<?>[] projected = optimizedQuery.getProjection()
                    .getProjectionVars();

            final TupleQueryResult queryResult = ASTEvalHelper
                    .evaluateTupleQuery(store, queryPlan, new QueryBindingSet(
                            getBindings()), context.queryEngine, projected);

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
                    getBindings(), getIncludeInferred(),
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

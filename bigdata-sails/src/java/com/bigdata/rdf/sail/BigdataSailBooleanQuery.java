package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.parser.ParsedBooleanQuery;
import org.openrdf.repository.sail.SailBooleanQuery;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.SailException;

import com.bigdata.bop.BOp;
import com.bigdata.bop.PipelineOp;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.ASTEvalHelper;
import com.bigdata.rdf.store.AbstractTripleStore;

public class BigdataSailBooleanQuery extends SailBooleanQuery 
        implements BigdataSailQuery {

    private static Logger log = Logger.getLogger(BigdataSailBooleanQuery.class);

    /**
     * Query hints are embedded in query strings as namespaces.  
     * See {@link QueryHints#PREFIX} for more information.
     */
    @Deprecated
    private final Properties queryHints;
    
    public Properties getQueryHints() {
    	
    	return queryHints;
    	
    }

    private final QueryRoot queryRoot;

    public QueryRoot getQueryRoot() {
        
        return queryRoot;
        
    }

    public AbstractTripleStore getTripleStore() {

        return ((BigdataSailRepositoryConnection) getConnection())
                .getTripleStore();

    }

    public BigdataSailBooleanQuery(final QueryRoot queryRoot,
            final BigdataSailRepositoryConnection con) {

        super(null/*tupleQuery*/, con); // TODO Might have to fake the TupleExpr with a Nop.
        
        this.queryHints = queryRoot.getQueryHints();
        
        this.queryRoot = queryRoot;
        
    }
    
    @Deprecated
    public BigdataSailBooleanQuery(ParsedBooleanQuery tupleQuery,
            SailRepositoryConnection con, Properties queryHints) {

    	super(tupleQuery, con);
    	
        this.queryHints = queryHints;
        
        this.queryRoot = null;
        
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

            final boolean queryResult = ASTEvalHelper.evaluateBooleanQuery(
                    store, queryPlan, new QueryBindingSet(),
                    context.queryEngine);
            
            return queryResult;
            
        }

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
        
        if (getParsedQuery() == null) {
            // native sparql evaluation.
            return null;
        }

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

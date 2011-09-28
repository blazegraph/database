package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.parser.ParsedBooleanQuery;
import org.openrdf.repository.sail.SailBooleanQuery;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.SailException;

import com.bigdata.bop.PipelineOp;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.eval.ASTEvalHelper;
import com.bigdata.rdf.store.AbstractTripleStore;

public class BigdataSailBooleanQuery extends SailBooleanQuery 
        implements BigdataSailQuery {

//    private static Logger log = Logger.getLogger(BigdataSailBooleanQuery.class);

    /**
     * Query hints are embedded in query strings as namespaces.  
     * See {@link QueryHints#PREFIX} for more information.
     */
    @Deprecated
    private final Properties queryHints;
    
    public Properties getQueryHints() {
    	
    	return queryHints;
    	
    }

    private final ASTContainer astContainer;

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

    public BigdataSailBooleanQuery(final ASTContainer astContainer,
            final BigdataSailRepositoryConnection con) {

        super(null/*tupleQuery*/, con);

        if(astContainer == null)
            throw new IllegalArgumentException();
        
        this.queryHints = astContainer.getOriginalAST().getQueryHints();
        
        this.astContainer = astContainer;
        
    }
    
    @Deprecated
    public BigdataSailBooleanQuery(ParsedBooleanQuery tupleQuery,
            SailRepositoryConnection con, Properties queryHints) {

    	super(tupleQuery, con);
    	
        this.queryHints = queryHints;
        
        this.astContainer = null;
        
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

        if (astContainer != null) {
            
            astContainer.clearOptimizedAST();

            final QueryRoot originalQuery = astContainer.getOriginalAST();
            
            if (getMaxQueryTime() > 0)
                originalQuery.setTimeout(TimeUnit.SECONDS
                        .toMillis(getMaxQueryTime()));

            originalQuery.setIncludeInferred(getIncludeInferred());

            final AbstractTripleStore store = getTripleStore();

            final AST2BOpContext context = new AST2BOpContext(astContainer, store);

            // Generate the query plan.
            final PipelineOp queryPlan = AST2BOpUtility.convert(context);

            final boolean queryResult = ASTEvalHelper.evaluateBooleanQuery(//
                    store, //
                    queryPlan,//
                    new QueryBindingSet(getBindings()),//
                    context.queryEngine//
                    );
            
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
                    getIncludeInferred(), queryHints);

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
}

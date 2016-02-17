package com.bigdata.rdf.sail;

import java.util.concurrent.TimeUnit;

import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.repository.sail.SailTupleQuery;

import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.BindingsClause;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.ASTEvalHelper;
import com.bigdata.rdf.store.AbstractTripleStore;

public class BigdataSailTupleQuery extends SailTupleQuery 
        implements BigdataSailQuery {

//    private static final Logger log = Logger.getLogger(BigdataSailTupleQuery.class);
    
    private final ASTContainer astContainer;
    
    public ASTContainer getASTContainer() {
        
        return astContainer;
        
    }

    @Override
    public String toString() {

        return astContainer.toString();
        
    }
    
    public AbstractTripleStore getTripleStore() {

        return ((BigdataSailRepositoryConnection) getConnection())
                .getTripleStore();

    }

    public BigdataSailTupleQuery(final ASTContainer astContainer,
            final BigdataSailRepositoryConnection con) {

        super(null/* tupleQuery */, con);

        if (astContainer == null)
            throw new IllegalArgumentException();

        this.astContainer = astContainer;

    }

    @Override
    public TupleQueryResult evaluate() throws QueryEvaluationException {

        return evaluate((BindingsClause) null);

    }

    public TupleQueryResult evaluate(final BindingsClause bc) 
    		throws QueryEvaluationException {

        final QueryRoot originalQuery = astContainer.getOriginalAST();

        if (bc != null)
        	originalQuery.setBindingsClause(bc);

        if (getMaxQueryTime() > 0)
            originalQuery.setTimeout(TimeUnit.SECONDS
                    .toMillis(getMaxQueryTime()));

        originalQuery.setIncludeInferred(getIncludeInferred());

        final TupleQueryResult queryResult = ASTEvalHelper.evaluateTupleQuery(
                getTripleStore(), astContainer, new QueryBindingSet(
                        getBindings()), getDataset());

        return queryResult;

    }
    
    public QueryRoot optimize() throws QueryEvaluationException {

        return optimize((BindingsClause) null);

    }

    public QueryRoot optimize(final BindingsClause bc) 
            throws QueryEvaluationException {

        final QueryRoot originalQuery = astContainer.getOriginalAST();

        if (bc != null)
            originalQuery.setBindingsClause(bc);

        if (getMaxQueryTime() > 0)
            originalQuery.setTimeout(TimeUnit.SECONDS
                    .toMillis(getMaxQueryTime()));

        originalQuery.setIncludeInferred(getIncludeInferred());

        final QueryRoot optimized = ASTEvalHelper.optimizeQuery(
                astContainer,
                new AST2BOpContext(astContainer, getTripleStore()),
                new QueryBindingSet(getBindings()), getDataset());

        return optimized;
        
    }

}

package com.bigdata.rdf.sail;

import java.util.concurrent.TimeUnit;

import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.repository.sail.SailBooleanQuery;

import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.BindingsClause;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.ASTEvalHelper;
import com.bigdata.rdf.store.AbstractTripleStore;

public class BigdataSailBooleanQuery extends SailBooleanQuery 
        implements BigdataSailQuery {

//    private static Logger log = Logger.getLogger(BigdataSailBooleanQuery.class);

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

    public BigdataSailBooleanQuery(final ASTContainer astContainer,
            final BigdataSailRepositoryConnection con) {

        super(null/*tupleQuery*/, con);

        if(astContainer == null)
            throw new IllegalArgumentException();
        
        this.astContainer = astContainer;
        
    }
    
    @Override
    public boolean evaluate() throws QueryEvaluationException {

        return evaluate((BindingsClause) null);

    }

    public boolean evaluate(final BindingsClause bc) 
    		throws QueryEvaluationException {

        final QueryRoot originalQuery = astContainer.getOriginalAST();

        if (bc != null)
        	originalQuery.setBindingsClause(bc);

        if (getMaxQueryTime() > 0)
            originalQuery.setTimeout(TimeUnit.SECONDS
                    .toMillis(getMaxQueryTime()));

        originalQuery.setIncludeInferred(getIncludeInferred());

        final boolean queryResult = ASTEvalHelper.evaluateBooleanQuery(
                getTripleStore(), astContainer, new QueryBindingSet(
                        getBindings()), getDataset());

        return queryResult;
    }

}

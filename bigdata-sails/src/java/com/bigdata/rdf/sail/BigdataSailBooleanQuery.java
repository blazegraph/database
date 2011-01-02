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
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;

public class BigdataSailBooleanQuery extends SailBooleanQuery 
        implements BigdataSailQuery {
    
    /**
     * Query hints are embedded in query strings as namespaces.  
     * See {@link QueryHints#NAMESPACE} for more information.
     */
    private final Properties queryHints;
    
    public BigdataSailBooleanQuery(ParsedBooleanQuery tupleQuery,
            SailRepositoryConnection con, Properties queryHints) {
        super(tupleQuery, con);
        this.queryHints = queryHints;
    }

    /**
     * Overriden to use query hints from SPARQL queries. Query hints are
     * embedded in query strings as namespaces.  
     * See {@link QueryHints#NAMESPACE} for more information.
     */
    @Override
    public boolean evaluate() throws QueryEvaluationException {
        ParsedBooleanQuery parsedBooleanQuery = getParsedQuery();
        TupleExpr tupleExpr = parsedBooleanQuery.getTupleExpr();
        Dataset dataset = getDataset();
        if (dataset == null) {
            // No external dataset specified, use query's own dataset (if any)
            dataset = parsedBooleanQuery.getDataset();
        }

        try {
            BigdataSailConnection sailCon =
                (BigdataSailConnection) getConnection().getSailConnection();

            CloseableIteration<? extends BindingSet, QueryEvaluationException> bindingsIter;
            bindingsIter = sailCon.evaluate(tupleExpr, dataset, getBindings(), getIncludeInferred(), queryHints);

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
            BigdataSailConnection sailCon =
                (BigdataSailConnection) getConnection().getSailConnection();
            tupleExpr = sailCon.optimize(tupleExpr, getActiveDataset(), 
                    getBindings(), getIncludeInferred(), queryHints);
            return tupleExpr;
        }
        catch (SailException e) {
            throw new QueryEvaluationException(e.getMessage(), e);
        }
    }

}

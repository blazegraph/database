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
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.store.BD;

public class BigdataSailTupleQuery extends SailTupleQuery {
    
    /**
     * Query hints are embedded in query strings as namespaces.  
     * See {@link BD#QUERY_HINTS_NAMESPACE} for more information.
     */
    private final Properties queryHints;
    
    public BigdataSailTupleQuery(ParsedTupleQuery tupleQuery,
            SailRepositoryConnection con, Properties queryHints) {
        super(tupleQuery, con);
        this.queryHints = queryHints;
    }

    /**
     * Overriden to use query hints from SPARQL queries. Query hints are
     * embedded in query strings as namespaces.  
     * See {@link BD#QUERY_HINTS_NAMESPACE} for more information.
     */
    @Override
    public TupleQueryResult evaluate() throws QueryEvaluationException {
        TupleExpr tupleExpr = getParsedQuery().getTupleExpr();

        try {
            CloseableIteration<? extends BindingSet, QueryEvaluationException> bindingsIter;

            BigdataSailConnection sailCon =
                (BigdataSailConnection) getConnection().getSailConnection();
            bindingsIter = sailCon.evaluate(tupleExpr, getActiveDataset(), getBindings(), getIncludeInferred(), queryHints);

            bindingsIter = enforceMaxQueryTime(bindingsIter);

            return new TupleQueryResultImpl(new ArrayList<String>(tupleExpr.getBindingNames()), bindingsIter);
        }
        catch (SailException e) {
            throw new QueryEvaluationException(e.getMessage(), e);
        }
    }
}

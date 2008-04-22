package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.FilterIteration;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.GraphQueryResultImpl;
import org.openrdf.query.parser.ParsedGraphQuery;
import org.openrdf.repository.sail.SailGraphQuery;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.SailException;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;

public class BigdataSailGraphQuery extends SailGraphQuery {
    public BigdataSailGraphQuery(ParsedGraphQuery tupleQuery,
            SailRepositoryConnection con) {
        super(tupleQuery, con);
    }

    @Override
    public GraphQueryResult evaluate() throws QueryEvaluationException {
        TupleExpr tupleExpr = getParsedQuery().getTupleExpr();
        try {
            BigdataSailConnection sailCon =
                    (BigdataSailConnection) getConnection().getSailConnection();
            CloseableIteration<? extends BindingSet, QueryEvaluationException> bindingsIter =
                    sailCon.evaluate(
                            tupleExpr, getActiveDataset(), getBindings(),
                            getIncludeInferred());
            // Filters out all partial and invalid matches
            bindingsIter =
                    new FilterIteration<BindingSet, QueryEvaluationException>(
                            bindingsIter) {
                        @Override
                        protected boolean accept(BindingSet bindingSet) {
                            Value context = bindingSet.getValue("context");
                            return bindingSet.getValue("subject") instanceof Resource
                                    && bindingSet.getValue("predicate") instanceof URI
                                    && bindingSet.getValue("object") instanceof Value
                                    && (context == null || context instanceof Resource);
                        }
                    };
            // Convert the BindingSet objects to actual RDF statements
            CloseableIteration<? extends Statement, QueryEvaluationException> stIter;
            stIter = new BigdataConstructIterator(sailCon.getDatabase(),  bindingsIter);
            return new GraphQueryResultImpl(getParsedQuery()
                    .getQueryNamespaces(), stIter);
        } catch (SailException e) {
            throw new QueryEvaluationException(e.getMessage(), e);
        }
    }
}

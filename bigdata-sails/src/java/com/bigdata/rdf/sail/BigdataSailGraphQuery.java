package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.ConvertingIteration;
import info.aduna.iteration.FilterIteration;
import java.util.Properties;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
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
import com.bigdata.rdf.store.BD;

public class BigdataSailGraphQuery extends SailGraphQuery {
    
    /**
     * Query hints are embedded in query strings as namespaces.  
     * See {@link BD#QUERY_HINTS_NAMESPACE} for more information.
     */
    private final Properties queryHints;
    
    /**
     * Allow clients to bypass the native construct iterator, which resolves
     * binding sets into SPOs into BigdataStatements.
     */
    private boolean useNativeConstruct = true;
    
    public BigdataSailGraphQuery(ParsedGraphQuery tupleQuery,
            SailRepositoryConnection con, Properties queryHints) {
        super(tupleQuery, con);
        this.queryHints = queryHints;
    }

    /**
     * Allow clients to bypass the native construct iterator, which resolves
     * binding sets into SPOs into BigdataStatements.  Sometimes this can
     * cause problems, especially when construct graphs contain values not
     * in the database's lexicon. 
     */
    public void setUseNativeConstruct(boolean useNativeConstruct) {
        this.useNativeConstruct = useNativeConstruct;
    }
    
    /**
     * Overriden to use query hints from SPARQL queries. Query hints are
     * embedded in query strings as namespaces.  
     * See {@link BD#QUERY_HINTS_NAMESPACE} for more information.
     */
    @Override
    public GraphQueryResult evaluate() throws QueryEvaluationException {
        
        try {
            
            TupleExpr tupleExpr = getParsedQuery().getTupleExpr();
            BigdataSailConnection sailCon =
                    (BigdataSailConnection) getConnection().getSailConnection();
            CloseableIteration<? extends BindingSet, QueryEvaluationException> bindingsIter =
                    sailCon.evaluate(
                            tupleExpr, getActiveDataset(), getBindings(),
                            getIncludeInferred(), queryHints);
            
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
                    
            if (!useNativeConstruct) {

                // Convert the BindingSet objects to actual RDF statements
                final ValueFactory vf = getConnection().getRepository().getValueFactory();
                CloseableIteration<Statement, QueryEvaluationException> stIter;
                stIter = new ConvertingIteration<BindingSet, Statement, QueryEvaluationException>(bindingsIter) {

                    @Override
                    protected Statement convert(BindingSet bindingSet) {
                        Resource subject = (Resource)bindingSet.getValue("subject");
                        URI predicate = (URI)bindingSet.getValue("predicate");
                        Value object = bindingSet.getValue("object");
                        Resource context = (Resource)bindingSet.getValue("context");

                        if (context == null) {
                            return vf.createStatement(subject, predicate, object);
                        }
                        else {
                            return vf.createStatement(subject, predicate, object, context);
                        }
                    }
                    
                };

                return new GraphQueryResultImpl(getParsedQuery().getQueryNamespaces(), stIter);

            } else {
                
                // Convert the BindingSet objects to actual RDF statements
                final ValueFactory vf = getConnection().getRepository().getValueFactory();
                CloseableIteration<? extends Statement, QueryEvaluationException> stIter;
                stIter = new BigdataConstructIterator(sailCon.getTripleStore(),  bindingsIter, vf);
                return new GraphQueryResultImpl(getParsedQuery()
                        .getQueryNamespaces(), stIter);
                
            }
            
        } catch (SailException e) {
            throw new QueryEvaluationException(e.getMessage(), e);
        }
        
    }
    
}

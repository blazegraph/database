/**
Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package com.bigdata.rdf.sail;

import java.util.concurrent.TimeUnit;

import org.openrdf.query.Dataset;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.repository.sail.SailGraphQuery;

import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.BindingsClause;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.ASTEvalHelper;
import com.bigdata.rdf.store.AbstractTripleStore;

public class BigdataSailGraphQuery extends SailGraphQuery implements
        BigdataSailQuery {

    // private static Logger log = Logger.getLogger(BigdataSailGraphQuery.class);

    private final ASTContainer astContainer;

    public ASTContainer getASTContainer() {

        return astContainer;

    }

    @Override
    public void setDataset(final Dataset dataset) {

        /*
         * Batch resolve RDF Values to IVs and then set on the query model.
         */

        final Object[] tmp = new BigdataValueReplacer(getTripleStore())
                .replaceValues(dataset, null/* bindings */);

        astContainer.getOriginalAST().setDataset(
                new DatasetNode((Dataset) tmp[0], false/* update */));

    }
    
    @Override
    public String toString() {

        return astContainer.toString();

    }
    
    public AbstractTripleStore getTripleStore() {

        return ((BigdataSailRepositoryConnection) getConnection())
                .getTripleStore();

    }

    public BigdataSailGraphQuery(final ASTContainer astContainer,
            final BigdataSailRepositoryConnection con) {

        super(null/*tupleQuery*/, con);

        if(astContainer == null)
            throw new IllegalArgumentException();
        
        this.astContainer = astContainer;
        
    }
    
    @Override
    public GraphQueryResult evaluate() throws QueryEvaluationException {

        return evaluate((BindingsClause) null);

    }

    public GraphQueryResult evaluate(final BindingsClause bc)
            throws QueryEvaluationException {

        final QueryRoot originalQuery = astContainer.getOriginalAST();

        if (bc != null)
            originalQuery.setBindingsClause(bc);

        if (getMaxQueryTime() > 0)
            originalQuery.setTimeout(TimeUnit.SECONDS
                    .toMillis(getMaxQueryTime()));

        originalQuery.setIncludeInferred(getIncludeInferred());

        final GraphQueryResult queryResult = ASTEvalHelper.evaluateGraphQuery(
                getTripleStore(), astContainer, new QueryBindingSet(
                        getBindings()));

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
                getTripleStore(), astContainer, new QueryBindingSet(
                        getBindings()));

        return optimized;

    }

}

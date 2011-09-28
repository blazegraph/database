/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
/*
 * Created on Sep 1, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import info.aduna.iteration.CloseableIteration;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.GraphQueryResultImpl;
import org.openrdf.query.impl.TupleQueryResultImpl;
import org.openrdf.sail.SailException;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.Bigdata2Sesame2BindingSetIterator;
import com.bigdata.rdf.sail.BigdataValueReplacer;
import com.bigdata.rdf.sail.RunningQueryCloseableIterator;
import com.bigdata.rdf.sail.sop.UnsupportedOperatorException;
import com.bigdata.rdf.sparql.ast.ConstructNode;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataBindingSetResolverator;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.Dechunkerator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Helper class for evaluating SPARQL queries.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTEvalHelper {

    /**
     * Return an {@link IAsynchronousIterator} that will read a single, empty
     * {@link IBindingSet}.
     * 
     * @param bindingSet
     *            the binding set.
     */
    static private ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet bindingSet) {

        return new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { new IBindingSet[] { bindingSet } });

    }

    /**
     * Setup the input binding sets which will be fed into the query pipeline.
     * 
     * @throws SailException 
     */
    static private IAsynchronousIterator<IBindingSet[]> wrapSource(
            final AbstractTripleStore store, final BindingSet bs)
            throws SailException {

        final IAsynchronousIterator<IBindingSet[]> source;
        
        if (bs != null) {
        
            /*
             * A single input binding set will be fed into the query pipeline
             * using the supplied bindings.
             */

            // batch resolve Values to IVs.
            final Object[] tmp = new BigdataValueReplacer(store)
                    .replaceValues(null/* dataset */, null/* tupleExpr */, bs);

            // wrap the resolved binding set.
            source = newBindingSetIterator(toBindingSet((BindingSet) tmp[1]));

        } else {
            
            /*
             * A single empty input binding set will be fed into the query
             * pipeline.
             */
            
            source = newBindingSetIterator(new ListBindingSet());
            
        }

        return source;
        
    }
    
    /**
     * Evaluate a boolean query.
     * 
     * @param store
     *            The {@link AbstractTripleStore} having the data.
     * @param queryPlan
     *            The query plan.
     * @param bs
     *            The initial solution to kick things off.
     * @param queryEngine
     *            The query engine.
     *            
     * @return <code>true</code> if there are any solutions to the query.
     * 
     * @throws QueryEvaluationException
     */
    static public boolean evaluateBooleanQuery(
            final AbstractTripleStore database, final PipelineOp queryPlan,
            final BindingSet bs, final QueryEngine queryEngine)
            throws QueryEvaluationException {

        IRunningQuery runningQuery = null;
        IAsynchronousIterator<IBindingSet[]> source = null;
        try {

            source = wrapSource(database, bs);
            
            // Submit query for evaluation.
            runningQuery = queryEngine.eval(queryPlan, source);

            // See if any solutions were produced.
            return runningQuery.iterator().hasNext();

        } catch (Exception e) {

            throw new QueryEvaluationException(e);
            
        } finally {
            
            // Ensure source is closed.
            if(source != null)
                source.close();
            
            if (runningQuery != null) {
            
                runningQuery.cancel(true/* mayInterruptIfRunning */);
                
                checkFuture(runningQuery);

            }

        }

    }

    /**
     * Test for abnormal completion of the {@link IRunningQuery}.
     */
    static private void checkFuture(final IRunningQuery runningQuery)
            throws QueryEvaluationException {

        try {
            runningQuery.get();
        } catch (InterruptedException e) {
            /*
             * Interrupted while waiting on the Future.
             */
            throw new RuntimeException(e);
        } catch (Throwable e) {
            /*
             * Exception thrown by the runningQuery.
             */
            if (runningQuery.getCause() != null) {
                // abnormal termination - wrap and rethrow.
                throw new QueryEvaluationException(e);
            }
            // otherwise this is normal termination.
        }

    }
    
    /**
     * Evaluate a SELECT query.
     * 
     * @param store
     *            The {@link AbstractTripleStore} having the data.
     * @param queryPlan
     *            The query plan.
     * @param bs
     *            The initial solution to kick things off.
     * @param queryEngine
     *            The query engine.
     * @param projected
     *            The variables projected by the query.
     *            
     * @return An object from which the solutions may be drained.
     * 
     * @throws QueryEvaluationException
     */
    static public TupleQueryResult evaluateTupleQuery(
            final AbstractTripleStore store, final PipelineOp queryPlan,
            final QueryBindingSet bs, final QueryEngine queryEngine,
            final IVariable<?>[] projected) throws QueryEvaluationException {

        final List<String> projectedSet = new LinkedList<String>();

        for (IVariable<?> var : projected)
            projectedSet.add(var.getName());

        return new TupleQueryResultImpl(projectedSet,
                ASTEvalHelper.evaluateQuery(store, queryPlan,
                        new QueryBindingSet(bs), queryEngine, projected));

    }
    
    /**
     * Evaluate a CONSTRUCT/DESCRIBE query.
     * 
     * @param store
     *            The {@link AbstractTripleStore} having the data.
     * @param queryPlan
     *            The query plan.
     * @param bs
     *            The initial solution to kick things off.
     * @param queryEngine
     *            The query engine.
     * @param projected
     *            The variables projected by the query.
     * @param prefixDecls
     *            The namespace prefix declarations map. This is a {@link Map}
     *            with {@link String} keys (prefix) and {@link String} values
     *            (the uri associated with that prefix).
     * @param construct
     *            The construct template.
     * 
     * @throws QueryEvaluationException
     */
    public static GraphQueryResult evaluateGraphQuery(
            final AbstractTripleStore store, final PipelineOp queryPlan,
            final QueryBindingSet queryBindingSet,
            final QueryEngine queryEngine, final IVariable<?>[] projected,
            final Map<String, String> prefixDecls, final ConstructNode construct)
            throws QueryEvaluationException {

        return new GraphQueryResultImpl(prefixDecls, new ASTConstructIterator(
                store, construct, ASTEvalHelper.evaluateQuery(store,
                        queryPlan, queryBindingSet, queryEngine, projected)));

    }
    
    /**
     * Evaluate a query plan.
     * 
     * @return An iteration which may be used to read Sesame {@link BindingSet}s
     *         containing the solutions for the query.
     * 
     *         TODO What is [required] for? It is probably where we are finally
     *         pruning off the variables which are not being projected. That
     *         should be done in {@link AST2BOpUtility}. (Yes, it gets passed
     *         through to {@link BigdataBindingSetResolverator}, but that will
     *         also accept a <code>null</code> and then just materialize all
     *         variables it finds, which is what it should be doing if we have
     *         already pruned the variables to just the projected variables in
     *         the query plan.)
     *         <p>
     *         There is a twist here. I believe that Sesame may allow the
     *         variables given in the initial binding set to flow through even
     *         if they were not projected.
     *         <p>
     *         see https://sourceforge.net/apps/trac/bigdata/ticket/368 (Prune
     *         variable bindings during query evaluation).
     */
    static public CloseableIteration<BindingSet, QueryEvaluationException> evaluateQuery(
            final AbstractTripleStore database, final PipelineOp queryPlan,
            final QueryBindingSet bs, final QueryEngine queryEngine,
            final IVariable<?>[] required) throws QueryEvaluationException {

        IRunningQuery runningQuery = null;
        IAsynchronousIterator<IBindingSet[]> source = null; 
        try {
            
            source = wrapSource(database, bs);

            // Submit query for evaluation.
            runningQuery = queryEngine.eval(queryPlan, source);

            /*
             * Wrap up the native bigdata query solution iterator as Sesame
             * compatible iteration with materialized RDF Values.
             */
            return iterator(runningQuery, database, required);

        } catch (UnsupportedOperatorException t) {
            if (runningQuery != null) {
                // ensure query is halted.
                runningQuery.cancel(true/* mayInterruptIfRunning */);
            }
            // ensure source is closed on error path.
            if(source != null) 
                source.close();
            /*
             * Note: Do not wrap as a different exception type. The caller is
             * looking for this.
             */
            throw new UnsupportedOperatorException(t);
        } catch (Throwable t) {
            if (runningQuery != null) {
                // ensure query is halted.
                runningQuery.cancel(true/* mayInterruptIfRunning */);
            }
            // ensure source is closed on error path.
            if(source != null) 
                source.close();
            throw new QueryEvaluationException(t);
        }

    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static IBindingSet toBindingSet(final BindingSet src) {

        if (src == null)
            throw new IllegalArgumentException();

        final ListBindingSet bindingSet = new ListBindingSet();

        final Iterator<Binding> itr = src.iterator();

        while(itr.hasNext()) {

            final Binding binding = itr.next();

            final IVariable<IV> var = com.bigdata.bop.Var.var(binding.getName());
            
            final IV iv = ((BigdataValue) binding.getValue()).getIV();
            
            final IConstant<IV> val = new Constant<IV>(iv);
            
            bindingSet.set(var, val);
            
        }
        
        return bindingSet;
        
    }
    
    private static ICloseableIterator<IBindingSet> iterator(
            final IRunningQuery runningQuery) {

        // The iterator draining the query solutions.
        final IAsynchronousIterator<IBindingSet[]> it1 = runningQuery
                .iterator();

        // Dechunkify the original iterator
        final ICloseableIterator<IBindingSet> it2 = 
            new Dechunkerator<IBindingSet>(it1);

        // Monitor IRunningQuery and cancel if Sesame iterator is closed.
        final ICloseableIterator<IBindingSet> it3 =
            new RunningQueryCloseableIterator<IBindingSet>(runningQuery, it2);

        return it3;
        
    }
    
    private static CloseableIteration<BindingSet, QueryEvaluationException> 
        iterator(final IRunningQuery runningQuery, final AbstractTripleStore db,
            final IVariable<?>[] required) {
    
        final ICloseableIterator<IBindingSet> it1 = iterator(runningQuery);
        
        // Wrap in an IChunkedOrderedIterator
        final IChunkedOrderedIterator<IBindingSet> it2 = 
            new ChunkedWrappedIterator<IBindingSet>(it1);
        
        // Materialize IVs as RDF Values.
        final CloseableIteration<BindingSet, QueryEvaluationException> it3 =
            // Convert bigdata binding sets to Sesame binding sets.
            new Bigdata2Sesame2BindingSetIterator<QueryEvaluationException>(
                // Materialize IVs as RDF Values.
                new BigdataBindingSetResolverator(db, it2, required).start(
                        db.getExecutorService()));
        
        return it3;
        
    }

}

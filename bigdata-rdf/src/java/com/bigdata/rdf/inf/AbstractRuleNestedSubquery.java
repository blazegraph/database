/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Oct 29, 2007
 */

package com.bigdata.rdf.inf;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IAccessPath;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Base class for rules comprised of one or more triple patterns to be evaluated
 * using a nested subquery. This is the most general technique for evaluation of
 * conjunctive triple patterns. The triple patterns are evaluated in the
 * {@link Rule#order} determined on the selectivity of the triple patterns.
 * 
 * @todo tweak order for (new+db).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractRuleNestedSubquery extends AbstractRuleRdf {

    /**
     * When true, enables subquery elimination by mapping a set of outer
     * {@link SPO}s that would result in the same subquery across the subquery.
     * The effect is that the subquery is executed once, but the same set of
     * bindings are generated as if you did not perform subquery elimination.
     * 
     * @todo subquery elimination is not entirely working yet. there is an
     *       assumption that there will be shared variables which is not always
     *       true (fast query 11 and 13 are counter examples). There is also
     *       clearly some other problem since it causes too many inferences to
     *       be drawn - perhaps related to variable bindings not be set or
     *       cleared correctly when the subqueries are formulated?
     */
    final boolean subqueryElimination = false;
    
    /**
     * @param db
     * @param head
     * @param body
     */
    public AbstractRuleNestedSubquery(AbstractTripleStore db, Triple head,
            Pred[] body) {
        
        super(db, head, body);
        
    }

    /**
     * Recursively evaluate the subqueries.
     */
    public RuleStats apply0( boolean justify, SPOBuffer buffer) {

        resetBindings();

        if(subqueryElimination) {

            apply2( 0, justify, buffer, EMPTY_SET);
            
        } else {
            
            apply1( 0, justify, buffer);
            
        }

        assert checkBindings();
        
        return stats;
        
    }
    
    /**
     * Variant does not attempt subquery elimination.
     * 
     * @param index
     *            The current index in order[] that is being scanned.
     *            <p>
     *            Note: You MUST indirect through order, e.g., order[index], to
     *            obtain the index of the corresponding predicate in the
     *            evaluation order.
     * 
     * @todo if(order[index]==focusIndex) {} else {}
     */
    public void apply1(final int index, final boolean justify, SPOBuffer buffer) {

        assert index >= 0;
        assert index < body.length;
        
        /*
         * Subquery iterator.
         */
        ISPOIterator itr = getAccessPath(order[index]).iterator();
        
        while(itr.hasNext()) {

            // next chunk of statements.
            SPO[] chunk = itr.nextChunk();

            if( index+1 < body.length ) {
                
                // nexted subquery.

                for(SPO stmt : chunk) {

                    if (DEBUG) {
                        log.debug("Considering: " + stmt.toString(db)
                                + ", index=" + index + ", rule=" + getName());
                    }

                    /*
                     * Then bind this statement, which propagates bindings to
                     * the next predicate. 
                     */
                    clearDownstreamBindings(index+1);
                    bind(order[index],stmt);
                    stats.nstmts[order[index]] ++;                    
                    
                    // run the subquery.
                    apply1(index+1,justify,buffer);
                    stats.nsubqueries[order[index]]++;

                }
                
            } else {
                
                // bottomed out.
                
                for( SPO stmt : chunk ) {
                    
                    if (DEBUG) {
                        log.debug("Considering: " + stmt.toString(db)
                                + ", index=" + index + ", rule=" + getName());
                    }

                    // bind this statement.
                    bind(order[index],stmt);
                    stats.nstmts[order[index]]++;

                    // emit entailment.
                    emit(justify, buffer);

                }
                
            }
            
        }

    }

    /**
     * 
     * @param index
     *            The current index in order[] that is being scanned.
     *            <p>
     *            Note: You MUST indirect through order, e.g., order[index], to
     *            obtain the index of the corresponding predicate in the
     *            evaluation order.
     * 
     * @todo if(order[index]==focusIndex) {} else {}
     */
    public void apply2(final int index, final boolean justify, SPOBuffer buffer, List<SPO> outerSet) {

        assert index >= 0;
        assert index < body.length;
        
        // the sort order for chunks for this iterator (iff there is a subquery).
        KeyOrder keyOrder = (index + 1 == body.length //
                ? null // no subquery
                : getSortOrder(order[index], order[index + 1]) // subquery
                );
        
        /*
         * Subquery iterator.
         * 
         * Note: if there was an outer subquery then the natural order of the
         * inner subquery MUST match the order into which we sort each chunk of
         * statement delivered by the outer subquery in order for subquery
         * elimination to work.
         */
        ISPOIterator itr = getAccessPath(order[index]).iterator();
        
        while(itr.hasNext()) {

            // next chunk of statements : sorted iff there is a subquery.
            SPO[] chunk = (keyOrder == null ? itr.nextChunk() : itr
                    .nextChunk(keyOrder));

            if( index+1 < body.length ) {
                
                // nexted subquery.

                for(int i=0; i<chunk.length; /*inc below*/) {

                    if(subqueryElimination && outerSet.isEmpty()) {
                    
                        /*
                         * Subquery elimination logic:
                         */
                        
                        /*
                         * Collect a sequence of statements that can be mapped
                         * across the same subquery.
                         */
                        List<SPO> innerSet = getStatementsBindingSameSubquery(
                                index, chunk, 0);
                        
                        // the #of statement in that sequence.
                        final int n = innerSet.size();
                        
                        i += n;
                    
                        stats.nstmts[order[index]] += n;
                        
                        /*
                         * Apply bindings for the 1st stmt (all statements in this
                         * set will cause the same values to be bound for the
                         * subquery so it does not matter which one you use here).
                         */
    
                        clearDownstreamBindings(index+1);

                        bind(order[index],innerSet.get(0));
                        
                        // run the subquery.
                        
                        stats.nsubqueries[order[index]]++;
    
                        apply2(index+1,justify,buffer,innerSet);
                        
                    } else {
                    
                        /*
                         * @todo The logic does not support recursive subquery
                         * elimination. In order to do that the outerSet would
                         * have to become a list of sets in case we were able to
                         * do subquery elimination for two or more predicates in
                         * the order[] of evaluation.
                         */
                        
                        stats.nstmts[order[index]] ++;
                        
                        // Apply bindings for the the current statement.
                        bind(order[index],chunk[i]);
                        
                        // run the subquery.
                        
                        stats.nsubqueries[order[index]]++;
    
                        apply2(index+1,justify,buffer,EMPTY_SET);

                        i++; // next in this chunk.
                        
                    }
                    
                }
                
            } else {
                
                // bottomed out.
                
                for( SPO stmt : chunk ) {
                    
                    // bind this statement.
                    bind(order[index],stmt);

                    stats.nstmts[order[index]]++;

                    if(outerSet.isEmpty()) {

                        emit(justify, buffer);

                    } else {

                        for (SPO ostmt : outerSet) {

                            // bind the statement from the _outer_ query.
                            bind(order[index-1],ostmt);
                        
                            emit(justify, buffer);
                        
                        }

                    }
                    
                }
                
            }
            
        }

    }

    /**
     * At each level of nexted subquery evaluation, the statements are sorted
     * such that the shared variable(s) with the nested subquery are ordered.
     * This allows us to eliminate subqueries with duplicate bindings by
     * skipping tuples in the outer query that would realize identical bindings
     * on the inner query.
     * 
     * @param outerIndex
     *            The index of the predicate for the outer query.
     * 
     * @param innerIndex
     *            The index of the predicate for the inner (aka nested) query.
     * 
     * @return The sort order to be applied to each chunk of statements that we
     *         visit in the outer query.
     * 
     * @throws IndexOutOfBoundsException
     *             if either index is out of bounds.
     */
    private KeyOrder getSortOrder(int outerIndex, int innerIndex) {
    
        Set<Var> sharedVars = getSharedVars(outerIndex, innerIndex);
        
        if (sharedVars.size() == 0) {

            throw new AssertionError("No shared variables: outer=" + outerIndex
                    + ", inner=" + innerIndex + ", " + this);
            
        }

        /*
         * Form a triple pattern that will have the correct order for
         * correlating the outer and inner subqueries so that we can eliminate
         * redundent subqueries.
         * 
         * If the variable is shared between the two predicates then the
         * corresponding position in the triple pattern will be non-NULL (it is
         * set to a fake constant since it will be bound when we evaluate the
         * subquery).
         * 
         * Otherwise the position in the triple pattern will be the currently
         * bound value.  This covers three distinct cases:
         * 
         * 1) If the position is a constant, then this will be a constant.
         * 
         * 2) If the position is a variable AND the variable was already bound
         * when evaluating a previous triple pattern, then the position will be
         * a constant.
         * 
         * 3) Finally, if the position is a variable and it is unbound, then the
         * subquery will be unbound in that position and the position will be
         * NULL.
         */
        
        // a fake constant.
        final long c = -1; 
        
        long s = sharedVars.contains(body[innerIndex].s)?c:get(body[innerIndex].s);
        long p = sharedVars.contains(body[innerIndex].p)?c:get(body[innerIndex].p);
        long o = sharedVars.contains(body[innerIndex].o)?c:get(body[innerIndex].o);

        IAccessPath accessPath = db.getAccessPath(s, p, o);
        
        KeyOrder keyOrder = accessPath.getKeyOrder(); 
        
        return keyOrder;
        
    }

    /**
     * 
     * @param index
     *            The index into {@link Rule#order} corresponding to the
     *            predicate for which the chunk of statements was materialized.
     * @param chunk
     *            A chunk of {@link SPO}s sorted according to the natural order
     *            that will be used by the triple pattern for the subquery.
     * @param i
     *            The starting index in <i>chunk</i>
     * 
     * @return The set of sequentially occurring {@link SPO}s in <i>chunk</i>
     *         that will impose the same bindings on the subquery.
     */
    private List<SPO> getStatementsBindingSameSubquery(int index, SPO[] chunk,int i) {

        // variables shared between the current predicate and the subquery.
        Set<Var> sharedVars = getSharedVars(order[index], order[index+1]);

        if(sharedVars.isEmpty()) {
            
            /*
             * if nothing is shared then all statements will cause the same
             * subquery to be issued (this would be pretty unusual).
             */
            
            return Arrays.asList(chunk);
            
        }

        // consider the inner predicate.
        Pred inner = body[order[index+1]];

        // set of statements that result in the same subquery.
        List<SPO> ret = new LinkedList<SPO>();
        
        /*
         * collect up a sequence of SPOs from the chunk[] in which the variables
         * shared with the subquery are invariant across the sequence.
         */
        
        while (i < chunk.length) {
        
            if (sharedVars.contains(inner.s)) {
                
                if (chunk[0].s != chunk[i].s)
                    break;
                
            }

            if (sharedVars.contains(inner.p)) {
                
                if (chunk[0].p != chunk[i].p)
                    break;
                
            }

            if (sharedVars.contains(inner.o)) {
                
                if (chunk[0].o != chunk[i].o)
                    break;
                
            }
            
            ret.add(chunk[i++]);
            
        }

        return ret;

    }
    
    private static final List<SPO> EMPTY_SET = Collections
            .unmodifiableList(new LinkedList<SPO>());

}

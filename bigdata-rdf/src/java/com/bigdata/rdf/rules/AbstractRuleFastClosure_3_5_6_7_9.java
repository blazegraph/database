/*

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
package com.bigdata.rdf.rules;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.RelationFusedView;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IRuleTaskFactory;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.Var;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.IStepTask;
import com.bigdata.relation.rule.eval.RuleStats;

/**
 * Rule used in steps 3, 5, 6, 7, and 9 of
 * {@link InferenceEngine#fastForwardClosure()}.
 * 
 * <pre>
 *    (?x, {P}, ?y) -&gt; (?x, propertyId, ?y)
 * </pre>
 * 
 * where <code>{P}</code> is the closure of the subproperties of one of the
 * FIVE (5) reserved keywords:
 * <ul>
 * <li><code>rdfs:subPropertyOf</code></li>
 * <li><code>rdfs:subClassOf</code></li>
 * <li><code>rdfs:domain</code></li>
 * <li><code>rdfs:range</code></li>
 * <li><code>rdf:type</code></li>
 * </ul>
 * 
 * The caller MUST provide a current version of "{P}" when they instantiate this
 * rule.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractRuleFastClosure_3_5_6_7_9 extends Rule {

    // private final Set<Long> P;

//    private final IConstant<Long> rdfsSubPropertyOf;
    
//    private final IConstant<Long> propertyId;

    // private final Var x, y, SetP;
    
    /**
     * @param propertyId
     * @param taskFactory
     *            An implementation returning a concrete instance of
     *            {@link FastClosureRuleTask}.
     */
    public AbstractRuleFastClosure_3_5_6_7_9(//
            String name,
            String relationName,
            final IConstant<Long> rdfsSubPropertyOf,
            final IConstant<Long> propertyId,
            IRuleTaskFactory taskFactory
    // , Set<Long> P
    ) {

        super(name, new SPOPredicate(relationName, var("x"), propertyId,
                var("y")), //
                new SPOPredicate[] {//
                new SPOPredicate(relationName, var("x"), var("{P}"), var("y")) //
                },//
                null, // constraints
                taskFactory
        );

        if (rdfsSubPropertyOf == null)
            throw new IllegalArgumentException();

        if (propertyId == null)
            throw new IllegalArgumentException();
        
        // this.P = P;

//        this.rdfsSubPropertyOf = rdfsSubPropertyOf;
//        
//        this.propertyId = propertyId;

        // this.x = var("x");
        // this.y = var("y");
        // this.SetP = var("{P}");

    }

    /**
     * Custom rule execution task. You must implement {@link #getSet()}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    abstract protected static class FastClosureRuleTask implements IStepTask {

        private final String database;
        
        private final String focusStore;
        
        private final IRule rule;

        private final IJoinNexus joinNexus; // Note: Not serializable

        private final IBuffer<ISolution> buffer; // Note: Not serializable.

        // private final Set<Long> P;

        protected final IConstant<Long> rdfsSubPropertyOf;

        protected final IConstant<Long> propertyId;

        /**
         * @see #getView()
         */
        private transient IRelation<SPO> view = null;
        
        private final static transient long NULL = IRawTripleStore.NULL;

        /**
         * <code>(?x, {P}, ?y) -> (?x, propertyId, ?y)</code>
         * 
         * Note: Both the database and the (optional) focusStore relation names
         * MUST be declared for these rules. While the rule only declares a
         * single tail predicate, there is a "hidden" query based on the
         * [database + focusStore] fused view that populates the P,D,C,R, or T
         * Set which is an input to the custom evaluation of the rule.
         * 
         * @param database
         *            Name of the database relation (required).
         * @param focusStore
         *            Optional name of the focusStore relation (may be null).
         *            When non-<code>null</code>, this is used to query the
         *            fused view of the [database + focusStore] in
         *            {@link FastClosureRuleTask#getView()}.
         * @param rule
         *            The rule.
         * @param joinNexus
         * @param buffer
         *            A buffer used to accumulate entailments.
         * @param rdfsSubPropertyOf
         *            The {@link Constant} corresponding to the term identifier
         *            for <code>rdfs:subPropertyOf</code>.
         * @param propertyId
         *            The propertyId to be used in the assertions.
         */
        public FastClosureRuleTask(//
                String database,
                String focusStore,
                IRule rule,
                IJoinNexus joinNexus,
                IBuffer<ISolution> buffer,
                // Set<Long> P,
                IConstant<Long> rdfsSubPropertyOf,
                IConstant<Long> propertyId) {

            if (database == null)
                throw new IllegalArgumentException();

            if (rule == null)
                throw new IllegalArgumentException();

            if (joinNexus == null)
                throw new IllegalArgumentException();

            if (buffer == null)
                throw new IllegalArgumentException();

            // if (P == null)
            // throw new IllegalArgumentException();

            if (rdfsSubPropertyOf== null)
                throw new IllegalArgumentException();

            if (propertyId == null)
                throw new IllegalArgumentException();

            this.database = database;
            
            this.focusStore = focusStore; // MAY be null.
            
            this.rule = rule;

            this.joinNexus = joinNexus;

            this.buffer = buffer;

            // this.P = P;

            this.rdfsSubPropertyOf = rdfsSubPropertyOf;
            
            this.propertyId = propertyId;

        }

        public RuleStats call() {

            final RuleStats stats = new RuleStats(rule);

            final long begin = System.currentTimeMillis();

            final long timestamp = joinNexus.getReadTimestamp();

            final IResourceLocator resourceLocator = joinNexus
                    .getIndexManager().getResourceLocator();
            
            /*
             * Note: Since this task is always applied to a single tail rule,
             * the {@link TMUtility} rewrite of the rule will always read from
             * the focusStore alone. This makes the choice of the relation on
             * which to read easy - just read on whichever relation is specified
             * for tail[0].
             */
            final SPORelation relation = (SPORelation) resourceLocator
                    .locate(rule.getHead().getOnlyRelationName(), timestamp);

            /*
             * Query for the set {P} rather than requiring it as an input.
             * 
             * Note: This is really aligning relations with different
             * arity/shape (long[1] vs SPO)
             * 
             * @todo Make {P} a chunked iterator, proceed by chunk, and put each
             * chunk into ascending Long[] order. However the methods that
             * compute {P} are a custom closure operator and they fix point {P}
             * in memory. To generalize with a chunked iterator there would need
             * to be a backing BigdataLongSet and that will be less efficient
             * unless the property hierarchy scale is very large.
             */
            final long[] a = getSortedArray(getSet());

            /*
             * For each p in the chunk.
             * 
             * @todo execute subqueries in parallel against shared thread pool.
             */
            for (long p : a) {

                if (p == propertyId.get()) {

                    /*
                     * The rule refuses to consider triple patterns where the
                     * predicate for the subquery is the predicate for the
                     * generated entailments since the support would then entail
                     * itself.
                     */

                    continue;

                }

                stats.nsubqueries[0]++;

                final IAccessPath<SPO> accessPath = relation.getAccessPath(
                        NULL, p, NULL);

                final IChunkedOrderedIterator<SPO> itr2 = accessPath.iterator();

                // ISPOIterator itr2 = (state.focusStore == null ?
                // state.database
                // .getAccessPath(NULL, p, NULL).iterator()
                // : state.focusStore.getAccessPath(NULL, p, NULL)
                // .iterator());

                final IBindingSet bindingSet = joinNexus.newBindingSet(rule);

                try {

                    while (itr2.hasNext()) {

                        final SPO[] chunk = itr2.nextChunk(SPOKeyOrder.POS);

                        stats.chunkCount[0] ++;

                        stats.elementCount[0] += chunk.length;
                        
                        if (log.isDebugEnabled()) {

                            log.debug("stmts1: chunk=" + chunk.length + "\n"
                                    + Arrays.toString(chunk));

                        }

                        for (SPO spo : chunk) {

                            /*
                             * Note: since P includes rdfs:subPropertyOf (as
                             * well as all of the sub-properties of
                             * rdfs:subPropertyOf) there are going to be some
                             * axioms in here that we really do not need to
                             * reassert and generally some explicit statements
                             * as well.
                             * 
                             * @todo so, filter out explicit and axioms?
                             */

                            assert spo.p == p;

                            joinNexus.copyValues(spo, rule.getTail(0),
                                    bindingSet);

                            if (rule.isConsistent(bindingSet)) {

                                buffer.add(joinNexus.newSolution(rule,
                                        bindingSet));

                                stats.solutionCount++;

                            }

                        } // next stmt

                    } // while(itr2)

                } finally {

                    itr2.close();

                }

            } // next p in {P}

            stats.elapsed += System.currentTimeMillis() - begin;

            return stats;

        }

        /**
         * Convert a {@link Set} of term identifiers into a sorted array of term
         * identifiers.
         * <P>
         * Note: When issuing multiple queries against the database, it is
         * generally faster to issue those queries in key order.
         * 
         * @return The sorted term identifiers.
         */
        protected long[] getSortedArray(Set<Long> ids) {

            int n = ids.size();

            long[] a = new long[n];

            int i = 0;

            for (Long id : ids) {

                a[i++] = id;

            }

            Arrays.sort(a);

            return a;

        }

        /**
         * Return the {@link IRelation} (or {@link RelationFusedView}) used by
         * the {@link #getSet()} impls for their {@link IAccessPath}s.
         */
        synchronized protected IRelation<SPO> getView() {

            if (view == null) {
             
                /*
                 * Setup the [database] or [database + focusStore] view used to
                 * compute the closure.
                 */
                
                final IResourceLocator resourceLocator = joinNexus
                        .getIndexManager().getResourceLocator();
                
                final long timestamp = joinNexus.getReadTimestamp();
                
                if (focusStore == null) {

                    return (IRelation<SPO>)resourceLocator.locate(database, timestamp);

                } else {

                    return new RelationFusedView<SPO>(
                            //
                            (IRelation<SPO>)resourceLocator.locate(database, timestamp),
                            (IRelation<SPO>)resourceLocator.locate(focusStore, timestamp));

                }
                    // final IAccessPath accessPath = (focusStore == null //
                    // ? database.getAccessPath(NULL, p, NULL)//
                    // : new AccessPathFusedView(focusStore
                    // .getAccessPath(NULL, p, NULL), //
                    // database.getAccessPath(NULL, p, NULL)//
                    // ));

                
            }
            
            return view;
            
        }        

        /**
         * Return the set of term identifiers that will be processed by the
         * rule. When the closure is being computed for truth maintenance the
         * implementation MUST read from the [database+focusStore] fused view.
         * Otherwise it reads from the database.
         * <p>
         * Note: The subclass need only invoke {@link #getSubProperties()} or
         * {@link #getSubPropertiesOf(IConstant)} as appropriate for the rule.
         * 
         * @return The set.
         */
        abstract protected Set<Long> getSet();
        
        /**
         * Delegates to {@link SubPropertyClosureTask}
         * 
         * @return The closure.
         */
        protected Set<Long> getSubProperties() {

            return new SubPropertyClosureTask(getView(), rdfsSubPropertyOf).call();

        }

        /**
         * Delegates to {@link SubPropertiesOfClosureTask}
         * 
         * @param propertyId
         *            The property of interest.
         * 
         * @return The closure.
         */
        protected Set<Long> getSubPropertiesOf(IConstant<Long> propertyId) {

            return new SubPropertiesOfClosureTask(getView(), rdfsSubPropertyOf,
                    propertyId).call();

        }
        
    } // FastClosureRuleTask

    /**
     * Computes the set of possible sub properties of rdfs:subPropertyOf (<code>P</code>).
     * This is used by step <code>2</code> in
     * {@link RDFSVocabulary#fastForwardClosure()}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class SubPropertyClosureTask implements Callable<Set<Long>> {
        
        final static protected Logger log = Logger.getLogger(SubPropertyClosureTask.class);

        private final IRelation<SPO> view; // Note: Not serializable.
        private final IConstant<Long> rdfsSubPropertyOf;
        
        public SubPropertyClosureTask(IRelation<SPO> view,
                IConstant<Long> rdfsSubPropertyOf) {

            if (view == null)
                throw new IllegalArgumentException();

            if (rdfsSubPropertyOf == null)
                throw new IllegalArgumentException();
            
            this.view = view;
            
            this.rdfsSubPropertyOf = rdfsSubPropertyOf;
            
        }
        
        public Set<Long> call() {
            
            return getSubProperties();
            
        }
        
        /**
         * Compute the closure.
         * 
         * @return A set containing the term identifiers for the members of P.
         */
        public Set<Long> getSubProperties() {

            final Set<Long> P = new HashSet<Long>();

            P.add(rdfsSubPropertyOf.get());

            /*
             * query := (?x, P, P), adding new members to P until P reaches fix
             * point.
             */
            {

                int nbefore;
                int nafter = 0;
                int nrounds = 0;

                final Set<Long> tmp = new HashSet<Long>();

                do {

                    nbefore = P.size();

                    tmp.clear();

                    /*
                     * query := (?x, p, ?y ) for each p in P, filter ?y element
                     * of P.
                     */

                    for (Long p : P) {

                        final SPOPredicate pred = new SPOPredicate(//
                                "view",//
                                Var.var("x"), new Constant<Long>(p), Var.var("y")//
                                );
                        
                        final IAccessPath<SPO> accessPath = view
                                .getAccessPath(pred);
                        
//                        final IAccessPath accessPath = (focusStore == null //
//                        ? database.getAccessPath(NULL, p, NULL)//
//                                : new AccessPathFusedView(focusStore
//                                        .getAccessPath(NULL, p, NULL), //
//                                        database.getAccessPath(NULL, p, NULL)//
//                                ));

                        final IChunkedOrderedIterator<SPO> itr = accessPath.iterator();

                        try {

                            while (itr.hasNext()) {

                                SPO[] stmts = itr.nextChunk();

                                for (SPO stmt : stmts) {

                                    if (P.contains(stmt.o)) {

                                        tmp.add(stmt.s);

                                    }

                                }

                            }
                        } finally {

                            itr.close();

                        }

                    }

                    P.addAll(tmp);

                    nafter = P.size();

                    nrounds++;

                } while (nafter > nbefore);

            }

//            if (log.isDebugEnabled()) {
//
//                Set<String> terms = new HashSet<String>();
//
//                for (Long id : P) {
//
//                    terms.add(database.toString(id));
//
//                }
//
//                log.debug("P: " + terms);
//
//            }

            return P;

        }

    }

    /**
     * Query the <i>database</i> for the sub properties of a given
     * property.
     * <p>
     * Pre-condition: The closure of <code>rdfs:subPropertyOf</code> has
     * been asserted on the database.
     * 
     * @param p
     *            The constant corresponding to the term identifier for the
     *            property whose sub-properties will be obtain.
     * 
     * @return A set containing the term identifiers for the sub properties
     *         of <i>p</i>.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class SubPropertiesOfClosureTask implements Callable<Set<Long>> {

        final static protected Logger log = Logger.getLogger(SubPropertyClosureTask.class);

        private final IRelation<SPO> view; // Note: Not serializable.
        private final IConstant<Long> rdfsSubPropertyOf;
        private final IConstant<Long> p;
        
        public SubPropertiesOfClosureTask(IRelation<SPO> view,
                IConstant<Long> rdfsSubPropertyOf, IConstant<Long> p) {
            
            if (view == null)
                throw new IllegalArgumentException();
            
            if (rdfsSubPropertyOf == null)
                throw new IllegalArgumentException();
            
            if (p == null)
                throw new IllegalArgumentException();
            
            this.view = view;
            
            this.rdfsSubPropertyOf = rdfsSubPropertyOf;
            
            this.p = p;
            
        }

        public Set<Long> call() {
            
            return getSubPropertiesOf(p);
            
        }

        /**
         * Compute the closure.
         * 
         * @param p
         *            The property of interest.
         *            
         * @return The closure.
         */
        public Set<Long> getSubPropertiesOf(IConstant<Long> p) {

            final SPOPredicate pred = new SPOPredicate(//
                    "view", //
                    Var.var("x"), rdfsSubPropertyOf, p//
            );
            
            final IAccessPath<SPO> accessPath = view.getAccessPath(pred);
            
//            final IAccessPath accessPath = //
//            (focusStore == null //
//            ? database.getAccessPath(NULL/* x */, rdfsSubPropertyOf.get(), p)//
//                    : new AccessPathFusedView(//
//                            focusStore.getAccessPath(NULL/* x */,
//                                    rdfsSubPropertyOf.get(), p), //
//                            database.getAccessPath(NULL/* x */,
//                                    rdfsSubPropertyOf.get(), p)//
//                    ));

//            if (log.isDebugEnabled()) {
//
//                log.debug("p=" + database.toString(p));
//
//            }

            final Set<Long> tmp = new HashSet<Long>();

            /*
             * query := (?x, rdfs:subPropertyOf, p).
             * 
             * Distinct ?x are gathered in [tmp].
             * 
             * Note: This query is two-bound on the POS index.
             */

            final IChunkedOrderedIterator<SPO> itr = accessPath.iterator();

            try {

                while (itr.hasNext()) {

                    SPO[] stmts = itr.nextChunk();

                    for (SPO spo : stmts) {

                        boolean added = tmp.add(spo.s);

                        if (log.isDebugEnabled()) {

                            log.debug(spo.toString(/* database */)
                                    + ", added subject=" + added);

                        }

                    }

                }

            } finally {

                itr.close();
            }

//            if (log.isDebugEnabled()) {
//
//                Set<String> terms = new HashSet<String>();
//
//                for (Long id : tmp) {
//
//                    terms.add(database.toString(id));
//
//                }
//
//                log.debug("sub properties: " + terms);
//
//            }

            return tmp;

        }

    }
    
}

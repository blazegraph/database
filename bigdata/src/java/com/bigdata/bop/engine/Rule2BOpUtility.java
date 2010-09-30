/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Sep 5, 2010
 */

package com.bigdata.bop.engine;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.rdf.join.DataSetJoin;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.sail.BigdataEvaluationStrategyImpl;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.spo.DefaultGraphSolutionExpander;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.InGraphHashSetFilter;
import com.bigdata.rdf.spo.NamedGraphSolutionExpander;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.accesspath.ElementFilter;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IStep;
import com.bigdata.relation.rule.eval.DefaultEvaluationPlan2;
import com.bigdata.relation.rule.eval.IRangeCountFactory;
import com.bigdata.relation.rule.eval.RuleState;

/**
 * Utility class converts {@link IRule}s to {@link BOp}s.
 * <p>
 * Note: This is a stopgap measure designed to allow us to evaluate SPARQL
 * queries and verify the standalone {@link QueryEngine} while we develop a
 * direct translation from Sesame's SPARQL operator tree onto {@link BOp}s and
 * work on the scale-out query buffer transfer mechanisms.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Rule2BOpUtility {

    protected static final Logger log = Logger.getLogger(Rule2BOpUtility.class);

    /**
     * Flag to conditionally enable the new named and default graph support.
     * <p>
     * Note: When enabled, the {@link NamedGraphSolutionExpander} and
     * {@link DefaultGraphSolutionExpander} must be stripped from the
     * {@link IPredicate.Annotations#EXPANDER}. In the long term, we will simply
     * no longer generate them in {@link BigdataEvaluationStrategyImpl}.
     * <p>
     * Note: If you want to test just the named graph stuff, then the default
     * graph processing could be handed off to the
     * {@link DefaultGraphSolutionExpander}.
     */
    private static boolean enableDecisionTree = false;
    
    /**
     * Annotations used by the {@link BigdataEvaluationStrategyImpl} to
     * communicate with the {@link Rule2BOpUtility}.
     * <p>
     * <h3>Quads Mode</h3>
     * Several annotations are used to mark named and default graph patterns on
     * the {@link IPredicate}s. Rather than attaching a named or default graph
     * expander, we annotate the predicate with the metadata for the access path
     * and then convert that annotation to the appropriate bop pattern in
     * {@link Rule2BOpUtility}.
     */
    public interface Annotations {

        /**
         * Boolean flag indicates that the database is operating in quads mode.
         */
        String QUADS = Rule2BOpUtility.class.getName() + ".quads";

        boolean DEFAULT_QUADS = false;

        /**
         * The {@link Dataset} associated with the access path (quads mode
         * only). The {@link Dataset} is only provided by openrdf when FROM or
         * FROM NAMED was used in the query. Otherwise the {@link Dataset} will
         * be <code>null</code> and is not attached as an annotation.
         * <p>
         * Note: This annotation MUST be stripped from the query plan to prevent
         * an attempt to serialized it for RMI in scale-out (the {@link Dataset}
         * is not {@link Serializable}, can be quite large, and is captured by
         * other constructions in the generated query plan).
         */
        String DATASET = Rule2BOpUtility.class.getName() + ".dataset";

        /**
         * The {@link Scope} of the access path (quads mode only). In quads mode
         * the {@link Scope} is always provided by openrdf.
         * 
         * @see Scope#NAMED_CONTEXTS
         * @see Scope#DEFAULT_CONTEXTS
         */
        String SCOPE = Rule2BOpUtility.class.getName() + ".scope";

        /**
         * The graph variable specified in the query (quads mode only). This is
         * <p>
         * Note: This is not used for SIDs mode because we use the standard
         * triple store access paths.
         * 
         * @see org.openrdf.query.algebra.Var
         * 
         * @todo can we just use pred.get(3)?
         */
        String CVAR = Rule2BOpUtility.class.getName() + ".cvar";

        /*
         * Cost estimates.
         */

        /**
         * The estimated cost of a SCAN + FILTER approach to a default graph or
         * named graph query.
         */
        String COST_SCAN = Rule2BOpUtility.class.getName() + ".costScan";

        /**
         * The estimated cost of a SUBQUERY approach to a default graph or named
         * graph query.
         */
        String COST_SUBQUERY = Rule2BOpUtility.class.getName()
                + ".costSubquery";

        /**
         * The #of known graphs in the {@link Dataset} for a default graph or
         * named graph query.
         */
        String NKNOWN = Rule2BOpUtility.class.getName() + ".nknown";

    }

    /**
     * A list of annotations to be cleared from {@link Predicate} when they are
     * copied into a query plan.
     */
    private static final String[] ANNS_TO_CLEAR_FROM_PREDICATE = new String[] {
            Annotations.QUADS,//
            Annotations.DATASET,//
            Annotations.SCOPE,//
            Annotations.CVAR,//
            IPredicate.Annotations.OPTIONAL //
    };
    
    /**
     * Convert an {@link IStep} into an operator tree. This should handle
     * {@link IRule}s and {@link IProgram}s as they are currently implemented
     * and used by the {@link BigdataSail}.
     * 
     * @param step
     *            The step.
     * 
     * @return
     */
    public static PipelineOp convert(final IStep step, final int startId,
            final AbstractTripleStore db, final QueryEngine queryEngine) {

        if (step instanceof IRule)
            return convert((IRule) step, startId, db, queryEngine);
        
        return convert((IProgram) step, startId, db, queryEngine);

    }

    /**
     * Convert a rule into an operator tree.
     * 
     * @param rule
     * 
     * @return
     */
    public static PipelineOp convert(final IRule rule, final int startId,
            final AbstractTripleStore db, final QueryEngine queryEngine) {

        int bopId = startId;

        final PipelineOp startOp = new StartOp(new BOp[] {},
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, bopId++),//
                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        }));
        
        /*
         * First put the tails in the correct order based on the logic in
         * DefaultEvaluationPlan2.
         */
        final BOpContextBase context = new BOpContextBase(queryEngine);
        final DefaultEvaluationPlan2 plan = new DefaultEvaluationPlan2(
                new IRangeCountFactory() {

            public long rangeCount(final IPredicate pred) {
                return context.getRelation(pred).getAccessPath(pred)
                                .rangeCount(false);
            }
            
        }, rule);
        
        // evaluation plan order.
        final int[] order = plan.getOrder();
        
        // variables to be retained for each join.
        final IVariable[][] selectVars = RuleState
                .computeRequiredVarsForEachTail(rule, order);
        
        /*
         * Map the constraints from the variables they use.  This way, we can
         * properly attach constraints to only the first tail in which the
         * variable appears.  This way we only run the appropriate constraint
         * once, instead of for every tail. 
         */
        final Map<IVariable<?>, Collection<IConstraint>> constraintsByVar = 
            new HashMap<IVariable<?>, Collection<IConstraint>>();
        for (int i = 0; i < rule.getConstraintCount(); i++) {
            final IConstraint c = rule.getConstraint(i);
            
            if (log.isDebugEnabled()) {
                log.debug(c);
            }
            
            final Set<IVariable<?>> uniqueVars = new HashSet<IVariable<?>>();
            final Iterator<IVariable<?>> vars = BOpUtility.getSpannedVariables(c);
            while (vars.hasNext()) {
                final IVariable<?> v = vars.next();
                uniqueVars.add(v);
            }
            
            for (IVariable<?> v : uniqueVars) {

                if (log.isDebugEnabled()) {
                    log.debug(v);
                }
                
                Collection<IConstraint> constraints = constraintsByVar.get(v);
                if (constraints == null) {
                    constraints = new LinkedList<IConstraint>();
                    constraintsByVar.put(v, constraints);
                }
                constraints.add(c);
            }
        }
        
        PipelineOp left = startOp;
        
        for (int i = 0; i < order.length; i++) {
            
            final int joinId = bopId++;
            
            // assign a bop id to the predicate
            Predicate<?> pred = (Predicate<?>) rule.getTail(order[i]).setBOpId(
                    bopId++);

            /*
             * Collect all the constraints for this predicate based on which
             * variables make their first appearance in this tail
             */
            final Collection<IConstraint> constraints = 
                new LinkedList<IConstraint>();
            
            /*
             * Peek through the predicate's args to find its variables. Use
             * these to attach constraints to the join based on the variables
             * that make their first appearance in this tail.
             */
            for (BOp arg : pred.args()) {
                if (arg instanceof IVariable<?>) {
                    final IVariable<?> v = (IVariable<?>) arg;
                    /*
                     * We do a remove because we don't ever need to run these
                     * constraints again during subsequent joins once they
                     * have been run once at the initial appearance of the
                     * variable.
                     * 
                     * FIXME revisit this when we dynamically re-order running
                     *          joins
                     */ 
                    if (constraintsByVar.containsKey(v))
                        constraints.addAll(constraintsByVar.remove(v));
                }
            }
           
            // annotations for this join.
            final List<NV> anns = new LinkedList<NV>();
            
            anns.add(new NV(BOp.Annotations.BOP_ID, joinId));

            anns.add(new NV(PipelineJoin.Annotations.SELECT,
                    selectVars[order[i]]));
            
            if (pred.isOptional())
                anns.add(new NV(PipelineJoin.Annotations.OPTIONAL, pred
                        .isOptional()));
            
            if (!constraints.isEmpty())
                anns.add(new NV(PipelineJoin.Annotations.CONSTRAINTS,
                        constraints
                                .toArray(new IConstraint[constraints.size()])));
            
            final Scope scope = (Scope) pred.getProperty(Annotations.SCOPE);

            // @todo can we just use pred.get(3)?
            final org.openrdf.query.algebra.Var cvar = (org.openrdf.query.algebra.Var) pred
                    .getProperty(Annotations.CVAR);

            // true iff this is a quads access path.
            final boolean quads = pred.getProperty(Annotations.QUADS,
                    Annotations.DEFAULT_QUADS);

            // strip off annotations that we do not want to propagate.
            pred = pred.clearAnnotations(ANNS_TO_CLEAR_FROM_PREDICATE);

            if (quads) {

                /*
                 * Quads mode.
                 */

                if (enableDecisionTree) {
                    /*
                     * Strip off the named graph or default graph expander (in
                     * the long term it will simply not be generated.)
                     */
                    pred = pred
                            .clearAnnotations(new String[] { IPredicate.Annotations.EXPANDER });

                    switch (scope) {
                    case NAMED_CONTEXTS:
                        left = namedGraphJoin(queryEngine, left, anns, pred,
                                cvar);
                        break;
                    case DEFAULT_CONTEXTS:
                        left = defaultGraphJoin(queryEngine, left, anns, pred,
                                cvar);
                        break;
                    default:
                        throw new AssertionError();
                    }
                    
                } else {

                    /*
                     * This is basically the old way of handling quads query
                     * using expanders which were attached by
                     * BigdataEvaluationStrategyImpl.
                     */
                    
                    final boolean scaleOut = queryEngine.isScaleOut();
                    if (scaleOut)
                        throw new UnsupportedOperationException();
                    
                    anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.ANY));

                    left = new PipelineJoin(new BOp[] { left, pred }, anns
                            .toArray(new NV[anns.size()]));

                }

            } else {

                /*
                 * Triples or provenance mode.
                 */

                left = triplesModeJoin(queryEngine, left, anns, pred);

            }

        }
        
        // just for now while i'm debugging
        System.err.println(toString(left));
        
        return left;
        
    }

    /**
     * Generate a {@link PipelineJoin} for a triples mode access path.
     * 
     * @param queryEngine
     * @param left
     * @param anns
     * @param pred
     * 
     * @return The join operator.
     */
    private static PipelineOp triplesModeJoin(final QueryEngine queryEngine,
            final PipelineOp left, final List<NV> anns, final Predicate pred) {

        final boolean scaleOut = queryEngine.isScaleOut();
        if (scaleOut) {
            anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                    BOpEvaluationContext.SHARDED));
        } else {
            anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                    BOpEvaluationContext.ANY));
        }

        return new PipelineJoin(new BOp[] { left, pred }, anns
                .toArray(new NV[anns.size()]));

    }

    /**
     * Generate a named graph join (quads mode).
     * 
     * @param queryEngine
     * @param left
     * @param anns
     * @param pred
     * @param cvar
     * @return
     */
    private static PipelineOp namedGraphJoin(final QueryEngine queryEngine,
            final PipelineOp left, final List<NV> anns, Predicate pred,
            final org.openrdf.query.algebra.Var cvar) {

        final Dataset dataset = (Dataset) pred.getProperty(Annotations.DATASET);

        final boolean scaleOut = queryEngine.isScaleOut();
        if (scaleOut) {
            anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                    BOpEvaluationContext.SHARDED));
        } else {
            anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                    BOpEvaluationContext.ANY));
        }

        final DataSetSummary summary = new DataSetSummary(dataset
                .getNamedGraphs());

        anns.add(new NV(Annotations.NKNOWN, summary.nknown));

        // true iff C is bound to a constant.
        final boolean isCBound = cvar.getValue() != null;
        
        if (isCBound) {

            /*
             * C is already bound.  The unmodified access path is used. 
             */

            return new PipelineJoin(new BOp[] { left, pred }, anns
                    .toArray(new NV[anns.size()]));

        } else if (summary.nknown == 0) {

            /*
             * The data set is empty (no graphs). Return a join backed by an
             * empty access path.
             * 
             * Note: Since the join could be optional or part of an optional
             * join group, we can not just drop it. Instead we need to return a
             * join against an empty access path. Since the join could also
             * "select" for some subset of variables, it seems that we really
             * need to modify PipelineJoin to recognize an annotation indicating
             * an empty access path. It can then substitute the empty access
             * path when processing the source binding sets. There should be
             * unit tests for this.
             * 
             * FIXME Return PipelineJoin with an EMPTY ACCESS PATH.
             */
            
            throw new UnsupportedOperationException();
            
        } else if (summary.nknown == 1) {

            /*
             * The dataset contains exactly one graph. Bind C.
             */
            
            pred = pred.asBound((IVariable) pred.get(3), new Constant(
                    summary.firstContext));
            
            return new PipelineJoin(new BOp[] { left, pred }, anns
                    .toArray(new NV[anns.size()]));

        } else if (dataset == null) {

            /*
             * The dataset is all graphs. C is left unbound and the unmodified
             * access path is used.
             */

            return new PipelineJoin(new BOp[] { left, pred }, anns
                    .toArray(new NV[anns.size()]));

        } else {

            /*
             * Estimate cost of SCAN with C unbound)
             */
            final double scanCost = getScanCost(pred);

            anns.add(new NV(Annotations.COST_SCAN, scanCost));

            /*
             * Estimate cost of SUBQUERY with C bound.
             */
            final double subqueryCost = getSubqueryCost(pred);

            anns.add(new NV(Annotations.COST_SUBQUERY, subqueryCost));

            if (scanCost < subqueryCost * summary.nknown) {

                /*
                 * Scan and filter. C is left unbound. We do a range scan on the
                 * index and filter using an IN constraint.
                 */

                // IN filter for the named graphs.
                final IElementFilter<ISPO> test = new InGraphHashSetFilter<ISPO>(
                        summary.nknown, summary.graphs);

                // layer filter onto the predicate.
                pred = pred
                        .addIndexLocalFilter(ElementFilter.newInstance(test));
                
                return new PipelineJoin(new BOp[] { left, pred }, anns
                        .toArray(new NV[anns.size()]));

            } else {

                /*
                 * Parallel Subquery.
                 */

                /*
                 * Setup the data set join.
                 * 
                 * @todo When the #of named graphs is large we need to do
                 * something special to avoid sending huge graph sets around
                 * with the query. For example, we should create named data sets
                 * and join against them rather than having an in-memory
                 * DataSetJoin.
                 * 
                 * @todo The historical approach performed parallel subquery
                 * using an expander pattern rather than a data set join. The
                 * data set join should have very much the same effect, but it
                 * may need to emit multiple chunks to have good parallelism.
                 */

                // The variable to be bound.
                final IVariable var = (IVariable) pred.get(3);
                
                // The data set join.
                final DataSetJoin dataSetJoin = new DataSetJoin(
                        new BOp[] { var }, NV.asMap(new NV[] {
                                new NV(DataSetJoin.Annotations.VAR, var),
                                new NV(DataSetJoin.Annotations.GRAPHS, summary
                                        .getGraphs()) }));

                if (scaleOut) {
                    anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.SHARDED));
                    anns.add(new NV(Predicate.Annotations.REMOTE_ACCESS_PATH,
                            false));
                } else {
                    anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.ANY));
                    anns.add(new NV(Predicate.Annotations.REMOTE_ACCESS_PATH,
                            false));
                }

                return new PipelineJoin(new BOp[] { left, pred }, anns
                        .toArray(new NV[anns.size()]));

            }

        }

    }

    /**
     * 
     * @param pred
     * @return
     * 
     * FIXME Cost models have been implemented, but are not yet hooked in.
     */
    static double getScanCost(Predicate pred) {
        /*
         * @todo Scan is more expensive on the Journal so this is set to ONE (1)
         * and subquery is set to ZERO (0). This will get replaced by the actual
         * computed costs shortly.
         */
        return 1d;
    }

    /**
     * 
     * @param pred
     * @return
     * 
     *         FIXME Cost models have been implemented, but are not yet hooked
     *         in.
     */
    static double getSubqueryCost(Predicate pred) {
        return 0d;
    }

    /**
     * Generate a default graph join (quads mode).
     * 
     * @param queryEngine
     * @param left
     * @param anns
     * @param pred
     * @return
     * 
     * @todo The default graph remote access path query estimates do not take
     *       RMI costs into account. This is Ok since we are only comparing
     *       remote access paths with other remote access paths.
     */
    private static PipelineOp defaultGraphJoin(final QueryEngine queryEngine,
            final PipelineOp left, final List<NV> anns, final Predicate pred,
            final org.openrdf.query.algebra.Var cvar) {

        // @todo decision of local vs remote ap.
        final boolean scaleOut = queryEngine.isScaleOut();
        if (scaleOut) {
            anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                    BOpEvaluationContext.SHARDED));
        } else {
            anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                    BOpEvaluationContext.ANY));
        }

        /*
         * FIXME implement the default graph decision tree. 
         */
        throw new UnsupportedOperationException();

    }

    /**
     * Pretty print (aspects of) a bop.
     * 
     * @param bop
     *            The bop.
     *            
     * @return The formatted representation.
     */
    private static String toString(final BOp bop) {
        
        StringBuilder sb = new StringBuilder();
        
        toString(bop, sb, 0);
        
        // chop off the last \n
        sb.setLength(sb.length() - 1);

        return sb.toString();

    }

    private static void toString(final BOp bop, final StringBuilder sb,
            final int indent) {

        for (int i = 0; i < indent; i++) {
            sb.append(' ');
        }
        sb.append(bop).append('\n');

        if (bop != null) {
            final List<BOp> args = bop.args();
            for (BOp arg : args) {
                toString(arg, sb, indent + 4);
            }
            final IConstraint[] constraints = (IConstraint[]) bop
                    .getProperty(PipelineJoin.Annotations.CONSTRAINTS);
            if (constraints != null) {
                for (IConstraint c : constraints) {
                    toString(c, sb, indent + 4);
                }
            }
        }

    }
    
    /**
     * Convert a program into an operator tree.
     * 
     * @param program
     * 
     * @return
     * 
     * FIXME What is the pattern for UNION?
     */
    public static PipelineOp convert(final IProgram rule, final int startId,
            final AbstractTripleStore db, final QueryEngine queryEngine) {

        throw new UnsupportedOperationException();

    }

    /**
     * Helper class summarizes the named graphs for a quads mode query.
     * 
     * @todo This could be used for either named or default graphs. All it does
     *       not report the #of URIs known to the database.
     * 
     * @todo This summary could be computed once for a given query for its named
     *       graphs and once for its default graph. We do not need to do this
     *       for each predicate in the query.
     */
    private static class DataSetSummary {

        /**
         * The set of graphs. The {@link URI}s MUST have been resolved against
         * the appropriate {@link LexiconRelation} such that their term
         * identifiers (when the exist) are known. If any term identifier is
         * {@link IRawTripleStore#NULL}, then the corresponding graph does not
         * exist and no access path will be queried for that graph. However, a
         * non- {@link IRawTripleStore#NULL} term identifier may also identify a
         * graph which does not exist, in which case an access path will be
         * created for that {@link URI}s but will not visit any data.
         */
        public final Iterable<? extends URI> graphs;

        /**
         * The #of graphs in {@link #graphs} whose term identifier is known.
         * While this is not proof that there is data in the quad store for a
         * graph having the corresponding {@link URI}, it does allow the
         * possibility that a graph could exist for that {@link URI}.
         */
        public final int nknown;
//        * <p>
//        * If {@link #nknown} is ZERO (0), then the access path is empty.
//        * <p>
//        * If {@link #nknown} is ONE (1), then the caller's {@link IAccessPath}
//        * should be used and filtered to remove the context information. If
//        * {@link #graphs} is <code>null</code>, which implies that ALL graphs
//        * in the quad store will be used as the default graph, then
//        * {@link #nknown} will be {@link Integer#MAX_VALUE}.

        /**
         * The term identifier for the first graph and
         * {@link IRawTripleStore#NULL} if no graphs were specified having a
         * term identifier.
         */
        public final IV firstContext;

        /**
         * 
         * @param graphs
         *            The set of named graphs in the SPARQL DATASET (optional).
         *            A runtime exception will be thrown during evaluation of
         *            the if the {@link URI}s are not {@link BigdataURI}s. If
         *            <code>graphs := null</code>, then the set of named graphs
         *            is understood to be ALL graphs in the quad store.
         */
        public DataSetSummary(final Iterable<? extends URI> graphs) {

            this.graphs = graphs;

            IV firstContext = null;

            if (graphs == null) {

                nknown = Integer.MAX_VALUE;

            } else {

                final Iterator<? extends URI> itr = graphs.iterator();

                int nknown = 0;

                while (itr.hasNext()) {

                    final BigdataURI uri = (BigdataURI) itr.next();

                    if (uri.getIV() != null) {

                        if (++nknown == 1) {

                            firstContext = uri.getIV();

                        }

                    }

                } // while

                this.nknown = nknown;

            }

            this.firstContext = firstContext;

        }

        /**
         * Return a dense array of the {@link IV}s for the graphs known to the
         * database.
         */
        public IV[] getGraphs() {
            
            final IV[] a = new IV[nknown];
            
            final Iterator<? extends URI> itr = graphs.iterator();

            int nknown = 0;

            while (itr.hasNext()) {

                final BigdataURI uri = (BigdataURI) itr.next();

                final IV id = uri.getIV();

                if (id != null) {

                    a[nknown++] = id;

                }

            } // while
            
            return a;
            
        }
        
    } // DataSetSummary

}

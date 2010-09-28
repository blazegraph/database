package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Compare;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Group;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.MultiProjection;
import org.openrdf.query.algebra.Or;
import org.openrdf.query.algebra.Order;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.SameTerm;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.query.algebra.StatementPattern.Scope;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.openrdf.query.algebra.evaluation.iterator.FilterIterator;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import com.bigdata.BigdataStatics;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.HashBindingSet;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.constraint.EQ;
import com.bigdata.bop.constraint.EQConstant;
import com.bigdata.bop.constraint.INBinarySearch;
import com.bigdata.bop.constraint.NE;
import com.bigdata.bop.constraint.NEConstant;
import com.bigdata.bop.constraint.OR;
import com.bigdata.bop.engine.LocalChunkMessage;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.engine.Rule2BOpUtility;
import com.bigdata.bop.engine.RunningQuery;
import com.bigdata.bop.solutions.ISortOrder;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.rdf.internal.DummyIV;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.constraints.InlineEQ;
import com.bigdata.rdf.internal.constraints.InlineGE;
import com.bigdata.rdf.internal.constraints.InlineGT;
import com.bigdata.rdf.internal.constraints.InlineLE;
import com.bigdata.rdf.internal.constraints.InlineLT;
import com.bigdata.rdf.internal.constraints.InlineNE;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.spo.DefaultGraphSolutionExpander;
import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.NamedGraphSolutionExpander;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.spo.SPOStarJoin;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.store.BigdataBindingSetResolverator;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IQueryOptions;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.ISolutionExpander;
import com.bigdata.relation.rule.IStep;
import com.bigdata.relation.rule.Program;
import com.bigdata.relation.rule.QueryOptions;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.eval.IRuleTaskFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.NestedSubqueryWithJoinThreadsTask;
import com.bigdata.relation.rule.eval.RuleStats;
import com.bigdata.search.FullTextIndex;
import com.bigdata.search.IHit;
import com.bigdata.striterator.ChunkedArraysIterator;
import com.bigdata.striterator.DistinctFilter;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Extended to rewrite Sesame {@link TupleExpr}s onto native {@link Rule}s and
 * to evaluate magic predicates for full text search, etc. Query evaluation can
 * proceed either by Sesame 2 evaluation or, if {@link Options#NATIVE_JOINS} is
 * enabled, then by translation of Sesame 2 query expressions into native
 * {@link IRule}s and native evaluation of those {@link IRule}s.
 * 
 * <h2>Query options</h2>
 * The following summarizes how various high-level query language feature are
 * mapped onto native {@link IRule}s.
 * <dl>
 * <dt>DISTINCT</dt>
 * <dd>{@link IQueryOptions#isDistinct()}, which is realized using
 * {@link DistinctFilter}.</dd>
 * <dt>ORDER BY</dt>
 * <dd>{@link IQueryOptions#getOrderBy()} is effected by a custom
 * {@link IKeyBuilderFactory} which generates sort keys that capture the desired
 * sort order from the bindings in an {@link ISolution}. Unless DISTINCT is
 * also specified, the generated sort keys are made unique by appending a one up
 * long integer to the key - this prevents sort keys that otherwise compare as
 * equals from dropping solutions. Note that the SORT is actually imposed by the
 * {@link DistinctFilter} using an {@link IKeyBuilderFactory} assembled from the
 * ORDER BY constraints.
 * 
 * FIXME BryanT - implement the {@link IKeyBuilderFactory}.
 * 
 * FIXME MikeP - assemble the {@link ISortOrder}[] from the query and set on
 * the {@link IQueryOptions}.</dd>
 * <dt>OFFSET and LIMIT</dt>
 * <dd>
 * <p>
 * {@link IQueryOptions#getSlice()}, which is effected as a conditional in
 * {@link NestedSubqueryWithJoinThreadsTask} based on the
 * {@link RuleStats#solutionCount}. Query {@link ISolution}s are counted as
 * they are generated, but they are only entered into the {@link ISolution}
 * {@link IBuffer} when the solutionCount is GE the OFFSET and LT the LIMIT.
 * Query evaluation halts once the LIMIT is reached.
 * </p>
 * <p>
 * Note that when DISTINCT and either LIMIT and/or OFFSET are specified
 * together, then the LIMIT and OFFSET <strong>MUST</strong> be applied after
 * the solutions have been generated since we may have to generate more than
 * LIMIT solutions in order to have LIMIT <em>DISTINCT</em> solutions. We
 * handle this for now by NOT translating the LIMIT and OFFSET onto the
 * {@link IRule} and instead let Sesame close the iterator once it has enough
 * solutions.
 * </p>
 * <p>
 * Note that LIMIT and SLICE requires an evaluation plan that provides stable
 * results. For a simple query this is achieved by setting
 * {@link IQueryOptions#isStable()} to <code>true</code>.
 * <p>
 * For a UNION query, you must also set {@link IProgram#isParallel()} to
 * <code>false</code> to prevent parallelized execution of the {@link IRule}s
 * in the {@link IProgram}.
 * </p>
 * </dd>
 * <dt>UNION</dt>
 * <dd>A UNION is translated into an {@link IProgram} consisting of one
 * {@link IRule} for each clause in the UNION.
 * 
 * FIXME MikeP - implement.</dd>
 * </dl>
 * <h2>Filters</h2>
 * The following provides a summary of how various kinds of FILTER are handled.
 * A filter that is not explicitly handled is left untranslated and will be
 * applied by Sesame against the generated {@link ISolution}s.
 * <p>
 * Whenever possible, a FILTER is translated into an {@link IConstraint} on an
 * {@link IPredicate} in the generated native {@link IRule}. Some filters are
 * essentially JOINs against the {@link LexiconRelation}. Those can be handled
 * either as JOINs (generating an additional {@link IPredicate} in the
 * {@link IRule}) or as an {@link INBinarySearch} constraint, where the inclusion set is
 * pre-populated by some operation on the {@link LexiconRelation}.
 * <dl>
 * <dt>EQ</dt>
 * <dd>Translated into an {@link EQ} constraint on an {@link IPredicate}.</dd>
 * <dt>NE</dt>
 * <dd>Translated into an {@link NE} constraint on an {@link IPredicate}.</dd>
 * <dt>IN</dt>
 * <dd>Translated into an {@link INBinarySearch} constraint on an {@link IPredicate}.</dd>
 * <dt>OR</dt>
 * <dd>Translated into an {@link OR} constraint on an {@link IPredicate}.</dd>
 * <dt></dt>
 * <dd></dd>
 * </dl>
 * <h2>Magic predicates</h2>
 * <p>
 * {@link BD#SEARCH} is the only magic predicate at this time. When the object
 * position is bound to a constant, the magic predicate is evaluated once and
 * the result is used to generate a set of term identifiers that are matches for
 * the token(s) extracted from the {@link Literal} in the object position. Those
 * term identifiers are then used to populate an {@link INBinarySearch} constraint. The
 * object position in the {@link BD#SEARCH} MUST be bound to a constant.
 * </p>
 * 
 * FIXME We are not in fact rewriting the query operation at all, simply
 * choosing a different evaluation path as we go. The rewrite should really be
 * isolated from the execution, e.g., in its own class. That more correct
 * approach is more than I want to get into right now as we will have to define
 * variants on the various operators that let us model the native rule system
 * directly, e.g., an n-ary IProgram, n-ary IRule operator, an IPredicate
 * operator, etc. Then we can handle evaluation using their model with anything
 * re-written to our custom operators being caught by our custom evaluate()
 * methods and everything else running their default methods. Definitely the
 * right approach, and much easier to write unit tests.
 * 
 * @todo REGEX : if there is a &quot;&circ;&quot; literal followed by a wildcard
 *       AND there are no flags which would cause problems (case-folding, etc)
 *       then the REGEX can be rewritten as a prefix scan on the lexicon, which
 *       is very efficient, and converted to an IN filter. When the set size is
 *       huge we should rewrite it as another tail in the query instead.
 *       <p>
 *       Otherwise, regex filters are left outside of the rule. We can't
 *       optimize that until we generate rules that perform JOINs across the
 *       lexicon and the spo relations (which we could do, in which case it
 *       becomes a constraint on that join).
 *       <p>
 *       We don't have any indices that are designed to optimize regex scans,
 *       but we could process a regex scan as a parallel iterator scan against
 *       the lexicon.
 * 
 * @todo Roll more kinds of filters into the native {@link IRule}s as
 *       {@link IConstraint}s on {@link IPredicate}s.
 *       <p>
 *       isURI(), etc. can be evaluated by testing a bit flag on the term
 *       identifier, which is very efficient.
 *       <p>
 * 
 * @todo Verify handling of datatype operations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: BigdataEvaluationStrategyImpl.java 2272 2009-11-04 02:10:19Z
 *          mrpersonick $
 */
public class BigdataEvaluationStrategyImpl extends EvaluationStrategyImpl {
    
    /**
     * Logger.
     */
    protected static final Logger log = 
        Logger.getLogger(BigdataEvaluationStrategyImpl.class);

//    protected static final boolean INFO = log.isInfoEnabled();
//
//    protected static final boolean DEBUG = log.isDebugEnabled();

    protected final BigdataTripleSource tripleSource;

    protected final Dataset dataset;

    private final AbstractTripleStore database;

    private final boolean nativeJoins;
    
    private final boolean starJoins;
    
    private final boolean inlineTerms;
    
    // private boolean slice = false, distinct = false, union = false;
    //    
    // // Note: defaults are illegal values.
    // private long offset = -1L, limit = 0L;
    // /**
    // * @param tripleSource
    // */
    // public BigdataEvaluationStrategyImpl(final BigdataTripleSource
    // tripleSource) {
    //
    // this(tripleSource, null/* dataset */, false WHY FALSE? /* nativeJoins
    // */);
    //
    // }
    /**
     * @param tripleSource
     * @param dataset
     */
    public BigdataEvaluationStrategyImpl(
            final BigdataTripleSource tripleSource, final Dataset dataset,
            final boolean nativeJoins, final boolean starJoins, 
            final boolean inlineTerms) {
        
        super(tripleSource, dataset);
        
        this.tripleSource = tripleSource;
        this.dataset = dataset;
        this.database = tripleSource.getDatabase();
        this.nativeJoins = nativeJoins;
        this.starJoins = starJoins;
        this.inlineTerms = inlineTerms;
        
    }

    // @Override
    // public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
    // org.openrdf.query.algebra.Slice slice, BindingSet bindings)
    // throws QueryEvaluationException {
    // /*
    // * Note: Sesame has somewhat different semantics for offset and limit.
    // * They are [int]s. -1 is used to indicate the the offset or limit was
    // * not specified. you use hasFoo() to see if there is an offset or a
    // * limit and then assign the value. For bigdata, the NOP offset is 0L
    // * and the NOP limit is Long.MAX_VALUE.
    // *
    // * Note: We can't process the offset natively unless we remove the slice
    // * from the Sesame operator tree. If we did then we would skip over the
    // * first OFFSET solutions and Sesame would skip over the first OFFSET
    // * solutions that we passed on, essentially doubling the offset.
    // *
    // * FIXME native rule slices work, but they can not be applied if there
    // * is a non-native filter outside of the join. This code could be
    // * modified to test for that using tuplExpr.visit(...), but really we
    // * just need to do a proper rewrite of the query expressions that is
    // * distinct from their evaluation!
    // */
    // //// if (!slice.hasOffset()) {
    // // this.slice = true;
    // // this.offset = slice.hasOffset() ? slice.getOffset() : 0L;
    // // this.limit = slice.hasLimit() ? slice.getLimit() : Long.MAX_VALUE;
    // //// return evaluate(slice.getArg(), bindings);
    // //// }
    // return super.evaluate(slice, bindings);
    // }
    //    
    // @Override
    // public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
    // Union union, BindingSet bindings) throws QueryEvaluationException {
    // this.union = true;
    // return super.evaluate(union, bindings);
    // }
    //
    // @Override
    // public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
    // Distinct distinct, BindingSet bindings)
    // throws QueryEvaluationException {
    // this.distinct = true;
    // return super.evaluate(distinct, bindings);
    // }

    /**
     * A set of properties that act as query hints for the join nexus.
     */
    private Properties queryHints;
    
    /**
     * This is the top-level method called by the SAIL to evaluate a query.
     * The TupleExpr parameter here is guaranteed to be the root of the operator
     * tree for the query.  Query hints are parsed by the SAIL from the
     * namespaces in the original query.  See {@link BD#QUERY_HINTS_NAMESPACE}.
     */
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
            TupleExpr expr, BindingSet bindings, Properties queryHints)
            throws QueryEvaluationException {
        
        // spit out the whole operator tree
        if (log.isInfoEnabled()) {
            log.info("operator tree:\n" + expr);
        }

        this.queryHints = queryHints;

        if (log.isInfoEnabled()) {
            log.info("queryHints:\n" + queryHints);
        }
        
        return super.evaluate(expr, bindings);
        
    }

    
    
    /**
     * Eventually we will want to translate the entire operator tree into a
     * native bigdata program. For now this is just a means of inspecting it.
     */
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
            TupleExpr expr, BindingSet bindings)
            throws QueryEvaluationException {
        
        if (log.isDebugEnabled()) {
            log.debug("tuple expr:\n" + expr);
        }
        
        return super.evaluate(expr, bindings);
        
    }

    /**
     * Translate top-level UNIONs into native bigdata programs for execution.
     * This will attempt to look down the operator tree from this point and turn
     * the Sesame operators into a set of native rules within a single program.
     * <p>
     * FIXME A Union is a BinaryTupleOperator composed of two expressions.  This 
     * native evaluation only handles the special case where the left and right
     * args are one of: {Join, LeftJoin, StatementPattern, Union}.  It's
     * possible that the left or right arg is something other than one of those
     * operators, in which case we punt to the Sesame evaluation, which
     * degrades performance.
     * <p>
     * FIXME Also, even if the left or right arg is one of the cases we handle, 
     * it's possible that the translation of that arg into a native rule will 
     * fail because of an unsupported SPARQL language feature, such as an 
     * embedded UNION or an unsupported filter type.
     */
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
            Union union, BindingSet bindings) throws QueryEvaluationException {
        
        if (!nativeJoins) {
            // Use Sesame 2 evaluation
            return super.evaluate(union, bindings);
        }

        if (log.isDebugEnabled()) {
            log.debug("union:\n" + union);
        }
        
        /*
         * FIXME Another deficiency in the native rule model. We can only handle
         * top-level UNIONs for now.
         */
        QueryModelNode operator = union;
        while ((operator = operator.getParentNode()) != null) {
            if (operator instanceof LeftJoin || operator instanceof Join) {
                // Use Sesame 2 evaluation
                
                if (log.isInfoEnabled()) {
                    log.info("could not evaluate natively, punting to Sesame"); 
                }
                if (log.isDebugEnabled()) {
                    log.debug(operator);
                }
                
                return super.evaluate(union, bindings);
            }
        }
        
        
        try {
            
            IStep query = createNativeQuery(union);
            
            if (query == null) {
                return new EmptyIteration<BindingSet, QueryEvaluationException>();
            }

            return execute(query);
            
        } catch (UnknownOperatorException ex) {
            
            // Use Sesame 2 evaluation
            
            if (log.isInfoEnabled()) {
                log.info("could not evaluate natively, punting to Sesame"); 
            }
            if (log.isDebugEnabled()) {
                log.debug(ex.getOperator());
            }

            return super.evaluate(union, bindings);
            
        } catch (Exception ex) {
            
            throw new QueryEvaluationException(ex);
            
//            // Use Sesame 2 evaluation
//
//            ex.printStackTrace();
//            
//            if (log.isInfoEnabled()) {
//                log.info("could not evaluate natively, punting to Sesame");
//            }
//            
//            return super.evaluate(union, bindings);
            
        }
        
    }
    
    /**
     * Override evaluation of StatementPatterns to recognize magic search 
     * predicate.
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
            final StatementPattern sp, final BindingSet bindings)
            throws QueryEvaluationException {
        
        // no check against the nativeJoins property here because we are simply
        // using the native execution model to take care of magic searches.
        
        if (log.isDebugEnabled()) {
            log.debug("evaluating statement pattern:\n" + sp);
        }
        
        IStep query = createNativeQuery(sp);
        
        if (query == null) {
            return new EmptyIteration<BindingSet, QueryEvaluationException>();
        }

        return execute(query);
        
    }
     */
    
    /**
     * Translate top-level JOINs into native bigdata programs for execution.
     * This will attempt to look down the operator tree from this point and turn
     * the Sesame operators into a native rule.
     * <p>
     * FIXME It's possible that the translation of the left or right arg into a 
     * native rule will fail because of an unsupported SPARQL language feature, 
     * such as an embedded UNION or an unsupported filter type.
     */
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
            Join join, BindingSet bindings) throws QueryEvaluationException {
        
        if (!nativeJoins) {
            // Use Sesame 2 evaluation
            return super.evaluate(join, bindings);
        }

        if (log.isDebugEnabled()) {
            log.debug("join:\n" + join);
        }
        
        /*
         * FIXME Another deficiency in the native rule model. If we are doing
         * a join that is nested inside an optional, we don't have the
         * appropriate variable bindings to arrive at the correct answer.
         * Example:
         * select * 
         * { 
         *    :x1 :p ?v .
         *    OPTIONAL { :x3 :q ?w }
         *    OPTIONAL { :x3 :q ?w . :x2 :p ?v }
         * }
         * 
         * 1. LeftJoin 
         *    2. LeftJoin 
         *       3. StatementPattern
         *       4. StatementPattern
         *    5. Join 
         *       6. StatementPattern
         *       7. StatementPattern
         *       
         * (1) punts, because the right arg is a Join and we can't mark an
         * entire Join as optional.  Then, (5) makes it here, to the evaluate
         * method.  But we can't evaluate it in isolation, we need to pump
         * the bindings in from the stuff above it.
         */
        QueryModelNode operator = join;
        while ((operator = operator.getParentNode()) != null) {
            if (operator instanceof LeftJoin) {

                // Use Sesame 2 evaluation
                
                if (log.isInfoEnabled()) {
                    log.info("could not evaluate natively, punting to Sesame"); 
                }
                if (log.isDebugEnabled()) {
                    log.debug(operator);
                }
                
                return super.evaluate(join, bindings);
            }
        }
        
        try {
            
            IStep query = createNativeQuery(join);
            
            if (query == null) {
                
                if (log.isDebugEnabled()) {
                    log.debug("query == null");
                }
                
                return new EmptyIteration<BindingSet, QueryEvaluationException>();
            }

            return execute(query);
            
        } catch (UnknownOperatorException ex) {
            
            // Use Sesame 2 evaluation
            
            if (log.isInfoEnabled()) {
                log.info("could not evaluate natively, punting to Sesame"); 
            }
            if (log.isDebugEnabled()) {
                log.debug(ex.getOperator());
            }

            return super.evaluate(join, bindings);
            
        } catch (Exception ex) {
            
            throw new QueryEvaluationException(ex);
            
//            // Use Sesame 2 evaluation
//            
//            ex.printStackTrace();
//            
//            if (log.isInfoEnabled()) {
//                log.info("could not evaluate natively, punting to Sesame"); 
//            }
//
//            return super.evaluate(join, bindings);
            
        }
        
    }
    
    /**
     * Translate top-level LEFTJOINs into native bigdata programs for execution.
     * This will attempt to look down the operator tree from this point and turn
     * the Sesame operators into a native rule.
     * <p>
     * FIXME It's possible that the translation of the left or right arg into a 
     * native rule will fail because of an unsupported SPARQL language feature, 
     * such as an embedded UNION or an unsupported filter type.
     */
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
            LeftJoin join, BindingSet bindings) throws QueryEvaluationException {
        
        if (!nativeJoins) {
            // Use Sesame 2 evaluation
            return super.evaluate(join, bindings);
        }

        if (log.isDebugEnabled()) {
            log.debug("left join:\n" + join);
        }
        
        /*
         * FIXME Another deficiency in the native rule model. If we are doing
         * a left join that is nested inside an optional, we don't have the
         * appropriate variable bindings to arrive at the correct answer.
         * Example:
         *  SELECT *
         *  { 
         *      :x1 :p ?v .
         *      OPTIONAL
         *      {
         *        :x3 :q ?w .
         *        OPTIONAL { :x2 :p ?v }
         *      }
         *  }         
         * 
         * 1. LeftJoin 
         *    2. StatementPattern 
         *    3. LeftJoin 
         *       4. StatementPattern
         *       5. StatementPattern
         *       
         * (1) punts, because the right arg is a LeftJoin and we can't mark an
         * entire Join as optional.  Then, (3) makes it here, to the evaluate
         * method.  But we can't evaluate it in isolation, we need to pump
         * the bindings in from the LeftJoin above it.
         */
        QueryModelNode operator = join;
        while ((operator = operator.getParentNode()) != null) {
            if (operator instanceof LeftJoin) {

                // Use Sesame 2 evaluation
                
                if (log.isInfoEnabled()) {
                    log.info("could not evaluate natively, punting to Sesame"); 
                }
                if (log.isDebugEnabled()) {
                    log.debug(operator);
                }

                return super.evaluate(join, bindings);
            }
        }
        
        try {
            
            IStep query = createNativeQuery(join);
            
            if (query == null) {
                return new EmptyIteration<BindingSet, QueryEvaluationException>();
            }
            
            return execute(query);
            
        } catch (UnknownOperatorException ex) {
            
            // Use Sesame 2 evaluation
            
            if (log.isInfoEnabled()) {
                log.info("could not evaluate natively, punting to Sesame"); 
            }
            if (log.isDebugEnabled()) {
                log.debug(ex.getOperator());
            }

            return super.evaluate(join, bindings);
            
        } catch (Exception ex) {
            
            throw new QueryEvaluationException(ex);
            
//            // Use Sesame 2 evaluation
//            
//            ex.printStackTrace();
//            
//            if (log.isInfoEnabled()) {
//                log.info("could not evaluate natively, punting to Sesame"); 
//            }
//
//            return super.evaluate(join, bindings);
            
        }
        
    }
    
    /**
     * This is the method that will attempt to take a top-level join or left
     * join and turn it into a native bigdata rule. The Sesame operators Join
     * and LeftJoin share only the common base class BinaryTupleOperator, but
     * other BinaryTupleOperators are not supported by this method. Other
     * specific types of BinaryTupleOperators will cause this method to throw
     * an exception.
     * <p>
     * This method will also turn a single top-level StatementPattern into a
     * rule with one predicate in it.
     * <p>
     * Note: As a pre-condition, the {@link Value}s in the query expression
     * MUST have been rewritten as {@link BigdataValue}s and their term
     * identifiers MUST have been resolved. Any term identifier that remains
     * {@link IRawTripleStore#NULL} is an indication that there is no entry for
     * that {@link Value} in the database. Since the JOINs are required (vs
     * OPTIONALs), that means that there is no solution for the JOINs and an
     * {@link EmptyIteration} is returned rather than evaluating the query.
     * 
     * @param join
     * @return native bigdata rule
     * @throws UnknownOperatorException
     *          this exception will be thrown if the Sesame join contains any
     *          SPARQL language constructs that cannot be converted into
     *          the bigdata native rule model
     * @throws QueryEvaluationException
     */
    private IRule createNativeQuery(final TupleExpr join)
            throws UnknownOperatorException,
            QueryEvaluationException {

        if (!(join instanceof StatementPattern || 
              join instanceof Join || join instanceof LeftJoin || 
              join instanceof Filter)) {
            throw new AssertionError(
                    "only StatementPattern, Join, and LeftJoin supported");
        }

        // flattened collection of statement patterns nested within this join,
        // along with whether or not each one is optional
        final Map<StatementPattern, Boolean> stmtPatterns = 
            new LinkedHashMap<StatementPattern, Boolean>();
        // flattened collection of filters nested within this join
        final Collection<Filter> filters = new LinkedList<Filter>();
        
        // will throw EncounteredUnknownTupleExprException if the join
        // contains something we don't handle yet
        collectStatementPatterns(join, stmtPatterns, filters);
        
        if (false) {
            for (Map.Entry<StatementPattern, Boolean> entry : 
                    stmtPatterns.entrySet()) {
                log.debug(entry.getKey() + ", optional=" + entry.getValue());
            }
            for (Filter filter : filters) {
                log.debug(filter.getCondition());
            }
        }
        
        // generate tails
        Collection<IPredicate> tails = new LinkedList<IPredicate>();
        // keep a list of free text searches for later to solve a named graphs
        // problem
        final Map<IPredicate, StatementPattern> searches = 
            new HashMap<IPredicate, StatementPattern>();
        for (Map.Entry<StatementPattern, Boolean> entry : stmtPatterns
                .entrySet()) {
            StatementPattern sp = entry.getKey();
            boolean optional = entry.getValue();
            IPredicate tail = generateTail(sp, optional);
            // encountered a value not in the database lexicon
            if (tail == null) {
                if (log.isDebugEnabled()) {
                    log.debug("could not generate tail for: " + sp);
                }
                if (optional) {
                    // for optionals, just skip the tail
                    continue;
                } else {
                    // for non-optionals, skip the entire rule
                    return null;
                }
            }
            if (tail.getSolutionExpander() instanceof FreeTextSearchExpander) {
                searches.put(tail, sp);
            }
            tails.add(tail);
        }
        
        /*
         * When in quads mode, we need to go through the free text searches and
         * make sure that they are properly filtered for the dataset where
         * needed. Joins will take care of this, so we only need to add a filter
         * when a search variable does not appear in any other tails that are
         * non-optional.
         * 
         * @todo Bryan seems to think this can be fixed with a DISTINCT JOIN
         * mechanism in the rule evaluation.
         */
        if (database.isQuads() && dataset != null) {
            for (IPredicate search : searches.keySet()) {
                final Set<URI> graphs;
                StatementPattern sp = searches.get(search);
                switch (sp.getScope()) {
                case DEFAULT_CONTEXTS: {
                    /*
                     * Query against the RDF merge of zero or more source
                     * graphs.
                     */
                    graphs = dataset.getDefaultGraphs();
                    break;
                }
                case NAMED_CONTEXTS: {
                    /*
                     * Query against zero or more named graphs.
                     */
                    graphs = dataset.getNamedGraphs();
                    break;
                }
                default:
                    throw new AssertionError();
                }
                if (graphs == null) {
                    continue;
                }
                // why would we use a constant with a free text search???
                if (search.get(0).isConstant()) {
                    throw new AssertionError();
                }
                // get ahold of the search variable
                com.bigdata.bop.Var searchVar = 
                    (com.bigdata.bop.Var) search.get(0);
                if (log.isDebugEnabled()) {
                    log.debug(searchVar);
                }
                // start by assuming it needs filtering, guilty until proven
                // innocent
                boolean needsFilter = true;
                // check the other tails one by one
                for (IPredicate<ISPO> tail : tails) {
                    ISolutionExpander<ISPO> expander = 
                        tail.getSolutionExpander();
                    // only concerned with non-optional tails that are not
                    // themselves magic searches
                    if (expander instanceof FreeTextSearchExpander
                            || tail.isOptional()) {
                        continue;
                    }
                    // see if the search variable appears in this tail
                    boolean appears = false;
                    for (int i = 0; i < tail.arity(); i++) {
                        IVariableOrConstant term = tail.get(i);
                        if (log.isDebugEnabled()) {
                            log.debug(term);
                        }
                        if (term.equals(searchVar)) {
                            appears = true;
                            break;
                        }
                    }
                    // if it appears, we don't need a filter
                    if (appears) {
                        needsFilter = false;
                        break;
                    }
                }
                // if it needs a filter, add it to the expander
                if (needsFilter) {
                    if (log.isDebugEnabled()) {
                        log.debug("needs filter: " + searchVar);
                    }
                    FreeTextSearchExpander expander = (FreeTextSearchExpander) 
                            search.getSolutionExpander();
                    expander.addNamedGraphsFilter(graphs);
                }
            }
        }
        
        // generate constraints
        final Collection<IConstraint> constraints = 
            new LinkedList<IConstraint>();
        final Iterator<Filter> filterIt = filters.iterator();
        while (filterIt.hasNext()) {
            final Filter filter = filterIt.next();
            final IConstraint constraint = generateConstraint(filter);
            if (constraint != null) {
                // remove if we are able to generate a native constraint for it
                if (log.isDebugEnabled()) {
                    log.debug("able to generate a constraint: " + constraint);
                }
                filterIt.remove();
                constraints.add(constraint);
            }
        }
        
        /*
         * FIXME Native slice, DISTINCT, etc. are all commented out for now.
         * Except for ORDER_BY, support exists for all of these features in the
         * native rules, but we need to separate the rewrite of the tupleExpr
         * and its evaluation in order to properly handle this stuff.
         */
        IQueryOptions queryOptions = QueryOptions.NONE;
        // if (slice) {
        // if (!distinct && !union) {
        // final ISlice slice = new Slice(offset, limit);
        // queryOptions = new QueryOptions(false/* distinct */,
        // true/* stable */, null/* orderBy */, slice);
        // }
        // } else {
        // if (distinct && !union) {
        // queryOptions = QueryOptions.DISTINCT;
        // }
        // }
        
        if (log.isDebugEnabled()) {
            for (IPredicate<ISPO> tail : tails) {
                ISolutionExpander<ISPO> expander = tail.getSolutionExpander();
                if (expander != null) {
                    IAccessPath<ISPO> accessPath = database.getSPORelation()
                            .getAccessPath(tail);
                    accessPath = expander.getAccessPath(accessPath);
                    IChunkedOrderedIterator<ISPO> it = accessPath.iterator();
                    while (it.hasNext()) {
                        log.debug(it.next().toString(database));
                    }
                }
            }
        }
        
        /*
         * Collect a set of variables required beyond just the join (i.e.
         * aggregation, projection, filters, etc.)
         */
        Set<String> required = new HashSet<String>(); 
        
        try {
            
            QueryModelNode p = join;
            while (true) {
                p = p.getParentNode();
                if (log.isDebugEnabled()) {
                    log.debug(p.getClass());
                }
                if (p instanceof UnaryTupleOperator) {
                    required.addAll(collectVariables((UnaryTupleOperator) p));
                }
                if (p instanceof QueryRoot) {
                    break;
                }
            }

            if (filters.size() > 0) {
                for (Filter filter : filters) {
                    required.addAll(collectVariables((UnaryTupleOperator) filter));
                }
            }
            
        } catch (Exception ex) {
            throw new QueryEvaluationException(ex);
        }

        IVariable[] requiredVars = new IVariable[required.size()];
        int i = 0;
        for (String v : required) {
            requiredVars[i++] = com.bigdata.bop.Var.var(v);
        }
        
        if (log.isDebugEnabled()) {
            log.debug("required binding names: " + Arrays.toString(requiredVars));
        }
        
        if (starJoins) { // database.isQuads() == false) {
            if (log.isDebugEnabled()) {
                log.debug("generating star joins");
            }
            tails = generateStarJoins(tails);
        }
        
        // generate native rule
        IRule rule = new Rule("nativeJoin", 
                // @todo should serialize the query string here for the logs.
                null, // head
                tails.toArray(new IPredicate[tails.size()]), queryOptions,
                // constraints on the rule.
                constraints.size() > 0 ? constraints
                        .toArray(new IConstraint[constraints.size()]) : null,
                null/* constants */, null/* taskFactory */, requiredVars);
        
        if (BigdataStatics.debug) {
            System.err.println(join.toString());
            System.err.println(rule.toString());
        }

        // we have filters that we could not translate natively
        if (filters.size() > 0) {
            if (log.isDebugEnabled()) {
                log.debug("could not translate " + filters.size()
                        + " filters into native constraints:");
                for (Filter filter : filters) {
                    log.debug("\n" + filter.getCondition());
                }
            }
            // use the basic filter iterator for remaining filters
            rule = new ProxyRuleWithSesameFilters(rule, filters);
        }
        
        return rule;
        
    }

    /**
     * Collect the variables used by this <code>UnaryTupleOperator</code> so
     * they can be added to the list of required variables in the query for
     * correct binding set pruning.
     * 
     * @param uto
     *          the <code>UnaryTupleOperator</code>
     * @return
     *          the variables it uses
     */
    protected Set<String> collectVariables(UnaryTupleOperator uto) 
        throws Exception {

        final Set<String> vars = new HashSet<String>();
        if (uto instanceof Projection) {
            List<ProjectionElem> elems = 
                ((Projection) uto).getProjectionElemList().getElements();
            for (ProjectionElem elem : elems) {
                vars.add(elem.getSourceName());
            }
        } else if (uto instanceof MultiProjection) {
            List<ProjectionElemList> elemLists = 
                ((MultiProjection) uto).getProjections();
            for (ProjectionElemList list : elemLists) {
                List<ProjectionElem> elems = list.getElements();
                for (ProjectionElem elem : elems) {
                    vars.add(elem.getSourceName());
                }
            }
        } else if (uto instanceof Filter) {
            Filter f = (Filter) uto;
            ValueExpr ve = f.getCondition();
            ve.visit(new QueryModelVisitorBase<Exception>() {
                @Override
                public void meet(Var v) throws Exception {
                    vars.add(v.getName());
                }
            });
        } else if (uto instanceof Group) {
            Group g = (Group) uto;
            g.visit(new QueryModelVisitorBase<Exception>() {
                @Override
                public void meet(Var v) {
                    vars.add(v.getName());
                }
            });
        } else if (uto instanceof Order) {
            Order o = (Order) uto;
            o.visit(new QueryModelVisitorBase<Exception>() {
                @Override
                public void meet(Var v) {
                    vars.add(v.getName());
                }
            });
        }
        return vars;

    }
    
    /**
     * This method will take a Union and attempt to turn it into a native
     * bigdata program. If either the left or right arg is a Union, the method
     * will act recursively to flatten the nested Unions into a single program.
     * <p>
     * See comments for {@link #evaluate(Union, BindingSet)}.
     * 
     * @param union
     * @return native bigdata program
     * @throws UnknownOperatorException
     *             this exception will be thrown if the Sesame join contains any
     *             SPARQL language constructs that cannot be converted into the
     *             bigdata native rule model
     * @throws QueryEvaluationException
     */
    private IProgram createNativeQuery(Union union)
            throws UnknownOperatorException,
            QueryEvaluationException {
        
        // create a new program that can run in parallel
        Program program = new Program("union", true);
        
        TupleExpr left = union.getLeftArg();
        // if the left arg is a union, create a program for it and merge it
        if (left instanceof Union) {
            Program p2 = (Program) createNativeQuery((Union) left);
            program.addSteps(p2.steps());
        } else if (left instanceof Join || left instanceof LeftJoin || 
                left instanceof Filter) {
            IRule rule = createNativeQuery(left);
            if (rule != null) {
                if (rule instanceof ProxyRuleWithSesameFilters) {
                    // unfortunately I think we just have to punt to be super safe
                    Collection<Filter> filters = 
                        ((ProxyRuleWithSesameFilters) rule).getSesameFilters();
                    if (log.isDebugEnabled()) {
                        log.debug("could not translate " + filters.size()
                                + " filters into native constraints:");
                        for (Filter filter : filters) {
                            log.debug("\n" + filter.getCondition());
                        }
                    }
                    throw new UnknownOperatorException(filters.iterator().next());
                }
                program.addStep(rule);
            }
        } else if (left instanceof StatementPattern) {
            IRule rule = createNativeQuery((StatementPattern) left);
            if (rule != null) {
                program.addStep(rule);
            }
        } else {
            throw new UnknownOperatorException(left);
        }
        
        TupleExpr right = union.getRightArg();
        // if the right arg is a union, create a program for it and merge it
        if (right instanceof Union) {
            Program p2 = (Program) createNativeQuery((Union) right);
            program.addSteps(p2.steps());
        } else if (right instanceof Join || right instanceof LeftJoin ||
                right instanceof Filter) {
            IRule rule = createNativeQuery(right);
            if (rule != null) {
                if (rule instanceof ProxyRuleWithSesameFilters) {
                    // unfortunately I think we just have to punt to be super safe
                    Collection<Filter> filters = 
                        ((ProxyRuleWithSesameFilters) rule).getSesameFilters();
                    if (log.isDebugEnabled()) {
                        log.debug("could not translate " + filters.size()
                                + " filters into native constraints:");
                        for (Filter filter : filters) {
                            log.debug("\n" + filter.getCondition());
                        }
                    }
                    throw new UnknownOperatorException(filters.iterator().next());
                }
                program.addStep(rule);
            }
        } else if (right instanceof StatementPattern) {
            IRule rule = createNativeQuery((StatementPattern) right);
            if (rule != null) {
                program.addStep(rule);
            }
        } else {
            throw new UnknownOperatorException(right);
        }
        
        return program;
        
    }

    /**
     * Take the supplied tuple expression and flatten all the statement patterns
     * into a collection that can then be fed into a bigdata rule.  So if the
     * tuple expression is itself a statement pattern or a filter, simply cast 
     * and add it to the appropriate collection.  If the tuple expression is a
     * join or left join, use recursion on the left and right argument of the
     * join.  If the tuple expression is anything else, for example a Union,
     * this method will throw an exception.  Currently Unions nested inside
     * of joins is not supported due to deficiencies in the native bigdata
     * rule model.
     * <p>
     * @todo support nested Unions
     *
     * @param tupleExpr
     * @param stmtPatterns
     * @param filters
     */
    private void collectStatementPatterns(final TupleExpr tupleExpr,
            final Map<StatementPattern, Boolean> stmtPatterns,
            final Collection<Filter> filters) 
        throws UnknownOperatorException {
        
        if (tupleExpr instanceof StatementPattern) {
            stmtPatterns.put((StatementPattern) tupleExpr, Boolean.FALSE);
        } else if (tupleExpr instanceof Filter) {
            final Filter filter = (Filter) tupleExpr;
            filters.add(filter);
            final TupleExpr arg = filter.getArg();
            collectStatementPatterns(arg, stmtPatterns, filters);
        } else if (tupleExpr instanceof Join) {
            final Join join = (Join) tupleExpr;
            final TupleExpr left = join.getLeftArg();
            final TupleExpr right = join.getRightArg();
            collectStatementPatterns(left, stmtPatterns, filters);
            collectStatementPatterns(right, stmtPatterns, filters);
        } else if (tupleExpr instanceof LeftJoin) {

            final LeftJoin join = (LeftJoin) tupleExpr;
            
            /*
             * FIXME Another deficiency in the native rule model. Incorrect
             * scoping of join.
             * Example:
             * SELECT *
             * { 
             *   ?X  :name "paul"
             *   {?Y :name "george" . OPTIONAL { ?X :email ?Z } }
             * }
             * 
             * 1. Join 
             *    2. StatementPattern 
             *    3. LeftJoin
             *       4. StatementPattern
             *       5. StatementPattern 
             *       
             * (1) starts collecting its child nodes and gets to (3), which
             * puts us here in the code.  But this is not a case where we
             * can just flatten the whole tree.  (3) needs to be evaluated
             * independently, as a subprogram.
             */
            QueryModelNode operator = join;
            while ((operator = operator.getParentNode()) != null) {
                if (operator instanceof Join) {
                    // Use Sesame 2 evaluation
                    throw new UnknownOperatorException(join);
                }
            }
            
            // FIXME is this right?  what about multiple optionals - do they nest?
            final TupleExpr left = join.getLeftArg();
            final TupleExpr right = join.getRightArg();
            // all we know how to handle right now is a left join of:
            // { StatementPattern || Join || LeftJoin } x { StatementPattern }
            if (!(right instanceof StatementPattern)) {
                throw new UnknownOperatorException(right);
            }
            final ValueExpr condition = join.getCondition();
            if (condition != null) {
                /*
                Filter filter = new Filter(right, condition);
                // fake a filter, we just need the value expr later
                filters.add(filter);
                */
                // we have to punt on nested optional filters just to be safe
                throw new UnknownOperatorException(join);
            }
            stmtPatterns.put((StatementPattern) right, Boolean.TRUE);
            collectStatementPatterns(left, stmtPatterns, filters);
        } else {
            throw new UnknownOperatorException(tupleExpr);
        }
        
    }

    /**
     * Generate a bigdata {@link IPredicate} (tail) for the supplied
     * StatementPattern.
     * <p>
     * As a shortcut, if the StatementPattern contains any bound values that
     * are not in the database, this method will return null.
     * 
     * @param stmtPattern
     * @param optional
     * @return the generated bigdata {@link Predicate} or <code>null</code> if
     *         the statement pattern contains bound values not in the database.
     * @throws QueryEvaluationException
     */
    private IPredicate generateTail(final StatementPattern stmtPattern,
            final boolean optional) throws QueryEvaluationException {
        
        // create a solution expander for free text search if necessary
        ISolutionExpander<ISPO> expander = null;
        final Value predValue = stmtPattern.getPredicateVar().getValue();
        if (log.isDebugEnabled()) {
            log.debug(predValue);
        }
        if (predValue != null && BD.SEARCH.equals(predValue)) {
            final Value objValue = stmtPattern.getObjectVar().getValue();
            if (log.isDebugEnabled()) {
                log.debug(objValue);
            }
            if (objValue != null && objValue instanceof Literal) {
                expander = new FreeTextSearchExpander(database,
                        (Literal) objValue);
            }
        }
        
        // @todo why is [s] handled differently?
        // because [s] is the variable in free text searches, no need to test
        // to see if the free text search expander is in place
        final IVariableOrConstant<IV> s = generateVariableOrConstant(
                stmtPattern.getSubjectVar());
        if (s == null) {
            return null;
        }
        
        final IVariableOrConstant<IV> p;
        if (expander == null) {
            p = generateVariableOrConstant(stmtPattern.getPredicateVar());
        } else {
            p = new Constant(DummyIV.INSTANCE);
        }
        if (p == null) {
            return null;
        }
        
        final IVariableOrConstant<IV> o;
        if (expander == null) {
            o = generateVariableOrConstant(stmtPattern.getObjectVar());
        } else {
            o = new Constant(DummyIV.INSTANCE);
        }
        if (o == null) {
            return null;
        }
        
        final IVariableOrConstant<IV> c;
        if (!database.isQuads()) {
            /*
             * Either triple store mode or provenance mode.
             */
            final Var var = stmtPattern.getContextVar();
            if (var == null) {
                // context position is not used.
                c = null;
            } else {
                final Value val = var.getValue();
                if (val != null && database.isStatementIdentifiers()) {
                    /*
                     * Note: The context position is used as a statement
                     * identifier (SID). SIDs may be used to retrieve provenance
                     * statements (statements about statement) using high-level
                     * query. SIDs are represented as blank nodes and is not
                     * possible to have them bound in the original query. They
                     * only become bound during query evaluation.
                     */
                    throw new QueryEvaluationException(
                            "Context position is a statement identifier and may not be bound in the original query: "
                                    + stmtPattern);
                }
                final String name = var.getName();
                c = com.bigdata.bop.Var.var(name);
            }
        } else {
            /*
             * Quad store mode.
             * 
             * FIXME Scale-out joins depend on knowledge of the best access path
             * and the index partitions (aka shards) which it will traverse.
             * Review all of the new expanders and make sure that they do not
             * violate this principle. Expanders tend to lazily determine the
             * access path, and I believe that RDFJoinNexus#getTailAccessPath()
             * may even refuse to operate with expanders. If this is the case,
             * then the choice of the access path needs to be completely coded
             * into the predicate as a combination of binding or clearing the
             * context variable and setting an appropriate constraint (filter).
             */
            if (BigdataStatics.debug) {
                if (dataset == null) {
                    System.err.println("No dataset.");
                } else {
                    final int defaultGraphSize = dataset.getDefaultGraphs()
                            .size();
                    final int namedGraphSize = dataset.getNamedGraphs().size();
                    if (defaultGraphSize > 10 || namedGraphSize > 10) {
                        System.err.println("large dataset: defaultGraphs="
                                + defaultGraphSize + ", namedGraphs="
                                + namedGraphSize);
                    } else {
                        System.err.println(dataset.toString());
                    }
                }
                System.err.println(stmtPattern.toString());
            }
            if (expander != null) {
                /*
                 * @todo can this happen? If it does then we need to look at how
                 * to layer the expanders.
                 */
                // throw new AssertionError("expander already set");
                // we are doing a free text search, no need to do any named or
                // default graph expansion work
                c = null;
            } else {
                final Var cvar = stmtPattern.getContextVar();
                if (dataset == null) {
                    if (cvar == null) {
                        /*
                         * There is no dataset and there is no graph variable,
                         * so the default graph will be the RDF Merge of ALL
                         * graphs in the quad store.
                         * 
                         * This code path uses an "expander" which strips off
                         * the context information and filters for the distinct
                         * (s,p,o) triples to realize the RDF Merge of the
                         * source graphs for the default graph.
                         */
                        c = null;
                        expander = new DefaultGraphSolutionExpander(null/* ALL */);
                    } else {
                        /*
                         * There is no data set and there is a graph variable,
                         * so the query will run against all named graphs and
                         * [cvar] will be to the context of each (s,p,o,c) in
                         * turn. This handles constructions such as:
                         * 
                         * "SELECT * WHERE {graph ?g {?g :p :o } }"
                         */
                        expander = new NamedGraphSolutionExpander(null/* ALL */);
                        c = generateVariableOrConstant(cvar);
                    }
                } else { // dataset != null
                    switch (stmtPattern.getScope()) {
                    case DEFAULT_CONTEXTS: {
                        /*
                         * Query against the RDF merge of zero or more source
                         * graphs.
                         */
                        expander = new DefaultGraphSolutionExpander(dataset
                                .getDefaultGraphs());
                        /*
                         * Note: cvar can not become bound since context is
                         * stripped for the default graph.
                         */
                        if (cvar == null)
                            c = null;
                        else
                            c = generateVariableOrConstant(cvar);
                        break;
                    }
                    case NAMED_CONTEXTS: {
                        /*
                         * Query against zero or more named graphs.
                         */
                        expander = new NamedGraphSolutionExpander(dataset
                                .getNamedGraphs());
                        if (cvar == null) {// || !cvar.hasValue()) {
                            c = null;
                        } else {
                            c = generateVariableOrConstant(cvar);
                        }
                        break;
                    }
                    default:
                        throw new AssertionError();
                    }
                }
            }
        }
        
        /*
         * This applies a filter to the access path to remove any inferred
         * triples when [includeInferred] is false.
         * 
         * @todo In order to rotate additional constraints onto an access path
         * we would need to either change IPredicate and AbstractAccessPath to
         * process an IConstraint[] or write a delegation pattern that let's us
         * wrap one filter inside of another.
         */
        final IElementFilter<ISPO> filter = 
            !tripleSource.includeInferred ? ExplicitSPOFilter.INSTANCE
                : null;
        
        return new SPOPredicate(
                new String[] { database.getSPORelation().getNamespace() },
                -1, // partitionId
                s, p, o, c,
                optional, // optional
                filter, // filter on elements visited by the access path.
                expander // free text search expander or named graphs expander
                );
        
    }

    /**
     * Generate a bigdata term from a Sesame term.
     * <p>
     * This method will return null if the Sesame term is bound and the value
     * does not exist in the lexicon.
     * 
     * @param var
     * @return
     */
    private IVariableOrConstant<IV> generateVariableOrConstant(final Var var) {
        final IVariableOrConstant<IV> result;
        final BigdataValue val = (BigdataValue) var.getValue();
        final String name = var.getName();
        if (val == null) {
            result = com.bigdata.bop.Var.var(name);
        } else {
            final IV iv = val.getIV();
            if (iv == null) {
                if (log.isDebugEnabled()) {
                    log.debug("null IV: " + val);
                }
                return null;
            }
            result = new Constant<IV>(iv);
        }
        return result;
    }

    private IConstraint generateConstraint(final Filter filter) {
        return generateConstraint(filter.getCondition());
    }

    private IConstraint generateConstraint(final ValueExpr valueExpr) {
        if (valueExpr instanceof Or) {
            return generateConstraint((Or) valueExpr);
        } else if (valueExpr instanceof SameTerm) {
            return generateConstraint((SameTerm) valueExpr);
        } else if (valueExpr instanceof Compare) {
            return generateConstraint((Compare) valueExpr);
        }
        return null;
    }

    private IConstraint generateConstraint(Or or) {
        IConstraint left = generateConstraint(or.getLeftArg());
        IConstraint right = generateConstraint(or.getRightArg());
        if (left != null && right != null) {
            return new OR(left, right);
        }
        return null;
    }

    private IConstraint generateConstraint(SameTerm sameTerm) {
        return generateConstraint(sameTerm.getLeftArg(),
                sameTerm.getRightArg(), CompareOp.EQ);
    }

    private IConstraint generateConstraint(Compare compare) {
        return generateConstraint(compare.getLeftArg(), compare.getRightArg(),
                compare.getOperator());
    }

    private IConstraint generateConstraint(ValueExpr left, ValueExpr right,
            CompareOp operator) {
        IVariable<IV> var = null;
        BigdataValue constant = null;
        if (left instanceof Var) {
            var = com.bigdata.bop.Var.var(((Var) left).getName());
        } else if (left instanceof ValueConstant) {
            constant = (BigdataValue) ((ValueConstant) left).getValue();
        } else {
            return null;
        }
        if (right instanceof Var) {
            var = com.bigdata.bop.Var.var(((Var) right).getName());
        } else if (right instanceof ValueConstant) {
            constant = (BigdataValue) ((ValueConstant) right).getValue();
        } else {
            return null;
        }
        if (log.isDebugEnabled()) {
            log.debug("var: " + var);
            log.debug("constant: " + constant);
            log.debug("constant.getIV(): " + constant.getIV());
        }
        if (var == null || constant == null || constant.getIV() == null) {
            if (log.isDebugEnabled()) {
                log.debug("left: " + left);
                log.debug("right: " + right);
            }
            return null;
        }
        final IV iv = constant.getIV();
        // we can do equals, not equals
        if (inlineTerms && IVUtility.canNumericalCompare(iv)) {
            if (log.isInfoEnabled()) {
                log.debug("inline constant, using inline numerical comparison: " 
                        + iv);
            }
            try {
                switch (operator) {
                case GT:
                    return new InlineGT(var, iv);
                case GE:
                    return new InlineGE(var, iv);
                case LT:
                    return new InlineLT(var, iv);
                case LE:
                    return new InlineLE(var, iv);
                case EQ:
                    return new InlineEQ(var, iv);
                case NE:
                    return new InlineNE(var, iv);
                default:
                    return null;
                }
            } catch (Exception ex) {
                return null;
            }
        } else if (operator == CompareOp.EQ) {
            return new EQConstant(var, new Constant(iv));
        } else if (operator == CompareOp.NE) {
            return new NEConstant(var, new Constant(iv));
        } else {
            return null;
        }
        
    }

    /**
     * Run a rule based on a {@link TupleExpr} as a query.
     * 
     * @param rule
     *            The rule to execute.
     * 
     * @return The Sesame 2 iteration that visits the {@link BindingSet}s that
     *         are the results for the query.
     * 
     * @throws QueryEvaluationException
     */
    protected CloseableIteration<BindingSet, QueryEvaluationException> execute(
            final IStep step)
            throws Exception {
        
        final QueryEngine queryEngine = tripleSource.getSail().getQueryEngine();
        
        final int startId = 1;
        final PipelineOp query = 
            Rule2BOpUtility.convert(step, startId, queryEngine);
        
        if (log.isInfoEnabled()) {
            log.info(query);
        }
        
        final UUID queryId = UUID.randomUUID();
        final RunningQuery runningQuery = queryEngine.eval(queryId, query,
                new LocalChunkMessage<IBindingSet>(queryEngine, queryId,
                        startId, -1/* partitionId */,
                        newBindingSetIterator(new HashBindingSet())));
        
        final IAsynchronousIterator<IBindingSet[]> it1 = 
            runningQuery.iterator();
        
        final IChunkedOrderedIterator<IBindingSet> it2 =
            new ChunkedArraysIterator<IBindingSet>(it1);
        
        CloseableIteration<BindingSet, QueryEvaluationException> result = 
            new Bigdata2Sesame2BindingSetIterator<QueryEvaluationException>(
                new BigdataBindingSetResolverator(database, it2).start(database
                        .getExecutorService()));
        
//        final boolean backchain = //
//        tripleSource.getDatabase().getAxioms().isRdfSchema()
//                && tripleSource.includeInferred
//                && tripleSource.conn.isQueryTimeExpander();
//        
//        if (log.isDebugEnabled()) {
//            log.debug("Running tupleExpr as native rule:\n" + step);
//            log.debug("backchain: " + backchain);
//        }
//        
//        // run the query as a native rule.
//        final IChunkedOrderedIterator<ISolution> itr1;
//        try {
//            final IEvaluationPlanFactory planFactory = 
//                DefaultEvaluationPlanFactory2.INSTANCE;
//            
//            /*
//             * alternative evaluation orders for LUBM Q9 (default is 1 4, 2, 3,
//             * 0, 5). All three evaluation orders are roughly as good as one
//             * another. Note that tail[2] (z rdf:type ...) is entailed by the
//             * ontology and could be dropped from evaluation.
//             */
//            // final IEvaluationPlanFactory planFactory = new
//            // FixedEvaluationPlanFactory(
//            // // new int[] { 1, 4, 3, 0, 5, 2 } good
//            // // new int[] { 1, 3, 0, 4, 5, 2 } good
//            // );
//            
//            final IJoinNexusFactory joinNexusFactory = database
//                    .newJoinNexusFactory(RuleContextEnum.HighLevelQuery,
//                            ActionEnum.Query, IJoinNexus.BINDINGS, 
//                            null, // filter
//                            false, // justify
//                            backchain, //
//                            planFactory, //
//                            queryHints
//                    );
//            
//            final IJoinNexus joinNexus = joinNexusFactory.newInstance(database
//                    .getIndexManager());
//            itr1 = joinNexus.runQuery(step);
//            
//        } catch (Exception ex) {
//            throw new QueryEvaluationException(ex);
//        }
//        
//        /*
//         * Efficiently resolve term identifiers in Bigdata ISolutions to RDF
//         * Values in Sesame 2 BindingSets and align the resulting iterator with
//         * the Sesame 2 API.
//         */
//        CloseableIteration<BindingSet, QueryEvaluationException> result = 
//            new Bigdata2Sesame2BindingSetIterator<QueryEvaluationException>(
//                new BigdataSolutionResolverator(database, itr1).start(database
//                        .getExecutorService()));
        
        // use the basic filter iterator for remaining filters
        if (step instanceof ProxyRuleWithSesameFilters) {
            Collection<Filter> filters = 
                ((ProxyRuleWithSesameFilters) step).getSesameFilters();
            if (log.isInfoEnabled() && filters.size() > 0) {
                log.info("could not translate " + filters.size()
                        + " filters into native constraints:");
            }
            for (Filter filter : filters) {
                if (log.isInfoEnabled())
                    log.info("\n" + filter.getCondition());
                result = new FilterIterator(filter, result, this);
            }
        }
        
        return result;
        
    }

    /**
     * Return an {@link IAsynchronousIterator} that will read a single,
     * empty {@link IBindingSet}.
     * 
     * @param bindingSet
     *            the binding set.
     */
    protected ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet bindingSet) {

        return new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { new IBindingSet[] { bindingSet } });

    }

    @SuppressWarnings("serial")
    private class UnknownOperatorException extends RuntimeException {
        private TupleExpr operator;

        public UnknownOperatorException(TupleExpr operator) {
            this.operator = operator;
        }

        public TupleExpr getOperator() {
            return operator;
        }
    }
    
    /**
     * Override evaluation of StatementPatterns to recognize magic search 
     * predicate.
     */
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
            final StatementPattern sp, final BindingSet bindings)
            throws QueryEvaluationException {
        
        if (log.isDebugEnabled()) {
            log.debug("evaluating statement pattern:\n" + sp);
        }
        
        // check for magic search
        final Var predVar = sp.getPredicateVar();
        final Value predValue = getVarValue(predVar, bindings);
        if (BD.SEARCH.equals(predValue)) {
            final Var ovar = sp.getObjectVar();
            final Value oval = getVarValue(ovar, bindings);
            if (oval == null) {
                throw new QueryEvaluationException(BD.SEARCH
                        + " : object must be bound.");
            }
            if (!(oval instanceof Literal)) {
                throw new QueryEvaluationException(BD.SEARCH
                        + " : object must be literal.");
            }
            final Literal lit = (Literal) oval;
            if (lit.getDatatype() != null) {
                throw new QueryEvaluationException(BD.SEARCH
                        + " : object is datatype literal.");
            }
            return search(sp.getSubjectVar(), lit.getLanguage(),
                    lit.getLabel(), bindings, sp.getScope());
        }
        
        return super.evaluate(sp, bindings);
        
    }
     

    /**
     * Evaluates the {@link BD#SEARCH} magic predicate as a full-text search
     * against the index literal in the database, binding <i>svar</i> to each
     * matched literal in turn.
     * <p>
     * Note: The minimum cosine (relevance score) is set to ZERO (0d) in order
     * to make sure that any match within a literal qualifies that literal for
     * inclusion within the set of bindings that are materialized. This is in
     * contrast to a standard search engine, where a minimum relevance score is
     * used to filter out less likely matches. However, it is my sense that
     * query against the KB is often used to find ALL matches. Regardless, the
     * matches will be materialized in order of decreasing relevance and an
     * upper bound of 10000 matches is imposed by the search implementation. See
     * {@link FullTextIndex#search(String, String, double, int)}.
     * 
     * @param svar
     *            The variable from the subject position of the
     *            {@link StatementPattern} in which the {@link BD#SEARCH} magic
     *            predicate appears.
     * @param languageCode
     *            An optional language code from the bound literal appearing in
     *            the object position of that {@link StatementPattern}.
     * @param label
     *            The required label from the bound literal appearing in the
     *            object position of that {@link StatementPattern}.
     * @param bindings
     *            The current bindings.
     * @param scope
     *            The scope of the statement pattern when in quads mode. The
     *            bound values for the text search must appear in a statement in
     *            the scope of the statement pattern.
     * 
     * @return Iteration visiting the bindings obtained by the search.
     * 
     * @throws QueryEvaluationException
     * 
     * @todo consider options for term weights and normalization. Search on the
     *       KB is probably all terms with anything matching, stopwords are
     *       excluded, and term weights can be without normalization since
     *       ranking does not matter. And maxRank should probably be defeated
     *       (Integer.MAX_VALUE or ZERO - whichever does the trick).
     * 
     * @todo it would be nice if there were a way for the caller to express more
     *       parameters for the search, e.g., to give the minCosine and maxRank
     *       values directly as a tuple-based function call. I'm not sure if
     *       that is supported within Sesame/SPARQL.
     */
    protected CloseableIteration<BindingSet, QueryEvaluationException> search(
            final Var svar, final String languageCode, final String label,
            final BindingSet bindings, final Scope scope)
            throws QueryEvaluationException {
        
        if (log.isDebugEnabled()) {
            log.debug("languageCode=" + languageCode + ", label=" + label);
        }
        
        final Iterator<IHit> itr = database.getLexiconRelation()
                .getSearchEngine().search(label, languageCode,
                        false/* prefixMatch */, 0d/* minCosine */,
                        10000/* maxRank */, 1000L/* timeout */,
                        TimeUnit.MILLISECONDS);
        
        // ensure that named graphs are handled correctly for quads
        Set<URI> graphs = null;
        if (database.isQuads() && dataset != null) {
            switch (scope) {
            case DEFAULT_CONTEXTS: {
                /*
                 * Query against the RDF merge of zero or more source graphs.
                 */
                graphs = dataset.getDefaultGraphs();
                break;
            }
            case NAMED_CONTEXTS: {
                /*
                 * Query against zero or more named graphs.
                 */
                graphs = dataset.getNamedGraphs();
                break;
            }
            default:
                throw new AssertionError();
            }
        }
        
        // Return an iterator that converts the term identifiers to var bindings
        return new HitConvertor(database, itr, svar, bindings, graphs);
        
    }
    
    protected Collection<IPredicate> generateStarJoins(
            Collection<IPredicate> tails) {
        
        Collection<IPredicate> newTails = new LinkedList<IPredicate>();
        
        Map<IVariable,Collection<IPredicate>> subjects = 
            new HashMap<IVariable,Collection<IPredicate>>();
        
        for (IPredicate pred : tails) {
            IVariableOrConstant s = pred.get(0);
            if (s.isVar() && /*pred.getSolutionExpander() == null &&*/ 
                    pred.getConstraint() == null) {
                IVariable v = (IVariable) s;
                Collection<IPredicate> preds = subjects.get(v);
                if (preds == null) {
                    preds = new LinkedList<IPredicate>();
                    subjects.put(v, preds);
                }
                preds.add(pred);
                if (log.isDebugEnabled()) {
                    log.debug("found a star joinable tail: " + pred);
                }
            } else {
                newTails.add(pred);
            }
        }
        
        for (Map.Entry<IVariable,Collection<IPredicate>> e : subjects.entrySet()) {
            Collection<IPredicate> preds = e.getValue();
            if (preds.size() <= 2) {
                newTails.addAll(preds);
                continue;
            }
            IVariable s = e.getKey();
            IPredicate mostSelective = null;
            long minRangeCount = Long.MAX_VALUE;
            int numOptionals = 0;
            for (IPredicate pred : preds) {
                if (pred.isOptional()) {
                    numOptionals++;
                    continue;
                }
                long rangeCount = database.getSPORelation().getAccessPath(
                        (SPOPredicate) pred).rangeCount(false);
                if (rangeCount < minRangeCount) {
                    minRangeCount = rangeCount;
                    mostSelective = pred;
                }
            }
            if (preds.size() - numOptionals < 2) {
                newTails.addAll(preds);
                continue;
            }
            if (mostSelective == null) {
                throw new RuntimeException(
                        "??? could not find a most selective tail for: " + s);
            }
            boolean sharedVars = false;
            Collection<IVariable> vars = new LinkedList<IVariable>();
            for (IPredicate pred : preds) {
                if (pred instanceof SPOPredicate) {
                    SPOPredicate spoPred = (SPOPredicate) pred;
                    if (spoPred.p().isVar()) {
                        IVariable v = (IVariable) spoPred.p();
                        if (vars.contains(v)) {
                            sharedVars = true;
                            break;
                        }
                        vars.add(v);
                    }
                    if (spoPred.o().isVar()) {
                        IVariable v = (IVariable) spoPred.o();
                        if (vars.contains(v)) {
                            sharedVars = true;
                            break;
                        }
                        vars.add(v);
                    }
                }
            }
            if (!sharedVars) {
                SPOStarJoin starJoin = new SPOStarJoin(
                        (SPOPredicate) mostSelective);
                for (IPredicate pred : preds) {
                    if (pred == mostSelective) {
                        continue;
                    }
                    starJoin.addStarConstraint(
                            new SPOStarJoin.SPOStarConstraint(
                                    pred.get(1), pred.get(2), pred.isOptional()));
                }
                newTails.add(starJoin);
                newTails.add(mostSelective);
            } else {
                newTails.addAll(preds);
            }
        }
        
        if (log.isDebugEnabled()) {
            log.debug("number of new tails: " + newTails.size());
            for (IPredicate tail : newTails) {
                log.debug(tail);
            }
        }
        
        return newTails;
        
    }

    // /** @issue make protected in openrdf. */
    // protected Value getVarValue(final Var var, final BindingSet bindings) {
    //
    // if (var == null) {
    //
    // return null;
    //
    // } else if (var.hasValue()) {
    //
    // return var.getValue();
    //
    // } else {
    //
    // return bindings.getValue(var.getName());
    //
    // }
    //
    // }
    
    protected static class ProxyRuleWithSesameFilters implements IRule {

        private IRule rule;
        
        private Collection<Filter> filters;
        
        public ProxyRuleWithSesameFilters(IRule rule, 
                Collection<Filter> filters) {
            
            this.rule = rule;
            this.filters = filters;
            
        }
        
        public IRule getProxyRule() {
            return rule;
        }
        
        public Collection<Filter> getSesameFilters() {
            return filters;
        }
        
        public IQueryOptions getQueryOptions() {
            return rule.getQueryOptions();
        }

        public boolean isRule() {
            return rule.isRule();
        }

        public IBindingSet getConstants() {
            return rule.getConstants();
        }

        public IConstraint getConstraint(int index) {
            return rule.getConstraint(index);
        }

        public int getConstraintCount() {
            return rule.getConstraintCount();
        }

        public Iterator getConstraints() {
            return rule.getConstraints();
        }

        public IPredicate getHead() {
            return rule.getHead();
        }

        public String getName() {
            return rule.getName();
        }

        public Set getSharedVars(int index1, int index2) {
            return rule.getSharedVars(index1, index2);
        }

        public Iterator getTail() {
            return rule.getTail();
        }

        public IPredicate getTail(int index) {
            return rule.getTail(index);
        }

        public int getTailCount() {
            return rule.getTailCount();
        }

        public IRuleTaskFactory getTaskFactory() {
            return rule.getTaskFactory();
        }

        public int getVariableCount() {
            return rule.getVariableCount();
        }

        public int getVariableCount(int index, IBindingSet bindingSet) {
            return rule.getVariableCount(index, bindingSet);
        }

        public Iterator getVariables() {
            return rule.getVariables();
        }

        public int getRequiredVariableCount() {
            return rule.getRequiredVariableCount();
        }

        public Iterator getRequiredVariables() {
            return rule.getRequiredVariables();
        }

        public boolean isConsistent(IBindingSet bindingSet) {
            return rule.isConsistent(bindingSet);
        }

        public boolean isDeclared(IVariable var) {
            return rule.isDeclared(var);
        }

        public boolean isFullyBound(IBindingSet bindingSet) {
            return rule.isFullyBound(bindingSet);
        }

        public boolean isFullyBound(int index, IBindingSet bindingSet) {
            return rule.isFullyBound(index, bindingSet);
        }

        public IRule specialize(IBindingSet bindingSet, IConstraint[] constraints) {
            return rule.specialize(bindingSet, constraints);
        }

        public IRule specialize(String name, IBindingSet bindingSet, IConstraint[] constraints) {
            return rule.specialize(name, bindingSet, constraints);
        }

        public String toString(IBindingSet bindingSet) {
            return rule.toString(bindingSet);
        }
        
        public String toString() {
            return rule.toString();
        }
        
    }
    
}

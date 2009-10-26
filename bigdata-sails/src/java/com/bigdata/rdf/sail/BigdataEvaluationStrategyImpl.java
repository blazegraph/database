package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Compare;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.Or;
import org.openrdf.query.algebra.SameTerm;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.openrdf.query.algebra.evaluation.iterator.FilterIterator;

import com.bigdata.BigdataStatics;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.rules.RuleContextEnum;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.spo.DefaultGraphSolutionExpander;
import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.NamedGraphSolutionExpander;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BNS;
import com.bigdata.rdf.store.BigdataSolutionResolverator;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.EQ;
import com.bigdata.relation.rule.EQConstant;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.IN;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IQueryOptions;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.ISolutionExpander;
import com.bigdata.relation.rule.ISortOrder;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.relation.rule.NE;
import com.bigdata.relation.rule.NEConstant;
import com.bigdata.relation.rule.OR;
import com.bigdata.relation.rule.QueryOptions;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.DefaultEvaluationPlanFactory2;
import com.bigdata.relation.rule.eval.IEvaluationPlanFactory;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.NestedSubqueryWithJoinThreadsTask;
import com.bigdata.relation.rule.eval.RuleStats;
import com.bigdata.search.FullTextIndex;
import com.bigdata.search.IHit;
import com.bigdata.striterator.DistinctFilter;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Extended to rewrite Sesame {@link TupleExpr}s onto native {@link Rule}s and
 * to evaluate magic predicates for full text search, etc. Query evaluation can
 * proceed either by Sesame 2 evaluation or, if {@link Options#NATIVE_JOINS} is
 * enabled, then by translation of Sesame 2 query expressions into native
 * {@link IRule}s and native evaluation of those {@link IRule}s.
 * 
 * <h2>Query options</h2> The following summarizes how various high-level query
 * language feature are mapped onto native {@link IRule}s.
 * <dl>
 * <dt>DISTINCT</dt>
 * <dd>{@link IQueryOptions#isDistinct()}, which is realized using
 * {@link DistinctFilter}.</dd>
 * <dt>ORDER BY</dt>
 * <dd>{@link IQueryOptions#getOrderBy()} is effected by a custom
 * {@link IKeyBuilderFactory} which generates sort keys that capture the desired
 * sort order from the bindings in an {@link ISolution}. Unless DISTINCT is also
 * specified, the generated sort keys are made unique by appending a one up long
 * integer to the key - this prevents sort keys that otherwise compare as equals
 * from dropping solutions. Note that the SORT is actually imposed by the
 * {@link DistinctFilter} using an {@link IKeyBuilderFactory} assembled from the
 * ORDER BY constraints.
 * 
 * FIXME BryanT - implement the {@link IKeyBuilderFactory}.
 * 
 * FIXME MikeP - assemble the {@link ISortOrder}[] from the query and set on the
 * {@link IQueryOptions}.</dd>
 * <dt>OFFSET and LIMIT</dt>
 * <dd>
 * <p>
 * {@link IQueryOptions#getSlice()}, which is effected as a conditional in
 * {@link NestedSubqueryWithJoinThreadsTask} based on the
 * {@link RuleStats#solutionCount}. Query {@link ISolution}s are counted as they
 * are generated, but they are only entered into the {@link ISolution}
 * {@link IBuffer} when the solutionCount is GE the OFFSET and LT the LIMIT.
 * Query evaluation halts once the LIMIT is reached.
 * </p>
 * <p>
 * Note that when DISTINCT and either LIMIT and/or OFFSET are specified
 * together, then the LIMIT and OFFSET <strong>MUST</strong> be applied after
 * the solutions have been generated since we may have to generate more than
 * LIMIT solutions in order to have LIMIT <em>DISTINCT</em> solutions. We handle
 * this for now by NOT translating the LIMIT and OFFSET onto the {@link IRule}
 * and instead let Sesame close the iterator once it has enough solutions.
 * </p>
 * <p>
 * Note that LIMIT and SLICE requires an evaluation plan that provides stable
 * results. For a simple query this is achieved by setting
 * {@link IQueryOptions#isStable()} to <code>true</code>.
 * <p>
 * For a UNION query, you must also set {@link IProgram#isParallel()} to
 * <code>false</code> to prevent parallelized execution of the {@link IRule}s in
 * the {@link IProgram}.
 * </p>
 * </dd>
 * <dt>UNION</dt>
 * <dd>A UNION is translated into an {@link IProgram} consisting of one
 * {@link IRule} for each clause in the UNION.
 * 
 * FIXME MikeP - implement.</dd>
 * </dl>
 * <h2>Filters</h2> The following provides a summary of how various kinds of
 * FILTER are handled. A filter that is not explicitly handled is left
 * untranslated and will be applied by Sesame against the generated
 * {@link ISolution}s.
 * <p>
 * Whenever possible, a FILTER is translated into an {@link IConstraint} on an
 * {@link IPredicate} in the generated native {@link IRule}. Some filters are
 * essentially JOINs against the {@link LexiconRelation}. Those can be handled
 * either as JOINs (generating an additional {@link IPredicate} in the
 * {@link IRule}) or as an {@link IN} constraint, where the inclusion set is
 * pre-populated by some operation on the {@link LexiconRelation}.
 * <dl>
 * <dt>EQ</dt>
 * <dd>Translated into an {@link EQ} constraint on an {@link IPredicate}.</dd>
 * <dt>NE</dt>
 * <dd>Translated into an {@link NE} constraint on an {@link IPredicate}.</dd>
 * <dt>IN</dt>
 * <dd>Translated into an {@link IN} constraint on an {@link IPredicate}.</dd>
 * <dt>OR</dt>
 * <dd>Translated into an {@link OR} constraint on an {@link IPredicate}.</dd>
 * <dt></dt>
 * <dd></dd>
 * </dl>
 * <h2>Magic predicates</h2>
 * <p>
 * {@link BNS#SEARCH} is the only magic predicate at this time. When the object
 * position is bound to a constant, the magic predicate is evaluated once and
 * the result is used to generate a set of term identifiers that are matches for
 * the token(s) extracted from the {@link Literal} in the object position. Those
 * term identifiers are then used to populate an {@link IN} constraint. The
 * object position in the {@link BNS#SEARCH} MUST be bound to a constant.
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
 * @version $Id$
 */
public class BigdataEvaluationStrategyImpl extends EvaluationStrategyImpl {

    /**
     * Logger.
     */
    protected static final Logger log = Logger
            .getLogger(BigdataEvaluationStrategyImpl.class);

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();

    /**
     * The magic predicate for text search.
     * 
     * @see BNS#SEARCH
     */
    static private final URI MAGIC_SEARCH = new URIImpl(BNS.SEARCH);

    private final long NULL = IRawTripleStore.NULL;

    protected final BigdataTripleSource tripleSource;

    protected final Dataset dataset;

    private final AbstractTripleStore database;

    private final boolean nativeJoins;

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
            final boolean nativeJoins) {

        super(tripleSource, dataset);

        this.tripleSource = tripleSource;

        this.dataset = dataset;

        this.database = tripleSource.getDatabase();

        this.nativeJoins = nativeJoins;
        // this.nativeJoins = false;

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
     * Overridden to recognize magic predicates.
     */
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
            final StatementPattern sp, final BindingSet bindings)
            throws QueryEvaluationException {

        final Var predVar = sp.getPredicateVar();

        final Value predValue = getVarValue(predVar, bindings);

        if (MAGIC_SEARCH.equals(predValue)) {

            final Var ovar = sp.getObjectVar();

            final Value oval = getVarValue(ovar, bindings);

            if (oval == null) {

                throw new QueryEvaluationException(MAGIC_SEARCH
                        + " : object must be bound.");

            }

            if (!(oval instanceof Literal)) {

                throw new QueryEvaluationException(MAGIC_SEARCH
                        + " : object must be literal.");

            }

            final Literal lit = (Literal) oval;

            if (lit.getDatatype() != null) {

                throw new QueryEvaluationException(MAGIC_SEARCH
                        + " : object is datatype literal.");

            }

            return search(sp.getSubjectVar(), lit.getLanguage(),
                    lit.getLabel(), bindings);

        }

        return super.evaluate(sp, bindings);

    }

    /**
     * Evaluates the {@link BNS#SEARCH} magic predicate as a full-text search
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
     *            {@link StatementPattern} in which the {@link BNS#SEARCH} magic
     *            predicate appears.
     * @param languageCode
     *            An optional language code from the bound literal appearing in
     *            the object position of that {@link StatementPattern}.
     * @param label
     *            The required label from the bound literal appearing in the
     *            object position of that {@link StatementPattern}.
     * @param bindings
     *            The current bindings.
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
            final BindingSet bindings) throws QueryEvaluationException {

        if (INFO)
            log.info("languageCode=" + languageCode + ", label=" + label);

        final Iterator<IHit> itr = database.getSearchEngine().search(label,
                languageCode, 0d/* minCosine */, 10000/* maxRank */);

        // Return an iterator that converts the term identifiers to var bindings
        return new HitConvertor(database, itr, svar, bindings);

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

    /**
     * Uses native joins iff {@link BigdataSail.Options#NATIVE_JOINS} is
     * specified.
     * <p>
     * Note: As a pre-condition, the {@link Value}s in the query expression MUST
     * have been rewritten as {@link BigdataValue}s and their term identifiers
     * MUST have been resolved. Any term identifier that remains
     * {@link IRawTripleStore#NULL} is an indication that there is no entry for
     * that {@link Value} in the database. Since the JOINs are required (vs
     * OPTIONALs), that means that there is no solution for the JOINs and an
     * {@link EmptyIteration} is returned rather than evaluating the query.
     */
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
            final Join join, final BindingSet bindings)
            throws QueryEvaluationException {

        if (!nativeJoins) {

            // Use Sesame 2 evaluation for JOINs.
            return super.evaluate(join, bindings);

        }

        if (INFO)
            log.info("evaluating native join:\n" + join);

        final Map<StatementPattern, Boolean> stmtPatterns = new LinkedHashMap<StatementPattern, Boolean>();

        final Collection<Filter> filters = new LinkedList<Filter>();

        try {

            collectStatementPatterns(join, stmtPatterns, filters);

        } catch (EncounteredUnknownTupleExprException ex) {

            /*
             * Use Sesame 2 evaluation for JOINs with unions.
             * 
             * FIXME We should really implement native Unions. All it requires
             * is running the rules as part of a parallel program writing on the
             * same query buffer.
             */
            log.warn("need to implement native TupleExpr: " + ex.getTupleExpr());

            return super.evaluate(join, bindings);

        }

        if (INFO) {
            for (Map.Entry<StatementPattern, Boolean> entry : stmtPatterns.entrySet()) {
                log.info(entry.getKey() + ", optional=" + entry.getValue());
            }
            for (Filter filter : filters) {
                log.info(filter.getCondition());
            }
        }

        // generate tails
        final Collection<IPredicate> tails = new LinkedList<IPredicate>();
        for (Map.Entry<StatementPattern, Boolean> entry : stmtPatterns.entrySet()) {
            IPredicate tail = generateTail(entry.getKey(), entry.getValue());
            if (tail == null) {
                return new EmptyIteration<BindingSet, QueryEvaluationException>();
            }
            tails.add(tail);
        }

        // generate constraints
        final Collection<IConstraint> constraints = new LinkedList<IConstraint>();
        final Iterator<Filter> filterIt = filters.iterator();
        while (filterIt.hasNext()) {
            final Filter filter = filterIt.next();
            final IConstraint constraint = generateConstraint(filter);
            if (constraint != null) {
                // remove if we are able to generate a native constraint for it
                if (INFO) {
                    log.info("able to generate a constraint: " + constraint);
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

        if (DEBUG) {
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

        // generate native rule
        final IRule rule = new Rule("nativeJoin", // @todo should serialize the
                                                  // query string here for the
                                                  // logs.
                null, // head
                tails.toArray(new IPredicate[tails.size()]), queryOptions,//
                // constraints on the rule.
                constraints.size() > 0 ? constraints
                        .toArray(new IConstraint[constraints.size()]) : null);

        if (BigdataStatics.debug) {
            System.err.println(join.toString());
            System.err.println(rule.toString());
        }

        CloseableIteration<BindingSet, QueryEvaluationException> result = runQuery(
                join, rule);

        // use the basic filter iterator for remaining filters
        if (INFO && filters.size() > 0) {
            log.info("could not translate " + filters.size()
                    + " filters into native constraints:");
        }
        for (Filter filter : filters) {
            if (INFO)
                log.info("\n" + filter.getCondition());
            result = new FilterIterator(filter, result, this);
        }

        return result;

    }

    private void collectStatementPatterns(final TupleExpr tupleExpr,
            final Map<StatementPattern, Boolean> stmtPatterns,
            final Collection<Filter> filters) {
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
        } else if (tupleExpr instanceof Union) {
            throw new EncounteredUnknownTupleExprException(tupleExpr);
        } else if (tupleExpr instanceof LeftJoin) {
            //throw new EncounteredUnknownTupleExprException(tupleExpr);
            final LeftJoin leftJoin = (LeftJoin) tupleExpr;
            final StatementPattern left = (StatementPattern) leftJoin.getLeftArg();
            final StatementPattern right = (StatementPattern) leftJoin.getRightArg();
            final ValueExpr condition = leftJoin.getCondition();
            if (condition != null) {
                // fake a filter, we just need the value expr later
                filters.add(new Filter(right, condition));
            }
            stmtPatterns.put(left, Boolean.FALSE);
            stmtPatterns.put(right, Boolean.TRUE);
        } else {
            throw new EncounteredUnknownTupleExprException(tupleExpr);
        }
    }

    private IPredicate generateTail(final StatementPattern stmtPattern, final boolean optional) 
        throws QueryEvaluationException {

        // create a solution expander for free text search if necessary
        ISolutionExpander<ISPO> expander = null;
        final Value predValue = stmtPattern.getPredicateVar().getValue();
        if (DEBUG)
            log.debug(predValue);
        if (predValue != null && MAGIC_SEARCH.equals(predValue)) {
            final Value objValue = stmtPattern.getObjectVar().getValue();
            if (DEBUG)
                log.debug(objValue);
            if (objValue != null && objValue instanceof Literal) {
                expander = new FreeTextSearchExpander(database,
                        (Literal) objValue);
            }
        }

        // @todo why is [s] handled differently?
        final IVariableOrConstant<Long> s = generateVariableOrConstant(stmtPattern
                .getSubjectVar());
        if (s == null) {
            return null;
        }
        final IVariableOrConstant<Long> p;
        if (expander == null) {
            p = generateVariableOrConstant(stmtPattern.getPredicateVar());
        } else {
            p = new Constant<Long>(IRawTripleStore.NULL);
        }
        if (p == null) {
            return null;
        }
        final IVariableOrConstant<Long> o;
        if (expander == null) {
            o = generateVariableOrConstant(stmtPattern.getObjectVar());
        } else {
            o = new Constant<Long>(IRawTripleStore.NULL);
        }
        if (o == null) {
            return null;
        }
        final IVariableOrConstant<Long> c;
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
                c = com.bigdata.relation.rule.Var.var(name);
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
                throw new AssertionError("expander already set");
            }
            final Var cvar = stmtPattern.getContextVar();
            if (dataset == null) {
                if (cvar == null) {
                    /*
                     * There is no dataset and there is no graph variable, so
                     * the default graph will be the RDF Merge of ALL graphs in
                     * the quad store.
                     * 
                     * This code path uses an "expander" which strips off the
                     * context information and filters for the distinct (s,p,o)
                     * triples to realize the RDF Merge of the source graphs for
                     * the default graph.
                     */
                    c = null;
                    expander = new DefaultGraphSolutionExpander(null/* ALL */);
                } else {
                    /*
                     * There is no data set and there is a graph variable, so
                     * the query will run against all named graphs and [cvar]
                     * will be to the context of each (s,p,o,c) in turn. This
                     * handles constructions such as:
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
                     * Note: cvar can not become bound since context is stripped
                     * for the default graph.
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

        /*
         * This applies a filter to the access path to remove any inferred
         * triples when [includeInferred] is false.
         * 
         * @todo In order to rotate additional constraints onto an access path
         * we would need to either change IPredicate and AbstractAccessPath to
         * process an IConstraint[] or write a delegation pattern that let's us
         * wrap one filter inside of another.
         */
        final IElementFilter<ISPO> filter = !tripleSource.includeInferred ? ExplicitSPOFilter.INSTANCE
                : null;

        return new SPOPredicate(new String[] { database.getSPORelation()
                .getNamespace() },//
                -1, // partitionId
                s, p, o, c, //
                optional, // optional
                filter, // filter on elements visited by the access path.
                expander);
    }

    private IVariableOrConstant<Long> generateVariableOrConstant(final Var var) {
        final IVariableOrConstant<Long> result;
        final Value val = var.getValue();
        final String name = var.getName();
        if (val == null) {
            result = com.bigdata.relation.rule.Var.var(name);
        } else {
            final Long id = database.getTermId(val);
            if (id.longValue() == NULL)
                return null;
            result = new Constant<Long>(id);
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
        IVariable<Long> var = null;
        IConstant<Long> constant = null;

        if (left instanceof Var) {
            var = com.bigdata.relation.rule.Var.var(((Var) left).getName());
        } else if (left instanceof ValueConstant) {
            Value value = ((ValueConstant) left).getValue();
            final Long id = database.getTermId(value);
            if (id.longValue() == NULL)
                return null;
            constant = new Constant<Long>(id);
        } else {
            return null;
        }

        if (right instanceof Var) {
            var = com.bigdata.relation.rule.Var.var(((Var) right).getName());
        } else if (right instanceof ValueConstant) {
            Value value = ((ValueConstant) right).getValue();
            final Long id = database.getTermId(value);
            if (id.longValue() == NULL)
                return null;
            constant = new Constant<Long>(id);
        } else {
            return null;
        }

        if (INFO) {
            log.info("var: " + var);
            log.info("constant: " + constant);
        }

        if (var == null || constant == null) {
            if (INFO) {
                log.info("left: " + left);
                log.info("right: " + right);
            }
            return null;
        }

        // we can do equals, not equals
        if (operator == CompareOp.EQ) {
            return new EQConstant(var, constant);
        } else if (operator == CompareOp.NE) {
            return new NEConstant(var, constant);
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
    protected CloseableIteration<BindingSet, QueryEvaluationException> runQuery(
            final TupleExpr tupleExpr, final IRule rule)
            throws QueryEvaluationException {

        final boolean backchain = //
        tripleSource.getDatabase().getAxioms().isRdfSchema()
                && tripleSource.includeInferred
                && tripleSource.conn.isQueryTimeExpander();

        if (INFO) {

            log.info("Running tupleExpr as native rule:\n" + tupleExpr + ",\n"
                    + rule);

            log.info("backchain: " + backchain);

        }

        // run the query as a native rule.
        final IChunkedOrderedIterator<ISolution> itr1;
        try {

            final IEvaluationPlanFactory planFactory = DefaultEvaluationPlanFactory2.INSTANCE;

            /*
             * alternative evaluation orders for LUBM Q9 (default is 1 4, 2, 3,
             * 0, 5). All three evaluation orders are roughly as good as one
             * another. Note that tail[2] (z rdf:type ...) is entailed by the
             * ontology and could be dropped from evaluation.
             */
            // final IEvaluationPlanFactory planFactory = new
            // FixedEvaluationPlanFactory(
            // // new int[] { 1, 4, 3, 0, 5, 2 } good
            // // new int[] { 1, 3, 0, 4, 5, 2 } good
            // );

            final IJoinNexusFactory joinNexusFactory = database
                    .newJoinNexusFactory(RuleContextEnum.HighLevelQuery,
                            ActionEnum.Query, IJoinNexus.BINDINGS, null, // filter
                            false, // justify
                            backchain, //
                            planFactory//
                    );

            final IJoinNexus joinNexus = joinNexusFactory.newInstance(database
                    .getIndexManager());

            itr1 = joinNexus.runQuery(rule);

        } catch (Exception ex) {

            throw new QueryEvaluationException(ex);

        }

        /*
         * Efficiently resolve term identifiers in Bigdata ISolutions to RDF
         * Values in Sesame 2 BindingSets and align the resulting iterator with
         * the Sesame 2 API.
         */
        return new Bigdata2Sesame2BindingSetIterator<QueryEvaluationException>(
                new BigdataSolutionResolverator(database, itr1).start(database
                        .getExecutorService()));

    }

    @SuppressWarnings("serial")
    private class EncounteredUnknownTupleExprException extends RuntimeException {
        private TupleExpr tupleExpr;
        
        public EncounteredUnknownTupleExprException(TupleExpr tupleExpr) {
            this.tupleExpr = tupleExpr;
        }
        
        public TupleExpr getTupleExpr() {
            return tupleExpr;
        }
    }

}

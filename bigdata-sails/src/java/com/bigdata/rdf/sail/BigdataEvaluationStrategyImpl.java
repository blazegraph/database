package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.BinaryTupleOperator;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;

import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.rules.RuleContextEnum;
import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BNS;
import com.bigdata.rdf.store.BigdataSolutionResolverator;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.DefaultEvaluationPlanFactory2;
import com.bigdata.relation.rule.eval.IEvaluationPlanFactory;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.search.FullTextIndex;
import com.bigdata.search.IHit;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Extended to rewrite Sesame {@link TupleExpr}s onto native {@link Rule}s and
 * to evaluate magic predicates for full text search, etc.
 * 
 * FIXME Capture more kinds of {@link BinaryTupleOperator} and
 * {@link UnaryTupleOperator} using native rule evaluation, including rolling
 * filters and optionals into the native rules, etc.
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

    /**
     * The magic predicate for text search.
     * 
     * @see BNS#SEARCH
     */
    static final URI MAGIC_SEARCH = new URIImpl(BNS.SEARCH);

    private final long NULL = IRawTripleStore.NULL;
    
    protected final BigdataTripleSource tripleSource;

    protected final Dataset dataset;

    private final AbstractTripleStore database;
    
    private final boolean nativeJoins;

    /**
     * @param tripleSource
     */
    public BigdataEvaluationStrategyImpl(BigdataTripleSource tripleSource) {

        this(tripleSource, null, false);

    }

    /**
     * @param tripleSource
     * @param dataset
     */
    public BigdataEvaluationStrategyImpl(BigdataTripleSource tripleSource,
            Dataset dataset, boolean nativeJoins) {

        super(tripleSource, dataset);

        this.tripleSource = tripleSource;

        this.dataset = null;

        this.database = tripleSource.getDatabase();
        
        this.nativeJoins = nativeJoins;

    }

    /**
     * Overriden to recognize magic predicates.
     */
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
            StatementPattern sp, BindingSet bindings)
            throws QueryEvaluationException {

        final Var predVar = sp.getPredicateVar();

        final Value predValue = getVarValue(predVar, bindings);

        if (MAGIC_SEARCH.equals(predValue)) {

            final Var ovar = sp.getObjectVar();

            final Value oval = getVarValue(ovar, bindings);

            if (oval == null) {

                throw new QueryEvaluationException(MAGIC_SEARCH
                        + " : value must be bound.");

            }

            if (!(oval instanceof Literal)) {

                throw new QueryEvaluationException(MAGIC_SEARCH
                        + " : value must be literal.");

            }

            Literal lit = (Literal) oval;

            if (lit.getDatatype() != null) {

                throw new QueryEvaluationException(MAGIC_SEARCH
                        + " : value must not be a datatype literal.");

            }

            return search(sp.getSubjectVar(), lit.getLanguage(),
                    lit.getLabel(), bindings);

        }

        return super.evaluate(sp, bindings);

    }

    /**
     * Evaluates the {@link BNS#SEARCH} magic predicate as a full-text
     * search against the index literal in the database, binding <i>svar</i>
     * to each matched literal in turn.
     * <p>
     * Note: The minimum cosine (relevance score) is set to ZERO (0d) in
     * order to make sure that any match within a literal qualifies that
     * literal for inclusion within the set of bindings that are
     * materialized. This is in contrast to a standard search engine, where
     * a minimum relevance score is used to filter out less likely matches.
     * However, it is my sense that query against the KB is often used to
     * find ALL matches. Regardless, the matches will be materialized in
     * order of decreasing relevance and an upper bound of 10000 matches is
     * imposed by the search implementation. See
     * {@link FullTextIndex#search(String, String, double, int)}.
     * 
     * @param svar
     *            The variable from the subject position of the
     *            {@link StatementPattern} in which the {@link BNS#SEARCH}
     *            magic predicate appears.
     * @param languageCode
     *            An optional language code from the bound literal appearing
     *            in the object position of that {@link StatementPattern}.
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
     * @todo consider options for term weights and normalization. Search on
     *       the KB is probably all terms with anything matching, stopwords
     *       are excluded, and term weights can be without normalization
     *       since ranking does not matter. And maxRank should probably be
     *       defeated (Integer.MAX_VALUE or ZERO - whichever does the
     *       trick).
     * 
     * @todo it would be nice if there were a way for the caller to express
     *       more parameters for the search, e.g., to give the minCosine and
     *       maxRank values directly as a tuple-based function call. I'm not
     *       sure if that is supported within Sesame/SPARQL.
     */
    protected CloseableIteration<BindingSet, QueryEvaluationException> search(
            Var svar, String languageCode, String label, BindingSet bindings)
            throws QueryEvaluationException {

        if (log.isInfoEnabled())
            log.info("languageCode=" + languageCode + ", label=" + label);

        final Iterator<IHit> itr;
        try {

            itr = database.getSearchEngine().search(label, languageCode,
                    0d/* minCosine */, 10000/* maxRank */);

        } catch (InterruptedException e) {

            throw new QueryEvaluationException("Interrupted.");

        }

        // Return an iterator that converts the term identifiers to var bindings
        return new HitConvertor(database, itr, svar, bindings);

    }

    /** @issue make protected in openrdf. */
    protected Value getVarValue(Var var, BindingSet bindings) {

        if (var == null) {

            return null;

        } else if (var.hasValue()) {

            return var.getValue();

        } else {

            return bindings.getValue(var.getName());

        }

    }

    /**
     * Uses native joins iff {@link BigdataSail.Options#NATIVE_JOINS} is
     * specified.
     * <p>
     * Note: As a pre-condition, the {@link Value}s in the query expression
     * MUST have been rewritten as {@link BigdataValue}s and their term
     * identifiers MUST have been resolved. Any term identifier that remains
     * {@link IRawTripleStore#NULL} is an indication that there is no entry for
     * that {@link Value} in the database. Since the JOINs are required (vs
     * OPTIONALs), that means that there is no solution for the JOINs and an
     * {@link EmptyIteration} is returned rather than evaluating the query.
     */
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
            Join join, BindingSet bindings) throws QueryEvaluationException
    {
        if (nativeJoins == false) {
            return super.evaluate(join, bindings);
        }
        
        if (log.isInfoEnabled()) log.info("evaluating native join");
        Collection<StatementPattern> stmtPatterns = 
            new LinkedList<StatementPattern>();
        collectStatementPatterns(join, stmtPatterns);
        if (log.isInfoEnabled()) {
            for (StatementPattern stmtPattern : stmtPatterns) {
                log.info(stmtPattern);
            }
        }
        // name of the SPO relation.
        final String SPO = database.getSPORelation().getNamespace();
        final Collection<IPredicate> tails = new LinkedList<IPredicate>();
        for (StatementPattern stmtPattern : stmtPatterns) {
            final IVariableOrConstant<Long> s;
            {
                Var var = stmtPattern.getSubjectVar();
                Value val = var.getValue();
                String name = var.getName();
                if (val == null) {
                    s = com.bigdata.relation.rule.Var.var(name);
                } else {
                    final Long id = ((BigdataValue)val).getTermId();
                    if (id.longValue() == NULL)
                        return new EmptyIteration<BindingSet, QueryEvaluationException>();
                    s = new Constant<Long>(id);
                }
            }
            final IVariableOrConstant<Long> p;
            {
                Var var = stmtPattern.getPredicateVar();
                Value val = var.getValue();
                String name = var.getName();
                if (val == null) {
                    p = com.bigdata.relation.rule.Var.var(name);
                } else {
                    final Long id = ((BigdataValue)val).getTermId();
                    if (id.longValue() == NULL)
                        return new EmptyIteration<BindingSet, QueryEvaluationException>();
                    p = new Constant<Long>(id);
                }
            }
            final IVariableOrConstant<Long> o;
            {
                Var var = stmtPattern.getObjectVar();
                Value val = var.getValue();
                String name = var.getName();
                if (val == null) {
                    o = com.bigdata.relation.rule.Var.var(name);
                } else {
                    final Long id = ((BigdataValue)val).getTermId();
                    if (id.longValue() == NULL)
                        return new EmptyIteration<BindingSet, QueryEvaluationException>();
                    o = new Constant<Long>(id);
                }
            }
            final IVariableOrConstant<Long> c;
            {
                final Var var = stmtPattern.getContextVar();
                if (var == null) {
                    // context position is not used.
                    c = null;
                } else {
                    final Value val = var.getValue();
                    if (val != null) {
                        /*
                         * Note: The context position is used as a statement
                         * identifier (SID). SIDs may be used to retrieve
                         * provenance statements (statements about statement)
                         * using high-level query. SIDs are represented as blank
                         * nodes and is not possible to have them bound in the
                         * original query. They only become bound during query
                         * evaluation.
                         */
                        throw new QueryEvaluationException(
                                "Context position is a statement identifier and may not be bound in the original query: "
                                        + join);
                    }
                    final String name = var.getName();
                    c = com.bigdata.relation.rule.Var.var(name);
                }
            }
            tails.add(new SPOPredicate(new String[] { SPO },//
                    s, p, o, c, //
                    false, // optional
                    !tripleSource.includeInferred ? ExplicitSPOFilter.INSTANCE : null,
                    null// expander
                    ));
        }
        final IRule rule = new Rule(
                "nativeJoin",
                null, // head
                tails.toArray(new IPredicate[tails.size()]),
                // constraints on the rule.
                null
        );

        return runQuery(join, rule);
        
    }
    
    private void collectStatementPatterns(TupleExpr tupleExpr, Collection<StatementPattern> stmtPatterns) {
        if (tupleExpr instanceof StatementPattern) {
            stmtPatterns.add((StatementPattern) tupleExpr);
        } else if (tupleExpr instanceof Join) {
            Join join = (Join) tupleExpr;
            TupleExpr left = join.getLeftArg();
            TupleExpr right = join.getRightArg();
            collectStatementPatterns(left, stmtPatterns);
            collectStatementPatterns(right, stmtPatterns);
        } else {
            throw new RuntimeException("encountered unexpected TupleExpr: "
                    + tupleExpr.getClass());
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

        if (log.isInfoEnabled()) {

            log.info("Running tupleExpr as native rule:\n" + tupleExpr + ",\n"
                    + rule);

        }

        // run the query as a native rule.
        final IChunkedOrderedIterator<ISolution> itr1;
        try {

            final IEvaluationPlanFactory planFactory = DefaultEvaluationPlanFactory2.INSTANCE;
            
            final IJoinNexusFactory joinNexusFactory = database
                    .newJoinNexusFactory(RuleContextEnum.HighLevelQuery,
                            ActionEnum.Query, IJoinNexus.BINDINGS,
                            null, // filter
                            false, // justify 
                            tripleSource.includeInferred, // backchain
                            planFactory
                            );

            final IJoinNexus joinNexus = joinNexusFactory.newInstance(database
                    .getIndexManager());
    
            itr1 = joinNexus.runQuery(rule);

        } catch (Exception ex) {
            
            throw new QueryEvaluationException(ex);
            
        }
        
        /*
         * Efficiently resolve term identifiers in Bigdata ISolutions to RDF
         * Values in Sesame 2 BindingSets.
         */
        final BigdataSolutionResolverator itr2 = new BigdataSolutionResolverator(
                database, itr1);

        /*
         * Align the iterator with the Sesame 2 API.
         */
        final Bigdata2Sesame2BindingSetIterator<QueryEvaluationException> itr3 = new Bigdata2Sesame2BindingSetIterator<QueryEvaluationException>(
                itr2);
        
        return itr3;
        
    }

}

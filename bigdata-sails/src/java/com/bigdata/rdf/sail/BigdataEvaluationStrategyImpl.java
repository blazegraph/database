package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;

import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;

import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rules.RuleContextEnum;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BNS;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.eval.ActionEnum;
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

    protected final BigdataTripleSource tripleSource;

    protected final Dataset dataset;

    private final AbstractTripleStore database;

    /**
     * @param tripleSource
     */
    public BigdataEvaluationStrategyImpl(BigdataTripleSource tripleSource) {

        this(tripleSource, null);

    }

    /**
     * @param tripleSource
     * @param dataset
     */
    public BigdataEvaluationStrategyImpl(BigdataTripleSource tripleSource,
            Dataset dataset) {

        super(tripleSource, dataset);

        this.tripleSource = tripleSource;

        this.dataset = null;

        this.database = tripleSource.getDatabase();

    }

    /**
     * FIXME override to evaluate {@link TupleExpr} queries using a rewrite onto
     * a native {@link Rule}.
     */
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
            TupleExpr tupleExpr, BindingSet bindings)
            throws QueryEvaluationException {

        if (log.isInfoEnabled()) {

            log.info("tupleExpr:\n" + tupleExpr);

        }

        if(false) {
            
            // FIXME REMOVE THIS CODE!
            log.error("Custom evaluation for LUBM QUERY 9");
            
            return evaluateLubmQuery9(tupleExpr, bindings);
            
        }
        
        return super.evaluate(tupleExpr, bindings);

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
     * Hardcodes query 9 from LUBM. The query is written as a rule, which is
     * formulated by hand in the code. The rule produces an iterator visiting
     * {@link ISolution}s whose bindings are term identifiers expressed as
     * {@link Long}s. That iterator is fed through a bulk resolving iterator
     * that efficiently converts the term identifiers for a chunk of solutions
     * at a time into RDF {@link Value}s. In turn, that iterator is wrapped to
     * make it conform with the Sesame 2 {@link CloseableIteration} and returned
     * to the caller.
     * 
     * <pre>
     * [query9]
     * PREFIX rdf: &lt;http://www.w3.org/1999/02/22-rdf-syntax-ns#&gt;
     * PREFIX ub: &lt;http://www.lehigh.edu/&tilde;zhp2/2004/0401/univ-bench.owl#&gt;
     * SELECT ?x ?y ?z
     * WHERE{
     *     ?x a ub:Student .
     *     ?y a ub:Faculty .
     *     ?z a ub:Course .
     *     ?x ub:advisor ?y .
     *     ?y ub:teacherOf ?z .
     *     ?x ub:takesCourse ?z .
     *     }
     * </pre>
     * 
     * @todo make the rule head optional (required for mutation, but optional
     *       for query).
     * 
     * @todo is there additional information in the lubm ontology that would
     *       allow us to interpret the range and domain as hard constraints such
     *       that we do not also need to evaluate the ({x,y,z} type
     *       (Student,Faculty,Course}) constraints?
     */
    @SuppressWarnings("unchecked")
    private CloseableIteration<BindingSet, QueryEvaluationException> evaluateLubmQuery9(
            TupleExpr tupleExpr, BindingSet bindings)
            throws QueryEvaluationException {

        final IRule rule;
        {

            // name of the SPO relation.
            final String SPO = database.getSPORelation().getNamespace();

//            // name of the LEXICON relation.
//            final String LEX = database.getLexiconRelation().getNamespace();

            // RDFS vocabulary.
//          final RDFSVocabulary vocab = database.getInferenceEngine();
//          final IConstant<Long> rdfType = vocab.rdfType;
            final BigdataValueFactory valueFactory = database.getValueFactory();

            // prefix used by the query.
            final String ub = "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#";

            final BigdataURI rdfType = valueFactory.asValue(RDF.TYPE);
            final BigdataURI Student = valueFactory.createURI(ub + "Student"); 
            final BigdataURI Faculty = valueFactory.createURI(ub + "Faculty"); 
            final BigdataURI Course  = valueFactory.createURI(ub + "Course"); 
            final BigdataURI advisor = valueFactory.createURI(ub + "advisor"); 
            final BigdataURI teacherOf = valueFactory.createURI(ub + "teacherOf"); 
            final BigdataURI takesCourse = valueFactory.createURI(ub + "takesCourse"); 

            /*
             * Resolve the values used by this query.
             * 
             * Note: If any value can not be resolved, then its term identifer
             * will remain ZERO (0L) (aka NULL). Except within OPTIONALs, this
             * indicates that the query CAN NOT be satisified by the data since
             * one or more required terms are unknown to the database.
             */
            {
                
                final Collection<BigdataValue> values = new Vector<BigdataValue>();

                values.add(rdfType);
                values.add(Student);
                values.add(Faculty);
                values.add(Course);
                values.add(advisor);
                values.add(teacherOf);
                values.add(takesCourse);

                final BigdataValue[] terms = values
                        .toArray(new BigdataValue[] {});

                database.getLexiconRelation().addTerms(terms, terms.length,
                        true/* readOnly */);

                for(int i=0; i<terms.length; i++) {
                    
                    if (terms[i].getTermId() == IRawTripleStore.NULL) {

                        // No match possible.
                        return new EmptyIteration<BindingSet, QueryEvaluationException>();
                        
                    }
                    
                }
            
                
            }

//                * SELECT ?x ?y ?z
//                * WHERE{
//                *     ?x a ub:Student .
//                *     ?y a ub:Faculty .
//                *     ?z a ub:Course .
//                *     ?x ub:advisor ?y .
//                *     ?y ub:teacherOf ?z .
//                *     ?x ub:takesCourse ?z .
//                *     }

            rule = new Rule(
                    "lubmQuery9",
                    null, // head
                    new IPredicate[] { // tail
                            new SPOPredicate(
                                    SPO,//
                                    com.bigdata.relation.rule.Var.var("x"),
                                    new Constant<Long>(rdfType.getTermId()),
                                    new Constant<Long>(Student.getTermId())),
                            new SPOPredicate(
                                    SPO, //
                                    com.bigdata.relation.rule.Var.var("y"),
                                    new Constant<Long>(rdfType.getTermId()),
                                    new Constant<Long>(Faculty.getTermId())),
                            new SPOPredicate(
                                    SPO,//
                                    com.bigdata.relation.rule.Var.var("z"),
                                    new Constant<Long>(rdfType.getTermId()),
                                    new Constant<Long>(Course.getTermId())),
                            new SPOPredicate(
                                    SPO, //
                                    com.bigdata.relation.rule.Var.var("x"),
                                    new Constant<Long>(advisor.getTermId()),
                                    com.bigdata.relation.rule.Var.var("y")),
                            new SPOPredicate(
                                    SPO, //
                                    com.bigdata.relation.rule.Var.var("y"),
                                    new Constant<Long>(teacherOf.getTermId()),
                                    com.bigdata.relation.rule.Var.var("z")),
                            new SPOPredicate(
                                    SPO, //
                                    com.bigdata.relation.rule.Var.var("x"),
                                    new Constant<Long>(takesCourse.getTermId()),
                                    com.bigdata.relation.rule.Var.var("z"))
                            //
                            },
                    // constraints on the rule.
                    null
            );
            
        }
        
        // run the query as a native rule.
        final IChunkedOrderedIterator<ISolution> itr1;
        try {

            final IJoinNexusFactory joinNexusFactory = database
                    .newJoinNexusFactory(RuleContextEnum.HighLevelQuery,
                            ActionEnum.Query, IJoinNexus.BINDINGS,
                            null/* filter */, false/* justify */);

            final IJoinNexus joinNexus = joinNexusFactory.newInstance(database
                    .getIndexManager());
    
            itr1 = joinNexus.runQuery(rule);
            
            if (true) {

                while (itr1.hasNext()) {

                    itr1.next();

                }

                itr1.close();

                throw new UnsupportedOperationException();

            }
            
        } catch (Exception ex) {
            
            throw new QueryEvaluationException(ex);
            
        }
        
        /*
         * Efficiently resolve term identifiers in Bigdata ISolutions to RDF
         * Values in Sesame 2 BindingSets.
         */

        final BigdataSolutionResolverator itr2 = new BigdataSolutionResolverator(
                database, itr1);

        // align exceptions for SAIL with those for Query.
        return new QueryEvaluationIterator<BindingSet>(itr2);
        
    }

}

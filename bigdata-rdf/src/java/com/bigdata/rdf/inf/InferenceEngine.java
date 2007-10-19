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
package com.bigdata.rdf.inf;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.sesame.constants.RDFFormat;
import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;

import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.inf.Rule.Stats;
import com.bigdata.rdf.inf.TestMagicSets.MagicRule;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.model.OptimizedValueFactory._Statement;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.rdf.util.KeyOrder;
import com.bigdata.rdf.util.RdfKeyBuilder;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Adds support for RDFS inference.
 * <p>
 * A fact always has the form:
 * 
 * <pre>
 * triple(s, p, o)
 * </pre>
 * 
 * where s, p, and or are identifiers for RDF values in the terms index. Facts
 * are stored either in the long-term database or in a per-query answer set.
 * <p>
 * A rule always has the form:
 * 
 * <pre>
 *      pred :- pred*.
 * </pre>
 * 
 * where <i>pred</i> is either
 * <code>magic(triple(varOrId,varOrId,varOrId))</code> or
 * <code>triple(varOrId,varOrId,varOrId)</code>. A rule is a clause
 * consisting of a head (a predicate) and a body (one or more predicates). Note
 * that the body of the rule MAY be empty. When there are multiple predicates in
 * the body of a rule the rule succeeds iff all predicates in the body succeed.
 * When a rule succeeds, the head of the clause is asserted. If the head is a
 * predicate then it is asserted into the rule base for the query. If it is a
 * fact, then it is asserted into the database for the query. Each predicate has
 * an "arity" with is the number of arguments, e.g., the predicate "triple" has
 * an arity of 3 and may be written as triple/3 while the predicate "magic" has
 * an arity of 1 and may be written as magic/1.
 * <p>
 * A copy is made of the basic rule base at the start of each query and a magic
 * transform is applied to the rule base, producing a new rule base that is
 * specific to the query. Each query is also associated with an answer set in
 * which facts are accumulated. Query execution consists of iteratively applying
 * all rules in the rule base. Execution terminates when no new facts or rules
 * are asserted in a given iteration - this is the <i>fixed point</i> of the
 * query.
 * <p>
 * Note: it is also possible to run the rule set without creating a magic
 * transform. This will produce the full forward closure of the entailments.
 * This is done by using the statements loaded from some source as the source
 * fact base and inserting the entailments created by the rules back into
 * statement collection. When the rules reach their fixed point, the answer set
 * contains both the told triples and the inferred triples and is simply
 * inserted into the long-term database.
 * <p>
 * rdfs9 is represented as:
 * 
 * <pre>
 *       triple(?v,rdf:type,?x) :-
 *          triple(?u,rdfs:subClassOf,?x),
 *          triple(?v,rdf:type,?u). 
 * </pre>
 * 
 * rdfs11 is represented as:
 * 
 * <pre>
 *       triple(?u,rdfs:subClassOf,?x) :-
 *          triple(?u,rdfs:subClassOf,?v),
 *          triple(?v,rdf:subClassOf,?x). 
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo provide option for "owl:sameAs" semantics using destructive merging
 *       (the terms are assigned the same term identifier, one of them is
 *       treated as a canonical, and there is no way to retract the sameAs
 *       assertion).
 * 
 * @todo experiment with use of a bloom filter
 * 
 * @todo provide fixed point transitive closure for "chain" rules (subClassOf)
 * 
 * @todo examine ways to prune the search space and avoid re-triggering rules or
 *       being more selective with rule triple patterns as a consequence of new
 *       entailments (avoid re-computing all entailments already proven in each
 *       pass).
 */
public class InferenceEngine implements ITripleStore {

    /**
     * The delegate.
     */
    final protected ITripleStore tripleStore;
    
    /**
     * The delegate {@link ITripleStore}.
     */
    public ITripleStore getTripleStore() {
    
        return tripleStore;
        
    }
    
    /**
     * Used to assign unique variable identifiers.
     */
    private long nextVar = -1;

    /**
     * Return the next unique variable identifier.
     */
    protected Var nextVar() {

        return new Var(nextVar--);

    }

    /*
     * Identifiers for well-known RDF values. 
     */
    Id rdfType;
    Id rdfProperty;
    Id rdfsSubClassOf;
    Id rdfsSubPropertyOf;
    Id rdfsDomain;
    Id rdfsRange;
    Id rdfsClass;
    Id rdfsResource;
    Id rdfsCMP;
    Id rdfsDatatype;
    Id rdfsMember;
    Id rdfsLiteral;

    /*
     * Rules.
     */
    Rule rdf1;
    Rule rdfs2;
    Rule rdfs3;
    Rule rdfs5;
    Rule rdfs6;
    Rule rdfs7;
    Rule rdfs8;
    Rule rdfs9;
    Rule rdfs10;
    Rule rdfs11;
    Rule rdfs12;
    Rule rdfs13;

    /**
     * All rules defined by the inference engine.
     */
    Rule[] rules;

    /**
     * @param properties
     * @throws IOException
     */
    public InferenceEngine(ITripleStore tripleStore) {

        if(tripleStore==null) throw new IllegalArgumentException();
        
        this.tripleStore = tripleStore;

        setup();

    }

    /**
     * Sets up the inference engine.
     */
    protected void setup() {

        setupIds();

        setupRules();

    }

    /**
     * Resolves or defines well-known RDF values.
     * 
     * @see #rdfType and friends which are initialized by this method.
     * 
     * @todo make this into a batch operation.
     */
    protected void setupIds() {

        rdfType = new Id(addTerm(new _URI(RDF.TYPE)));

        rdfProperty = new Id(addTerm(new _URI(RDF.PROPERTY)));

        rdfsSubClassOf = new Id(addTerm(new _URI(RDFS.SUBCLASSOF)));

        rdfsSubPropertyOf = new Id(addTerm(new _URI(RDFS.SUBPROPERTYOF)));

        rdfsDomain = new Id(addTerm(new _URI(RDFS.DOMAIN)));

        rdfsRange = new Id(addTerm(new _URI(RDFS.RANGE)));

        rdfsClass = new Id(addTerm(new _URI(RDFS.CLASS)));
        
        rdfsResource = new Id(addTerm(new _URI(RDFS.RESOURCE)));
        
        rdfsCMP = new Id(addTerm(new _URI(RDFS.CONTAINERMEMBERSHIPPROPERTY)));
        
        rdfsDatatype = new Id(addTerm(new _URI(RDFS.DATATYPE)));
        
        rdfsMember = new Id(addTerm(new _URI(RDFS.MEMBER)));
        
        rdfsLiteral = new Id(addTerm(new _URI(RDFS.LITERAL)));
    
    }

    public void setupRules() {

        rdf1 = new RuleRdf01(this,nextVar(),nextVar(),nextVar());
        
        rdfs2 = new RuleRdfs02(this,nextVar(),nextVar(),nextVar(),nextVar());

        rdfs3 = new RuleRdfs03(this,nextVar(),nextVar(),nextVar(),nextVar());

        rdfs5 = new RuleRdfs05(this,nextVar(),nextVar(),nextVar());

        rdfs6 = new RuleRdfs06(this,nextVar(),nextVar(),nextVar());

        rdfs7 = new RuleRdfs07(this,nextVar(),nextVar(),nextVar(),nextVar());

        rdfs8 = new RuleRdfs08(this,nextVar(),nextVar(),nextVar());

        rdfs9 = new RuleRdfs09(this,nextVar(),nextVar(),nextVar());

        rdfs10 = new RuleRdfs10(this,nextVar(),nextVar(),nextVar());

        rdfs11 = new RuleRdfs11(this,nextVar(),nextVar(),nextVar());

        rdfs12 = new RuleRdfs12(this,nextVar(),nextVar(),nextVar());

        rdfs13 = new RuleRdfs13(this,nextVar(),nextVar(),nextVar());

        // rules = new Rule[] { rdfs9, rdfs11 };
        
        rules = new Rule[] { rdf1, rdfs2, rdfs3, rdfs5, rdfs6, rdfs7, rdfs8, rdfs9, rdfs10, rdfs11, rdfs12, rdfs13 };

    }

    public void addStatement(Resource s, URI p, Value o) {
        tripleStore.addStatement(s, p, o);
    }

    public void addStatements(_Statement[] stmts, int numStmts) {
        tripleStore.addStatements(stmts, numStmts);
    }

    public long addTerm(Value value) {
        return tripleStore.addTerm(value);
    }

    public boolean containsStatement(Resource s, URI p, Value o) {
        return tripleStore.containsStatement(s, p, o);
    }

    public ArrayList<Long> distinctTermScan(KeyOrder keyOrder) {
        return tripleStore.distinctTermScan(keyOrder);
    }

    public void generateSortKeys(RdfKeyBuilder keyBuilder, _Value[] terms, int numTerms) {
        tripleStore.generateSortKeys(keyBuilder, terms, numTerms);
    }

    public int getBNodeCount() {
        return tripleStore.getBNodeCount();
    }

    public IIndex getIdTermIndex() {
        return tripleStore.getIdTermIndex();
    }

    public RdfKeyBuilder getKeyBuilder() {
        return tripleStore.getKeyBuilder();
    }

    public int getLiteralCount() {
        return tripleStore.getLiteralCount();
    }

    public IIndex getOSPIndex() {
        return tripleStore.getOSPIndex();
    }

    public IIndex getPOSIndex() {
        return tripleStore.getPOSIndex();
    }

    public Properties getProperties() {
        return tripleStore.getProperties();
    }

    public IIndex getSPOIndex() {
        return tripleStore.getSPOIndex();
    }

    public int getStatementCount() {
        return tripleStore.getStatementCount();
    }

    public IIndex getStatementIndex(KeyOrder keyOrder) {
        return tripleStore.getStatementIndex(keyOrder);
    }

    public _Value getTerm(long id) {
        return tripleStore.getTerm(id);
    }

    public int getTermCount() {
        return tripleStore.getTermCount();
    }

    public long getTermId(Value value) {
        return tripleStore.getTermId(value);
    }

    public IIndex getTermIdIndex() {
        return tripleStore.getTermIdIndex();
    }

    public int getURICount() {
        return tripleStore.getURICount();
    }

    public void insertTerms(_Value[] terms, int numTerms, boolean haveKeys, boolean sorted) {
        tripleStore.insertTerms(terms, numTerms, haveKeys, sorted);
    }

    public LoadStats loadData(File file, String baseURI, RDFFormat rdfFormat, boolean verifyData, boolean commit) throws IOException {
        return tripleStore.loadData(file, baseURI, rdfFormat, verifyData, commit);
    }

    public int rangeCount(long s, long p, long o) {
        return tripleStore.rangeCount(s, p, o);
    }

    public IEntryIterator rangeQuery(long s, long p, long o) {
        return tripleStore.rangeQuery(s, p, o);
    }

    public int removeStatements(long _s, long _p, long _o) {
        return tripleStore.removeStatements(_s, _p, _o);
    }

    public int removeStatements(Resource s, URI p, Value o) {
        return tripleStore.removeStatements(s, p, o);
    }

    public String toString(long s, long p, long o) {
        return tripleStore.toString(s, p, o);
    }

    public String toString(long termId) {
        return tripleStore.toString(termId);
    }
    
    public void usage() {
        tripleStore.usage();
    }
    
    public void dumpStore() {
        tripleStore.dumpStore();
    }

    public void commit() {
        tripleStore.commit();
    }

    public boolean isStable() {
        return tripleStore.isStable();
    }
    
    public void clear() {
        tripleStore.clear();
    }
    
    public void close() {
        tripleStore.close();
    }
    
    public void closeAndDelete() {
        tripleStore.closeAndDelete();
    }
    
    /**
     * Compute the complete forward closure of the store using a set-at-a-time
     * inference strategy.
     * <p>
     * The general approach is a series of rounds in which each rule is applied
     * to all data in turn. The rules directly embody queries that cause only
     * the statements which can trigger the rule to be visited. Since most rules
     * require two antecedents, this typically means that the rules are running
     * two range queries and performing a join operation in order to identify
     * the set of rule firings. Entailments computed in each round are fed back
     * into the source against which the rules can match their preconditions, so
     * derived entailments may be computed in a succession of rounds. The
     * process halts when no new entailments are computed in a given round.
     * <p>
     * 
     * @todo Rules can be computed in parallel using a pool of worker threads.
     *       The round ends when the queue of rules to process is empty and all
     *       workers are done.
     * 
     * @todo The entailments computed in each round are inserted back into the
     *       primary triple store at the end of the round. The purpose of this
     *       is to read from a fused view of the triples already in the primary
     *       store and those that have been computed by the last application of
     *       the rules. This is necessary in order for derived entailments to be
     *       computed. However, an alternative approach would be to explicitly
     *       read from a fused view of the indices in the temporary store and
     *       those in the primary store (or simply combining their iterators in
     *       the rules). This could have several advantages and an approach like
     *       this is necessary in order to compute entailments at query time
     *       that are not to be inserted back into the kb.
     * 
     * @todo The SPO[] buffer might be better off as a more interesting
     *       structure that only accepted the distinct statements. This was a
     *       big win for the batch oriented parser and would probably eliminate
     *       even more duplicates in the context of the inference engine.
     * 
     * @todo support closure of a document against an ontology and then bulk
     *       load the result into the store.
     */
    public void fullForwardClosure() {

        /*
         * @todo configuration paramater.
         * 
         * There is a factor of 2 performance difference for a sample data set
         * from a buffer size of one (unordered inserts) to a buffer size of
         * 10k.
         */
        final int BUFFER_SIZE = 100 * Bytes.kilobyte32;
        
        /*
         * Note: Unlike the parser buffer, making statements distinct appears
         * to slow things down significantly (2x slower!).
         */
        final boolean distinct = false;

        final Rule[] rules = this.rules;

        final long[] timePerRule = new long[rules.length];
        
        final int[] entailmentsPerRule = new int[rules.length];
        
        final int nrules = rules.length;

        final int firstStatementCount = getStatementCount();

        final long begin = System.currentTimeMillis();

        log.debug("Closing kb with " + firstStatementCount
                + " statements");

        int round = 0;

        /*
         * The temporary store used to accumulate the entailments.
         */
        TempTripleStore tmpStore = new TempTripleStore(getProperties()); 

        /*
         * This is a buffer that is used to hold entailments so that we can
         * insert them into the indices using ordered insert operations (much
         * faster than random inserts). The buffer is reused by each rule. The
         * rule assumes that the buffer is empty and just keeps a local counter
         * of the #of entailments that it has inserted into the buffer. When the
         * buffer overflows, those entailments are transfered enmass into the
         * tmp store.
         */
        final SPOBuffer buffer = new SPOBuffer(tmpStore, BUFFER_SIZE, distinct);
        
        Stats totalStats = new Stats();
        
        while (true) {

            final int numEntailmentsBefore = tmpStore.getStatementCount();
            
            for (int i = 0; i < nrules; i++) {

                Stats ruleStats = new Stats();
                
                Rule rule = rules[i];

                int nbefore = ruleStats.numComputed;
                
                rule.apply( ruleStats, buffer );
                
                int nnew = ruleStats.numComputed - nbefore;

                // #of statements examined by the rule.
                int nstmts = ruleStats.stmts1 + ruleStats.stmts2;
                
                long elapsed = ruleStats.computeTime;
                
                timePerRule[i] += elapsed;
                
                entailmentsPerRule[i] = ruleStats.numComputed; // Note: already a running sum.
                
                long stmtsPerSec = (nstmts == 0 || elapsed == 0L ? 0
                        : ((long) (((double) nstmts) / ((double) elapsed) * 1000d)));
                                
                if (DEBUG||true) {
                    log.debug("round# " + round + ", "
                            + rule.getClass().getSimpleName()
                            + ", entailments=" + nnew + ", #stmts1="
                            + ruleStats.stmts1 + ", #stmts2="
                            + ruleStats.stmts2 + ", #subqueries="
                            + ruleStats.numSubqueries
                            + ", #stmtsExaminedPerSec=" + stmtsPerSec);
                }
                
                totalStats.numComputed += ruleStats.numComputed;
                
                totalStats.computeTime += ruleStats.computeTime;
                
            }

            if(true) {
            
                /*
                 * Show times for each rule so far.
                 */
                System.err.println("rule    \tms\t#entms\tentms/ms");
                
                for(int i=0; i<timePerRule.length; i++) {
                    
                    System.err.println(rules[i].getClass().getSimpleName()
                            + "\t"
                            + timePerRule[i]
                            + "\t"
                            + entailmentsPerRule[i]
                            + "\t"
                            + (timePerRule[i] == 0 ? 0 : entailmentsPerRule[i]
                                    / timePerRule[i]));
                    
                }
                
            }
            
            /*
             * Flush the statements in the buffer to the temporary store. 
             */
            buffer.flush();

            final int numEntailmentsAfter = tmpStore.getStatementCount();
            
            if ( numEntailmentsBefore == numEntailmentsAfter ) {
                
                // This is the fixed point.
                break;
                
            }
            
            /*
             * Transfer the entailments into the primary store so that derived
             * entailments may be computed.
             */
            final long insertStart = System.currentTimeMillis();

            final int numInserted = copyStatements(tmpStore);

            final long insertTime = System.currentTimeMillis() - insertStart;

            if (DEBUG) {
            StringBuilder debug = new StringBuilder();
            debug.append( "round #" ).append( round ).append( ": " );
            debug.append( totalStats.numComputed ).append( " computed in " );
            debug.append( totalStats.computeTime ).append( " millis, " );
            debug.append( numInserted ).append( " inserted in " );
            debug.append( insertTime ).append( " millis " );
            log.debug( debug.toString() );
            }

            round++;
            
        }

        final long elapsed = System.currentTimeMillis() - begin;

        final int lastStatementCount = getStatementCount();

        if (INFO) {
        
            final int inferenceCount = lastStatementCount - firstStatementCount;
            
            log.info("Computed closure of store in "
                            + round + " rounds and "
                            + elapsed
                            + "ms yeilding "
                            + lastStatementCount
                            + " statements total, "
                            + (inferenceCount)
                            + " inferences"
                            + ", entailmentsPerSec="
                            + ((long) (((double) inferenceCount)
                                    / ((double) elapsed) * 1000d)));

        }

    }

    /**
     * Fast forward closure of the store based on <a
     * href="http://www.cs.iastate.edu/~tukw/waim05.pdf">"An approach to RDF(S)
     * Query, Manipulation and Inference on Databases" by Lu, Yu, Tu, Lin, and
     * Zhang</a>.
     * 
     * @todo can I reuse the rule implementations that we have already defined?
     * 
     * @todo Do not run 14 or 15 since they produce rdfs:Resource.
     * 
     * @todo If the head is (x, rdf:type, rdfs:Resource) then do not insert into
     *       the store (filter at the reasoner level only).
     * 
     * @todo mark axiom vs explicit vs inferred and reserve a bit for suspended
     *       for each statement.
     * 
     * @todo store the proofs in an index: key := [head][tail]. The rule could
     *       be the value, or the step in the algorithm could be the value, or
     *       the value could be null.
     *       <p>
     *       Can this approach produce ungrounded justification chains?
     * 
     * @todo make entailments for rdfs:domain and rdfs:range optional. we can
     *       get by with just Rdfs5, Rdfs7, Rdfs9, and Rdfs11.
     * 
     * @todo test on alibaba (entity-link data) as well as on ontology heavy
     *       (nciOncology).
     * 
     * @todo run the metrics test suites.
     * 
     * @todo owl:sameAs by backward chaining on query.
     * @todo rdfs:Resource by backward chaining on query.
     * 
     * @todo We don’t do owl:equivalentClass and owl:equivalentProperty
     *       currently. You can simulate those by doing a bi-directional
     *       subClassOf or subPropertyOf, which has always sufficed for our
     *       purposes. If you were going to write rules for those two things
     *       this is how you would do it:
     * 
     * <pre>
     *   equivalentClass:
     *   
     *     add an axiom to the KB: equivalentClass subPropertyOf subClassOf
     *     add an entailment rule: xxx equivalentClass yyy à yyy equivalentClass xxx
     * </pre>
     * 
     * It would be analogous for equivalentProperty.
     * 
     * @todo can some steps be run in parallel? E.g., [5,6,7]?
     */
    public void fastForwardClosure() {

        /*
         * Note: The steps below are numbered with regard to the paper cited in
         * the javadoc above.
         * 
         * Most steps presume that the computed entailments have been added to
         * the database (vs the temp store).
         */
        
        /*
         * The temporary store used to accumulate the entailments.
         */
        TempTripleStore tmpStore = new TempTripleStore(getProperties()); 

        // 1. add RDF(S) axioms to the database.
        addRdfsAxioms(tmpStore); // add to temp store.
        copyStatements(tmpStore); // copy to database.
        
        // 2-4. Calculate the rdfs:subPropertyOf closure, returning D,C,R,T.
        // doRdfsSubPropertyOfClosure(tmpStore);

        // 5. (?x, D, ?y ) -> (?x, rdfs:domain, ?y)
        
        // 6. (?x, R, ?y ) -> (?x, rdfs:range, ?y)

        // 7. (?x, C, ?y ) -> (?x, rdfs:subClassOf, ?y)

        // 8. Calculate the rdfs:subClassOf closure.
        
        // 9. (?x, T, ?y ) -> (?x, rdf:type, ?y)

        // 10. RuleRdfs02
        
        /*
         * 11. special rule w/ 3-part antecedent.
         * 
         * (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:domain, ?b) ->
         * (?x, rdf:type, ?b).
         */
        
        // 12. RuleRdfs03
        
        /* 13. special rule w/ 3-part antecedent.
         * 
         * (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:range, ?b ) ->
         * (?x, rdf:type, ?b )
         */
        
        /*
         * 14-15. These steps skipped. They correspond to rdfs4a and rdfs4b and
         * generate rdfs:Resource assertions.
         */

        // 16. RuleRdf01
        
        // 17. RuleRdfs09
        
        // 18. RuleRdfs10
        
        // 19. RuleRdfs08. Generates (x?, rdfs:subClassOf, rdfs:Resource).

        // 20. RuleRdfs13.
        
        // 21. RuleRdfs06.
        
        // 22. RuleRdfs07.
        
        // Done.
    
    }

    /**
     * Add the axiomatic RDF(S) triples to the store.
     * <p>
     * Note: The termIds are defined with respect to the backing triple store
     * since the axioms will be copied into the store when the closure is
     * complete.
     * 
     * @param store
     *            The store to which the axioms will be added.
     */
    private void addRdfsAxioms(ITripleStore store) {
        
        Axioms axiomModel = RdfsAxioms.INSTANCE;

        /* 
         * Cache the URI -> termId mapping for the persistent database.
         * 
         * @todo do once in the ctor.
         */
        Map<String,_URI> uriCache = cacheURIs(axiomModel.getVocabulary());
        
        _Statement[] stmts = new _Statement[axiomModel.getAxioms().size()];
        
        // add the axioms to the graph
        
        int numStmts = 0;

        for (Iterator<Axioms.Triple> itr = axiomModel.getAxioms().iterator(); itr
                .hasNext();) {

            Axioms.Triple triple = itr.next();
            
            _URI s = uriCache.get(triple.getS().getURI());
            
            _URI p = uriCache.get(triple.getP().getURI());
            
            _URI o = uriCache.get(triple.getO().getURI());
            
            _Statement stmt = new _Statement(s, p, o, StatementEnum.Axiom);

            stmts[numStmts++] = stmt;
            
        }

        store.addStatements(stmts, numStmts);
        
    }
    
    /**
     * Create a vocabulary cache for URIs as defined in the {@link #tripleStore}.
     * <p>
     * Note: The URIs are inserted into the {@link #tripleStore} iff they are
     * not already defined. This ensures that statements generated using the
     * assigned term identifiers will have the correct term identifiers for the
     * target {@link ITripleStore}.
     * 
     * @param uri
     *            The uri.
     * 
     * @return The URI as defined in the {@link #tripleStore}.
     * 
     * @todo use the batch addTerm method.
     */
    private Map<String, _URI> cacheURIs(Set<String> uris) {

        HashMap<String, _URI> uriCache = new HashMap<String, _URI>();

        for (Iterator<String> it = uris.iterator(); it.hasNext();) {

            String uri = it.next();

            _URI tmp = uriCache.get(uri);

            if (tmp == null) {

                tmp = new _URI(uri);

                tripleStore.addTerm(tmp);

                uriCache.put(uri, tmp);

            }

        }

        return uriCache;
        
    }
    
    /**
     * Copies the statements from the temporary store into the main store.
     * <p>
     * Note: The statements in the {@link TempTripleStore} are NOT removed.
     * 
     * @param tmpStore
     *            The temporary store.
     * 
     * @return The #of statements inserted into the main store (the count only
     *         reports those statements that were not already in the main
     *         store).
     */
    public int copyStatements( final TempTripleStore tmpStore ) {

        List<Callable<Long>> tasks = new LinkedList<Callable<Long>>();
        
        tasks.add( new CopyStatements(tmpStore.getSPOIndex(),getSPOIndex()));
        tasks.add( new CopyStatements(tmpStore.getPOSIndex(),getPOSIndex()));
        tasks.add( new CopyStatements(tmpStore.getOSPIndex(),getOSPIndex()));
        
        final long numInserted;
        
        try {

            final List<Future<Long>> futures = indexWriteService.invokeAll(tasks);

            final long numInserted1 = futures.get(0).get();
            
            final long numInserted2 = futures.get(1).get();
            
            final long numInserted3 = futures.get(2).get();

            assert numInserted1 == numInserted2;
            
            assert numInserted1 == numInserted3;
            
            numInserted = numInserted1;
        
        } catch (Exception ex) {
            
            throw new RuntimeException(ex);
            
        }
        
        return (int) numInserted;

    }
    
    /**
     * Copies statements from one index to another. 
     */
    static class CopyStatements implements Callable<Long> {

        private final IIndex src;
        private final IIndex dst;
        
        /**
         * @param src
         * @param dst
         */
        CopyStatements(IIndex src, IIndex dst) {
            
            this.src = src;
            
            this.dst = dst;
            
        }
        
        public Long call() throws Exception {
            
            long numInserted = 0;
            
            IEntryIterator it = src.rangeIterator(null, null);
            
            while (it.hasNext()) {

                it.next();
                
                byte[] key = it.getKey();
                
                if (!dst.contains(key)) {

                    // @todo copy the statement type.
                    // @todo upgrade the statement type if necessary (inferred -> explicit).
                    dst.insert(key, null);
                    
                    numInserted++;
                    
                }
                
            }
            
            return numInserted;
        }
        
    };
    
    /**
     * A service used to write on each of the statement indices in parallel.
     */
    protected ExecutorService indexWriteService = Executors.newFixedThreadPool(3,
            DaemonThreadFactory.defaultThreadFactory());

    /**
     * Extracts the object ids from a key scan.
     * 
     * @param itr
     *            The key scan iterator.
     * 
     * @return The objects visited by that iterator.
     */
    public SPO[] getStatements(IIndex ndx, KeyOrder keyOrder, byte[] fromKey, byte[] toKey) {

        final RdfKeyBuilder keyBuilder = getKeyBuilder();
        
        final int n = ndx.rangeCount(fromKey, toKey);

        // buffer for storing the extracted s:p:o data.
        SPO[] ids = new SPO[n];

        IEntryIterator itr1 = ndx.rangeIterator(fromKey, toKey);

        int i = 0;

        while (itr1.hasNext()) {

            itr1.next();
            ids[i++] = new SPO(keyOrder,keyBuilder,itr1.getKey());

        }

        assert i == n;

        return ids;

    }

    /**
     * Accepts a triple pattern and returns the closure over that triple pattern
     * using a magic transform of the RDFS entailment rules.
     * 
     * @param query
     *            The triple pattern.
     * 
     * @param rules
     *            The rules to be applied.
     * 
     * @return The answer set.
     * 
     * @exception IllegalArgumentException
     *                if query is null.
     * @exception IllegalArgumentException
     *                if query is a fact (no variables).
     * 
     * FIXME Magic sets has NOT been implemented -- this method does NOT
     * function.
     */
    public ITripleStore query(Triple query, Rule[] rules) throws IOException {

        if (query == null)
            throw new IllegalArgumentException("query is null");

        if (query.isFact())
            throw new IllegalArgumentException("no variables");

        if (rules == null)
            throw new IllegalArgumentException("rules is null");

        if (rules.length == 0)
            throw new IllegalArgumentException("no rules");
        
        final int nrules = rules.length;

        /*
         * prepare the magic transform of the provided rules.
         */
        
        Rule[] rules2 = new Rule[nrules];
        
        for( int i=0; i<nrules; i++ ) {

            rules2[i] = new MagicRule(this,rules[i]);

        }
        
        /*
         * @todo create the magic seed and insert it into the answer set.
         */
        Magic magicSeed = new Magic(query);

        /*
         * Run the magic transform.
         */
        
        /*
         * @todo support bufferQueue extension for the transient mode or set the
         * default capacity to something larger.  if things get too large
         * then we need to spill over to disk.
         */
        
        ITripleStore answerSet = new TempTripleStore(getProperties());
        
        int lastStatementCount = getStatementCount();

        final long begin = System.currentTimeMillis();

        System.err.println("Running query: "+query);

        int nadded = 0;

        while (true) {

            for (int i = 0; i < nrules; i++) {

                Rule rule = rules[i];

                // nadded += rule.apply();
                // rule.apply();

            }

            int statementCount = getStatementCount();

            // testing the #of statement is less prone to error.
            if (lastStatementCount == statementCount) {

                //                if( nadded == 0 ) { // should also work.

                // This is the fixed point.
                break;

            }

        }

        final long elapsed = System.currentTimeMillis() - begin;

        System.err.println("Ran query in " + elapsed + "ms; "
                + lastStatementCount + " statements in answer set.");

        return answerSet;
        
    }

}

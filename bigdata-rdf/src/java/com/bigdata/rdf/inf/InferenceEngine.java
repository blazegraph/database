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

import java.io.IOException;
import java.util.Properties;

import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;

import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Options;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.KeyOrder;
import com.bigdata.rdf.TempTripleStore;
import com.bigdata.rdf.TripleStore;
import com.bigdata.rdf.inf.Rule.Stats;
import com.bigdata.rdf.inf.TestMagicSets.MagicRule;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;

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
 *    pred :- pred*.
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
 *     triple(?v,rdf:type,?x) :-
 *        triple(?u,rdfs:subClassOf,?x),
 *        triple(?v,rdf:type,?u). 
 * </pre>
 * 
 * rdfs11 is represented as:
 * 
 * <pre>
 *     triple(?u,rdfs:subClassOf,?x) :-
 *        triple(?u,rdfs:subClassOf,?v),
 *        triple(?v,rdf:subClassOf,?x). 
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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
public class InferenceEngine extends TripleStore {

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
    public InferenceEngine(Properties properties) throws IOException {

        super(properties);

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
     * @todo can the dependency array among the rules be of use to us when we
     *       are computing full foward closure as opposed to using magic sets to
     *       answer a specific query?
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
        TempTripleStore tmpStore = new TempTripleStore(); 

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
            debug.append( "round #" ).append( round++ ).append( ": " );
            debug.append( totalStats.numComputed ).append( " computed in " );
            debug.append( totalStats.computeTime ).append( " millis, " );
            debug.append( numInserted ).append( " inserted in " );
            debug.append( insertTime ).append( " millis " );
            log.debug( debug.toString() );
            }

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
     * Copies the statements from the temporary store into the main store.
     * 
     * @param tmpStore
     * 
     * @return The #of statements inserted into the main store (the count only
     *         reports those statements that were not already in the main
     *         store).
     */
    public int copyStatements( TempTripleStore tmpStore ) {
        
        int numInserted = 0;
        
        IEntryIterator it = tmpStore.getSPOIndex().rangeIterator(null, null);
        while (it.hasNext()) {
            it.next();
            byte[] key = it.getKey();
            if (!getSPOIndex().contains(key)) {
                numInserted++;
                getSPOIndex().insert(key, null);
            }
        }
        
        it = tmpStore.getPOSIndex().rangeIterator(null, null);
        while (it.hasNext()) {
            it.next();
            byte[] key = it.getKey();
            if (!getPOSIndex().contains(key)) {
                getPOSIndex().insert(key, null);
            }
        }
        
        it = tmpStore.getOSPIndex().rangeIterator(null, null);
        while (it.hasNext()) {
            it.next();
            byte[] key = it.getKey();
            if (!getOSPIndex().contains(key)) {
                getOSPIndex().insert(key, null);
            }
        }
        
        return numInserted;
        
    }

    /**
     * Extracts the object ids from a key scan.
     * 
     * @param itr
     *            The key scan iterator.
     * 
     * @return The objects visited by that iterator.
     */
    public SPO[] getStatements(IIndex ndx, KeyOrder keyOrder, byte[] fromKey, byte[] toKey) {

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
    public TripleStore query(Triple query, Rule[] rules) throws IOException {

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

        Properties answerSetProperties = new Properties();
        
        /*
         * @todo support bufferQueue extension for the transient mode or set the
         * default capacity to something larger.  if things get too large
         * then we need to spill over to disk.
         */
        answerSetProperties.setProperty(Options.BUFFER_MODE,
                BufferMode.Transient.toString());
        
        TripleStore answerSet = new TripleStore(answerSetProperties);
        
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

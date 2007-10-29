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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.URI;

import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.inf.Rule.RuleStats;
import com.bigdata.rdf.inf.TestMagicSets.MagicRule;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.Justification;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.store.TempTripleStore;
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
 *                          pred :- pred*.
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
 *                           triple(?v,rdf:type,?x) :-
 *                              triple(?u,rdfs:subClassOf,?x),
 *                              triple(?v,rdf:type,?u). 
 * </pre>
 * 
 * rdfs11 is represented as:
 * 
 * <pre>
 *                           triple(?u,rdfs:subClassOf,?x) :-
 *                              triple(?u,rdfs:subClassOf,?v),
 *                              triple(?v,rdf:subClassOf,?x). 
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Provide incremental closure of data sets are they are loaded.
 * <p>
 * The {@link Rule} class needs to be modified to accept a "new" (statements
 * being loaded or removed from the database) and "db" (either the database
 * (during TM for statement removal) or the union of the new and the database
 * (when adding statements)) parameter and to automatically appy() the rule N
 * times, where N is the #of terms in the tail. In each pass, tail[i] is
 * designated as reading from the "new" data and the other terms in the tail
 * read from the "old" data. This can be done by a variant of apply(). The
 * results are read as a union over the passes, e.g., as an {@link ISPOIterator}.
 * On each pass, the rule should do a rangeCount and execute the individual
 * terms as subqueries where the most selective subquery is run first.
 * <p>
 * 
 * @todo refactor rules to isolate each subquery so that we can choose the
 *       execution order dynamically based on the selectivity of the different
 *       subqueries.
 *       <p>
 *       This will also require that we declare the joins, e.g.,
 *       <code>term[i].s = term[j].p</code> so that we can execute the join
 *       correctly regardless of the order in which we execute the subqueries.
 * 
 * @todo maintain an instance SPO[] in each rule for the bindings. Copy the
 *       static bindings across when the rule is initialized. Define the joins
 *       in terms of variables used in the body[] and then map those variables
 *       into the term[i] index and the {s,p,o} position on the bindings[].
 * 
 * @todo the RuleStats should collect data on a per term-in-the-tail basis.
 * 
 * @todo support subquery reuse.
 * 
 * @todo refactor rules to define apply() that maps over the terms collecting
 *       the union of the results when executing the rule with each term in turn
 *       reading from the "new" vs the "dbView". Note that the "dbView" is
 *       either just the db or a fused view of the db and "new".
 * 
 * @todo tests of the rule mapping logic and tests of the sub-queries. The rule
 *       execution is now going to be refactored into some logic for choosing
 *       the term execution order and unioning the results, so that can get
 *       tested by itself and then the various rules should work correctly if
 *       they are using the correct triple patterns and join variables.
 * 
 * FIXME truth maintenance (check the SAIL also).
 * 
 * FIXME rdfs:Resource by backward chaining on query. This means that we need a
 * query wrapper for the store or for the inference engine.
 * 
 * FIXME owl:sameAs by backward chaining on query.
 * 
 * @todo We don’t do owl:equivalentClass and owl:equivalentProperty currently.
 *       You can simulate those by doing a bi-directional subClassOf or
 *       subPropertyOf, which has always sufficed for our purposes. If you were
 *       going to write rules for those two things this is how you would do it:
 * 
 * <pre>
 *   equivalentClass:
 *                              
 *   add an axiom to the KB: equivalentClass subPropertyOf subClassOf
 *  
 *   add an entailment rule: xxx equivalentClass yyy -&gt; yyy equivalentClass xxx
 * </pre>
 * 
 * It would be analogous for equivalentProperty.
 * 
 * @todo Option to turn off range/domain processing.
 * 
 * @todo Improve write efficiency for the proofs - they are slowing things way
 *       down.
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
 * @todo if we are not storing rules or magic facts in the main statement
 *       indices then get rid of the leading byte used in all keys for the
 *       statement indices.
 */
public class InferenceEngine extends RDFSHelper {

    final public Logger log = Logger.getLogger(InferenceEngine.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final public boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final public boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * Value used for a "NULL" term identifier.
     */
    public static final long NULL = ITripleStore.NULL;

    /**
     * The capacity of the {@link SPOBuffer}.
     * <p>
     * Results on a 45k triple data set:
     * <pre>
     * 1k    - Computed closure in 4469ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=17966
     * 10k   - Computed closure in 3250ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=24704
     * 50k   - Computed closure in 3187ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=25193
     * 100k  - Computed closure in 3359ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=23903
     * 1000k - Computed closure in 3954ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=20306
     * </pre>
     * 
     * Note that the actual performance will depend on the sustained behavior
     * of the JVM, including tuning, and the characteristics of the specific
     * ontology, especially how it influences the #of entailments generated
     * by each rule.  A larger value will be better for sustained closure
     * operations.
     */
    final int BUFFER_SIZE = 200 * Bytes.kilobyte32;
    
    /**
     * Note: making statements distinct in the {@link SPOBuffer} appears to slow
     * things down slightly:
     * 
     * <pre>
     *  
     *  fast: distinct := false;
     *  Computed closure in 3407ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=23566
     *  
     *  fast: distinct := true;
     *  Computed closure in 3594ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=22340
     *  
     *  full: distinct := false;
     *  Computed closure of 12 rules in 3 rounds and 2015ms yeilding 75449 statements total, 29656 inferences, entailmentsPerSec=14717
     *  
     *  full: distinct := true
     *  Computed closure of 12 rules in 3 rounds and 2188ms yeilding 75449 statements total, 29656 inferences, entailmentsPerSec=13553
     * </pre>
     * 
     * FIXME The numbers above were based on the default
     * {@link Object#hashCode()}, which is clearly wrong for this application.
     * I tried to uncomment {@link SPO#hashCode()}, but it slowed things down
     * by 2x. Research the correct way to compute the hash code of three longs
     * and then try this out again!
     */
    final boolean distinct = false;

    /**
     * True iff the Truth Maintenance strategy requires that we store
     * {@link Justification}s for entailments.
     */
    final boolean justify;
    
    /**
     * True iff the Truth Maintenance strategy requires that we store
     * {@link Justification}s for entailments.
     */
    final public boolean isJustified() {
        
        return justify;
        
    }
    
//    /**
//     * The persistent database (vs the temporary store).
//     */
//    public AbstractTripleStore getDatabase() {
//    
//        return database;
//        
//    }

    /*
     * Rules.
     */
    RuleRdf01 rdf1;
    Rule rdf2; // @todo not defined yet.
    RuleRdfs02  rdfs2;
    RuleRdfs03  rdfs3;
    RuleRdfs04a rdfs4a;
    RuleRdfs04b rdfs4b;
    RuleRdfs05  rdfs5;
    RuleRdfs06 rdfs6;
    RuleRdfs07 rdfs7;
    RuleRdfs08 rdfs8;
    RuleRdfs09 rdfs9;
    RuleRdfs10 rdfs10;
    RuleRdfs11 rdfs11;
    RuleRdfs12 rdfs12;
    RuleRdfs13 rdfs13;

    /**
     * A filter for keeping certain entailments out of the database. It is
     * configured based on how the {@link InferenceEngine} is configured.
     * 
     * @see DoNotAddFilter
     */
    public final DoNotAddFilter doNotAddFilter = new DoNotAddFilter(this);
    
    /**
     * Choice of the forward closure algorithm.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static enum ForwardClosureEnum {
        
        /**
         * Runs {@link InferenceEngine#fastForwardClosure()}.
         */
        Fast(),

        /**
         * Runs {@link InferenceEngine#fullForwardClosure()}.
         */
        Full();
        
    }
    
    /**
     * Options for the {@link InferenceEngine}.
     * 
     * @todo Add options:
     *       <p>
     *       Make entailments for rdfs:domain and rdfs:range optional?
     *       <p>
     *       For the {@link #fullForwardClosure()} we can get by with just
     *       Rdfs5, Rdfs7, Rdfs9, and Rdfs11.  Allow the caller to specify
     *       the rule model?
     *       <p>
     *       The {@link SPOBuffer} capacity.
     *       <p>
     *       The {@link InferenceEngine#readService} capacity?
     *       
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Options {
        
//        public static final String TRUTH_MAINENANCE_STRATEGY = "truthMaintenanceStrategy";
//        
//        public static final String DEFAULT_TRUTH_MAINENANCE_STRATEGY = AllProofs.class.getName();

        /**
         * Boolean option - when true the proofs for entailments will be generated and stored in
         * the database.  This option is required by some truth maintenance strategies.
         * 
         * @todo one proof, all proofs, no proofs?
         */
        public static final String JUSTIFY = "justify"; 
        
        public static final String DEFAULT_JUSTIFY = "true"; 
        
        /**
         * Choice of the forward closure algorithm.
         * 
         * @see ForwardClosureEnum
         */
        public static final String FORWARD_CLOSURE = "forwardClosure";

        public static final String DEFAULT_FORWARD_CLOSURE = ForwardClosureEnum.Fast.toString();
        
        /**
         * When true <code>(?x rdf:type rdfs:Resource)</code> entailments are
         * computed AND stored in the database. When false, rules that produce
         * those entailments are turned off such that they are neither computer
         * NOR stored and the backward chainer will generate the entailments at
         * query time.
         * 
         * @todo implement backward chaining for this and change the default to
         *       [false].
         */
        public static final String FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE = "forwardChainRdfTypeRdfsResource";

        public static final String DEFAULT_FORWARD_RDF_TYPE_RDFS_RESOURCE = "true";

        /**
         * @todo document and implement. if we only support this by backward
         *       chaining then get rid of this option.
         * 
         * @todo option for destructive merging for owl:sameAs (collapes term
         *       identifers together)?
         */
        public static final String FORWARD_CHAIN_OWL_SAMEAS= "forwardChainOwlSameAs";

        public static final String DEFAULT_FORWARD_CHAIN_OWL_SAMEAS = "true";
        
//        public static final String FORWARD_CHAINER = "forwardChainer";
//
//        public static final String BACKWARD_CHAINER = "backwardChainer";
        
    }

//    /**
//     * Forward closure is performed when data are loaded into the store and all
//     * proofs are stored.
//     * 
//     * @todo must also determine which rules are forward chained and which are
//     * backward chained.
//     * 
//     * @todo update javadoc. Statement removal for a statement that is no longer
//     *       explicitly asserted simply examines the proofs for that statement.
//     *       If the statement is no longer proven then it is removed and
//     *       dependent statements are tested to see if they are no longer
//     *       provable. This option is the slowest when statements are inserted
//     *       into the store since a large number of proofs objects are
//     *       generated, but it is the fastest for truth maintenance and can pay
//     *       off when statement removal is a common operation for the kb.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class AllProofs implements ITruthMaintenanceStrategy {
//
//        public void createProof(Rule rule, SPO head, SPO[] tail) {
//            // TODO Auto-generated method stub
//            
//        }
//
//        public void createProof(Rule rule, SPO head, long[] tail) {
//            // TODO Auto-generated method stub
//            
//        }
//
//        public int addStatements(ISPOIterator stmts) {
//            // TODO Auto-generated method stub
//            return 0;
//        }
//        
//        public int removeStatements(Resource s, URI p, Value o) {
//            // TODO Auto-generated method stub
//            return 0;
//        }
//        
//    }
    
    /**
     * Configure {@link InferenceEngine} using default {@link Options}.
     * 
     * @param database
     */
    public InferenceEngine(ITripleStore database) {
    
        this(new Properties(),database);
        
    }
    
    /**
     * @param properties
     *            Configuration {@link Options}.
     * @param database
     *            The database for which this class will compute entailments.
     */
    public InferenceEngine(Properties properties, ITripleStore database) {

        super((AbstractTripleStore) database);

        this.justify = Boolean.parseBoolean(properties.getProperty(
                Options.JUSTIFY, Options.DEFAULT_JUSTIFY));
       
        log.info(Options.JUSTIFY+"="+justify);
        
        this.forwardClosure = ForwardClosureEnum
                .valueOf(properties.getProperty(Options.FORWARD_CLOSURE,
                        Options.DEFAULT_FORWARD_CLOSURE)); 

        log.info(Options.FORWARD_CLOSURE+"="+forwardClosure);

        this.forwardChainRdfTypeRdfsResource = Boolean.parseBoolean(properties
                .getProperty(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,
                        Options.DEFAULT_FORWARD_RDF_TYPE_RDFS_RESOURCE));

        log.info(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE + "="
                + forwardChainRdfTypeRdfsResource);

        setupRules();

//        // writes out the base rule model (does not include the "fast" rules).
//        for(Rule r : getRuleModel() ) {
//
//            System.err.println(r.toString());
//
//        }
        
    }
    
    /**
     * Set based on {@link Options#FORWARD_CLOSURE}. 
     */
    final protected ForwardClosureEnum forwardClosure;
    
    /**
     * Set based on {@link Options#FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE}.
     */
    final protected boolean forwardChainRdfTypeRdfsResource;

    /**
     * Sets up the basic rule model for the inference engine.
     */
    private void setupRules() {

        rdf1 = new RuleRdf01(this);

        // @todo write and initialize rdf2 (?x rdf:type rdf:XMLLiteral).
//        
//        rdf2 = new RuleRdf02(this);

        rdfs2 = new RuleRdfs02(this);

        rdfs3 = new RuleRdfs03(this);

        rdfs4a = new RuleRdfs04a(this);
        
        rdfs4b = new RuleRdfs04b(this);
        
        rdfs5 = new RuleRdfs05(this);

        rdfs6 = new RuleRdfs06(this);

        rdfs7 = new RuleRdfs07(this);

        rdfs8 = new RuleRdfs08(this);

        rdfs9 = new RuleRdfs09(this);

        rdfs10 = new RuleRdfs10(this);

        rdfs11 = new RuleRdfs11(this);

        rdfs12 = new RuleRdfs12(this);

        rdfs13 = new RuleRdfs13(this);
        
    }

    /**
     * Return the rule model to be used by {@link #fullForwardClosure()}.
     * 
     * @todo cache this between runs since it never changes.
     */
    public Rule[] getRuleModel() {

        List<Rule> rules = new LinkedList<Rule>();

        rules.add(rdf1);
        
        /*
         * Note: skipping rdf2: (?u ?a ?l) -> ( _:n rdf:type rdf:XMLLiteral),
         * where ?l is a well-typed XML Literal.
         * 
         * @todo should this be included?
         */

        rules.add(rdfs2);
        
        rules.add(rdfs3);
        
        if(forwardChainRdfTypeRdfsResource) {

            /*
             * Note: skipping rdfs4a (?u ?a ?x) -> (?u rdf:type rdfs:Resource)
             * 
             * Note: skipping rdfs4b (?u ?a ?v) -> (?v rdf:type rdfs:Resource)
             * 
             * @todo make sure that they are back-chained.
             */

            rules.add(rdfs4a);
            
            rules.add(rdfs4b);
            
        }
        
        rules.add(rdfs5);
        
        rules.add(rdfs6);
        
        rules.add(rdfs7);
        
        /*
         * @todo Should we run this rule or backchain?
         * 
         * [MikeP] I would say generate, but you can backchain (I think?) as
         * long as you have the full closure of the type hierarchy, specifically
         * of ? type Class. If you don't have that (i.e. if you are not
         * calculating the domain and range inferences), then you'd need to
         * recursively backchain the tail too. That is why I did not do more
         * backchaining - the backchain for ? type Resource is super easy
         * because it's never longer than 1 move back.
         */
        rules.add(rdfs8);
        
        rules.add(rdfs9);
        
        rules.add(rdfs10);
        
        rules.add(rdfs11);
        
        rules.add(rdfs12);
        
        rules.add(rdfs13);

        /*
         * Note: The datatype entailment rules are being skipped.
         * 
         * @todo make sure that they are back-chained or add them in here.
         */

        /*
         * @todo owl same as if forward closure used for that.
         */
        
        return rules.toArray(new Rule[rules.size()]);

    }
    
    /**
     * Executor service for rule parallelism.
     * 
     * @todo use for parallel execution of map of new vs old+new over the terms
     *       of a rule.
     * 
     * @todo use for parallel execution of sub-queries.
     * 
     * @todo {@link SPOBuffer} must be thread-safe.  {@link Rule} bindings must be per-thread.
     * 
     * @todo Note that rules that write entailments on the database statement
     *       MUST coordinate to avoid concurrent modification during traversal
     *       of the statement indices. The chunked iterators go a long way to
     *       addressing this.
     */
    final ExecutorService readService = Executors
            .newCachedThreadPool(DaemonThreadFactory.defaultThreadFactory());

    /**
     * Compute the forward closure using the algorithm selected by
     * {@link Options#FORWARD_CLOSURE}.
     * 
     * @return Statistics about the operation.
     */
    public ClosureStats computeClosure() {

        /*
         * Reset the stats for each rule.
         * 
         * Note: If there are pre-initialized rules for the fast forward closure
         * that are not part of the full closure then they need to be returned
         * by getRuleModel() in order to have their statistics reset before the
         * closure is computed. Right now, all of those rules are created
         * dynamically inside of fastForwardClosure(). However, steps 11 and 13
         * could be pre-initialized since they do not depend on dynamic data.
         */
        for(Rule rule : getRuleModel() ) {
            
            rule.stats.reset();
            
        }
        
        switch(forwardClosure) {

        case Fast:
            return fastForwardClosure();
        
        case Full:
            return fullForwardClosure();
        
        default: throw new UnsupportedOperationException();
        
        }
        
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
     */
    protected ClosureStats fullForwardClosure() {

        final long begin = System.currentTimeMillis();
       
        final int nbefore = database.getStatementCount();
        
        final ClosureStats closureStats = new ClosureStats();
        
        // add RDF(S) axioms to the database.
        addRdfsAxioms(database);

        /*
         * This is a buffer that is used to hold entailments so that we can
         * insert them into the indices of the <i>tmpStore</i> using ordered
         * insert operations (much faster than random inserts). The buffer is
         * reused by each rule. The rule assumes that the buffer is empty and
         * just keeps a local counter of the #of entailments that it has
         * inserted into the buffer. When the buffer overflows, those
         * entailments are transfered enmass into the tmp store. The buffer is
         * always flushed after each rule and therefore will have been flushed
         * when this method returns.
         */ 
        final SPOBuffer buffer = new SPOBuffer(database, doNotAddFilter,
                BUFFER_SIZE, distinct, isJustified());

        // do the full forward closure of the database.
        System.err.println(fixedPoint(closureStats,getRuleModel(), buffer).toString());
       
        final int nafter = database.getStatementCount();
        
        final long elapsed = System.currentTimeMillis() - begin;
        
        closureStats.elapsed = elapsed;
        closureStats.numComputed = nafter - nbefore;
        
        return closureStats;        
        
    }

    /**
     * Fast forward closure of the store based on <a
     * href="http://www.cs.iastate.edu/~tukw/waim05.pdf">"An approach to RDF(S)
     * Query, Manipulation and Inference on Databases" by Lu, Yu, Tu, Lin, and
     * Zhang</a>.
     * 
     * @todo When modifying to support incremental TM note that some rules are
     *       "special". In particular {@link AbstractRuleFastClosure_3_5_6_7_9}
     *       accept a set of term identifiers that are specially computed. We
     *       need to pass those rules the term identifiers as computed over the
     *       union of the new and old statements.
     */
    protected ClosureStats fastForwardClosure() {

        /*
         * Note: The steps below are numbered with regard to the paper cited in
         * the javadoc above.
         * 
         * Most steps presume that the computed entailments have been added to
         * the database (vs the temp store).
         */

        final ClosureStats closureStats = new ClosureStats();
        
        // add the basic rule model.
        closureStats.addAll(Arrays.asList(getRuleModel()));
        
        final int firstStatementCount = database.getStatementCount();

        final long begin = System.currentTimeMillis();

        log.debug("Closing kb with " + firstStatementCount
                + " statements");
        
        /*
         * Entailment buffer.
         */
        final SPOBuffer buffer = new SPOBuffer(database, doNotAddFilter,
                BUFFER_SIZE, distinct, isJustified());

        // 1. add RDF(S) axioms to the database.
        addRdfsAxioms(database); // add to the database.
        
        // 2. Compute P (the set of possible sub properties).
        final Set<Long> P = getSubProperties(database);

        // 3. (?x, P, ?y) -> (?x, rdfs:subPropertyOf, ?y)
        {
            Rule r = new RuleFastClosure3(this, P);
            closureStats.add(r);
            r.apply(justify, buffer);
            System.err.println("step3: " + r.stats);
            buffer.flush();
        }

        // 4. RuleRdfs05 until fix point (rdfs:subPropertyOf closure).
        System.err.println("rdfs5: "
                + fixedPoint(closureStats,new Rule[] { rdfs5 },buffer));

        // 4a. Obtain: D,R,C,T.
        final Set<Long> D = getSubPropertiesOf(database, rdfsDomain.id);
        final Set<Long> R = getSubPropertiesOf(database, rdfsRange.id);
        final Set<Long> C = getSubPropertiesOf(database, rdfsSubClassOf.id);
        final Set<Long> T = getSubPropertiesOf(database, rdfType.id);

        // 5. (?x, D, ?y ) -> (?x, rdfs:domain, ?y)
        {
            Rule r = new RuleFastClosure5(this, D);
            closureStats.add(r);
            r.apply(justify, buffer);
            System.err.println("step5: " + r.stats);
            // Note: deferred buffer.flush() since the next step has no
            // dependency.
        }

        // 6. (?x, R, ?y ) -> (?x, rdfs:range, ?y)
        {
            Rule r = new RuleFastClosure6(this, R);
            closureStats.add(r);
            r.apply(justify, buffer);
            System.err.println("step6: " + r.stats);
            // Note: deferred buffer.flush() since the next step has no
            // dependency.
        }

        // 7. (?x, C, ?y ) -> (?x, rdfs:subClassOf, ?y)
        {
            Rule r = new RuleFastClosure7(this, C);
            closureStats.add(r);
            r.apply(justify, buffer);
            System.err.println("step7: " + r.stats);
            // Note: flush buffer before running rdfs11.
            buffer.flush();
        }

        // 8. RuleRdfs11 until fix point (rdfs:subClassOf closure).
        System.err.println("rdfs11: "
                + fixedPoint(closureStats,new Rule[] { rdfs11 }, buffer));

        // 9. (?x, T, ?y ) -> (?x, rdf:type, ?y)
        {
            Rule r = new RuleFastClosure9(this, T);
            closureStats.add(r);
            r.apply(justify, buffer);
            System.err.println("step9: " + r.stats);
            buffer.flush();
        }

        // 10. RuleRdfs02
        System.err.println("rdfs2: "+rdfs2.apply(justify,buffer));
        buffer.flush();
        
        /*
         * 11. special rule w/ 3-part antecedent.
         * 
         * (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:domain, ?b) ->
         * (?x, rdf:type, ?b).
         */
        {
            Rule r = new RuleFastClosure11(this);
            closureStats.add(r);
            r.apply(justify, buffer);
            System.err.println("step11: " + r.stats);
            buffer.flush();
        }
        
        // 12. RuleRdfs03
        System.err.println("rdfs3: "+rdfs3.apply(justify, buffer));
        buffer.flush();
        
        /* 13. special rule w/ 3-part antecedent.
         * 
         * (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:range, ?b ) ->
         * (?x, rdf:type, ?b )
         */
        {
            Rule r = new RuleFastClosure13(this);
            closureStats.add(r);
            r.apply(justify, buffer);
            System.err.println("step13: " + r.stats);
            buffer.flush();
        }
        
        if(forwardChainRdfTypeRdfsResource) {
            
            /*
             * 14-15. These steps correspond to rdfs4a and rdfs4b and generate
             * (?x rdf:type rdfs:Resource) entailments. We execute these steps
             * iff we are storing those entailments.
             */

            // 14-15. RuleRdf04
            System.err.println("rdfs4a: "+rdfs4a.apply(justify, buffer));
            System.err.println("rdfs4b: "+rdfs4b.apply(justify, buffer));
            buffer.flush();

        }

        // 16. RuleRdf01
        System.err.println("rdf1: "+rdf1.apply(justify, buffer));
        buffer.flush();
        
        // 17. RuleRdfs09
        System.err.println("rdfs9: "+rdfs9.apply(justify, buffer));
        buffer.flush();
        
        // 18. RuleRdfs10
        System.err.println("rdfs10: "+rdfs10.apply(justify, buffer));
        buffer.flush();
        
        // 19. RuleRdfs08.
        System.err.println("rdfs8: "+rdfs8.apply(justify, buffer));
        buffer.flush();

        // 20. RuleRdfs13.
        System.err.println("rdfs13: "+rdfs13.apply(justify, buffer));
        buffer.flush();
        
        // 21. RuleRdfs06.
        System.err.println("rdfs6: "+rdfs6.apply(justify, buffer));
        buffer.flush();
        
        // 22. RuleRdfs07.
        System.err.println("rdfs7: "+rdfs7.apply(justify, buffer));
        buffer.flush();
        
        /*
         * Done.
         */
        
        final long elapsed = System.currentTimeMillis() - begin;

        final int lastStatementCount = database.getStatementCount();

        final int inferenceCount = lastStatementCount - firstStatementCount;
        
        if(INFO) {

            log.info("\nComputed closure in "
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

        closureStats.elapsed = elapsed;
        closureStats.numComputed = inferenceCount;
        
        return closureStats;
        
    }

    /**
     * Computes the fixed point for the {@link #database} using a specified rule
     * set.
     * <p>
     * Note: The buffer will have been flushed when this method returns.
     * 
     * @param rules
     *            The rules to be executed.
     * 
     * @param buffer
     *            This is a buffer that is used to hold entailments so that we
     *            can insert them into the indices of the backing database using
     *            ordered insert operations (much faster than random inserts).
     * 
     * @return Some statistics about the fixed point computation.
     */
    public ClosureStats fixedPoint(ClosureStats closureStats, Rule[] rules,
            SPOBuffer buffer) {
        
//        final long[] timePerRule = new long[rules.length];
//        
//        final int[] entailmentsPerRule = new int[rules.length];
        
        final int nrules = rules.length;

        final int firstStatementCount = database.getStatementCount();

        final long begin = System.currentTimeMillis();

        log.debug("Closing kb with " + firstStatementCount
                + " statements");

        int round = 0;

        while (true) {

            final int numEntailmentsBefore = buffer.getBackingStore().getStatementCount();
            
            for (int i = 0; i < nrules; i++) {

                Rule rule = rules[i];

//                RuleStats ruleStats = rule.stats;
                
//                if(round==0) ruleStats.reset();

//                int nbefore = ruleStats.numComputed;
                
                RuleStats ruleStats = rule.apply( justify, buffer );
                
                ruleStats.nrounds ++;
                
//                int nnew = ruleStats.numComputed - nbefore;
//
//                // #of statements examined by the rule.
//                int nstmts = ruleStats.getStatementCount();
//                
//                long elapsed = ruleStats.elapsed;
                
//                timePerRule[i] += elapsed;
                
//                entailmentsPerRule[i] = ruleStats.numComputed; // Note: already a running sum.
                
                if (DEBUG || true) {

                    log.debug("round# " + round + ":" + ruleStats);
                    
                }
                
                closureStats.numComputed += ruleStats.numComputed;
                
                closureStats.elapsed += ruleStats.elapsed;
                
            }

            // Flush the statements in the buffer 
            buffer.flush();

            final int numEntailmentsAfter = buffer.getBackingStore().getStatementCount();
            
            if ( numEntailmentsBefore == numEntailmentsAfter ) {
                
                // This is the fixed point.
                break;
                
            }
            
//            /*
//             * Transfer the entailments into the primary store so that derived
//             * entailments may be computed.
//             */
//            final long insertStart = System.currentTimeMillis();

//            final int numInserted = numEntailmentsAfter - numEntailmentsBefore;
            
//            final int numInserted = copyStatements(tmpStore,database);

//            final long insertTime = System.currentTimeMillis() - insertStart;

//            debug.append( numInserted ).append( " inserted in " );
//            debug.append( insertTime ).append( " millis " );

            if(INFO) {

                log.info("round #"+round+"\n"+closureStats.toString());
                
            }
            
//            if (DEBUG) {
//                StringBuilder sb = new StringBuilder();
//                sb.append( "round #" ).append( round ).append( ": " );
//                sb.append( closureStats.numComputed ).append( " computed in " );
//                sb.append( closureStats.elapsed ).append( " millis, " );
//                log.debug( sb.toString() );
//            }

            round++;
            
        }

        final long elapsed = System.currentTimeMillis() - begin;

        final int lastStatementCount = database.getStatementCount();

        if (INFO) {
        
            final int inferenceCount = lastStatementCount - firstStatementCount;
            
            log.info("\nComputed closure of "+rules.length+" rules in "
                            + (round+1) + " rounds and "
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

        return closureStats;

    }

    /**
     * Computes the set of possible sub properties of rdfs:subPropertyOf (<code>P</code>).
     * This is used by steps 2-4 in {@link #fastForwardClosure()}.
     * 
     * @param database
     *            The database to be queried.
     * 
     * @return A set containing the term identifiers for the members of P.
     */
    public Set<Long> getSubProperties(AbstractTripleStore database) {

        final Set<Long> P = new HashSet<Long>();
        
        P.add(rdfsSubPropertyOf.id);

        /*
         * query := (?x, P, P), adding new members to P until P reaches fix
         * point.
         */
        {

            int nbefore;
            int nafter = 0;
            int nrounds = 0;

            Set<Long> tmp = new HashSet<Long>();

            do {

                nbefore = P.size();

                tmp.clear();

                /*
                 * query := (?x, p, ?y ) for each p in P, filter ?y element of
                 * P.
                 */

                for (Long p : P) {

                    ISPOIterator itr = database.getAccessPath(NULL, p, NULL).iterator();

                    while(itr.hasNext()) {
                        
                        SPO[] stmts = itr.nextChunk();
                            
                        for(SPO stmt : stmts) {

                            if (P.contains(stmt.o)) {

                                tmp.add(stmt.s);

                            }

                        }

                    }
                    
                }

                P.addAll(tmp);

                nafter = P.size();

                nrounds++;

            } while (nafter > nbefore);

        }
        
        if(DEBUG){
            
            Set<String> terms = new HashSet<String>();
            
            for( Long id : P ) {
                
                terms.add(database.getTerm(id).term);
                
            }
            
            log.debug("P: "+terms);
        
        }
        
        return P;

    }
    
    /**
     * Query the <i>database</i> for the sub properties of a given property.
     * <p>
     * Pre-condition: The closure of <code>rdfs:subPropertyOf</code> has been
     * asserted on the database.
     * 
     * @param database
     *            The database to be queried.
     * @param p
     *            The term identifier for the property whose sub-properties will
     *            be obtain.
     * 
     * @return A set containing the term identifiers for the sub properties of
     *         <i>p</i>. 
     */
    public Set<Long> getSubPropertiesOf(AbstractTripleStore database, final long p) {

        if(DEBUG) {
            
            log.debug("p="+database.getTerm(p).term);
            
        }
        
        final Set<Long> tmp = new HashSet<Long>();

        /*
         * query := (?x, rdfs:subPropertyOf, p).
         * 
         * Distinct ?x are gathered in [tmp].
         * 
         * Note: This query is two-bound on the POS index.
         */

        ISPOIterator itr = database.getAccessPath(NULL/*x*/, rdfsSubPropertyOf.id, p).iterator();

        while(itr.hasNext()) {
            
            SPO[] stmts = itr.nextChunk();
            
            for( SPO spo : stmts ) {
                
                boolean added = tmp.add(spo.s);
                
                if(DEBUG) {
                    
                    log.debug(spo.toString(database) + ", added subject="+added);
                    
                }
                
            }

        }
        
        if(DEBUG){
        
            Set<String> terms = new HashSet<String>();
            
            for( Long id : tmp ) {
                
                terms.add(database.getTerm(id).term);
                
            }
            
            log.debug("sub properties: "+terms);
        
        }
        
        return tmp;

    }

    /**
     * Add the axiomatic RDF(S) triples to the store.
     * <p>
     * Note: The termIds are defined with respect to the backing triple store
     * since the axioms will be copied into the store when the closure is
     * complete.
     * 
     * @param database
     *            The store to which the axioms will be added.
     */
    public void addRdfsAxioms(ITripleStore database) {
        
        Axioms axiomModel = RdfsAxioms.INSTANCE;

        StatementBuffer buffer = new StatementBuffer(database, axiomModel
                .getAxioms().size(), true/*distinct*/);
        
        for (Iterator<Axioms.Triple> itr = axiomModel.getAxioms().iterator(); itr
                .hasNext();) {

            Axioms.Triple triple = itr.next();
            
            URI s = triple.getS();
            
            URI p = triple.getP();
            
            URI o = triple.getO();
            
            buffer.add( s, p, o, StatementEnum.Axiom );
            
        }

        // write on the database.
        buffer.flush();
        
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

        if (query.isConstant())
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
        
        ITripleStore answerSet = new TempTripleStore(new Properties());
        
        int lastStatementCount = database.getStatementCount();

        final long begin = System.currentTimeMillis();

        System.err.println("Running query: "+query);

        int nadded = 0;

        while (true) {

            for (int i = 0; i < nrules; i++) {

                Rule rule = rules[i];

                // nadded += rule.apply();
                // rule.apply();

            }

            int statementCount = database.getStatementCount();

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

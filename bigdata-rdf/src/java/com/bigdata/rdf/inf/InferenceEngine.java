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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.Justification;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AccessPathFusedView;
import com.bigdata.rdf.store.IAccessPath;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.ITripleStore;
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
 *                              pred :- pred*.
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
 *                               triple(?v,rdf:type,?x) :-
 *                                  triple(?u,rdfs:subClassOf,?x),
 *                                  triple(?v,rdf:type,?u). 
 * </pre>
 * 
 * rdfs11 is represented as:
 * 
 * <pre>
 *                               triple(?u,rdfs:subClassOf,?x) :-
 *                                  triple(?u,rdfs:subClassOf,?v),
 *                                  triple(?v,rdf:subClassOf,?x). 
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
 * FIXME truth maintenance (check the SAIL also).
 * 
 * FIXME owl:sameAs by backward chaining on query.
 * 
 * <pre>
 *    Here are the axioms:
 *    
 *    owl:equivP rdfs:subPropertyOf rdfs:subPropertyOf
 *    
 *    owl:equivC rdfs:subPropertyOf rdfs:subClassOf
 *    
 *    
 *    Here are the rules:
 *    
 *    eqp: aaa owl:equivP bbb -&gt; bbb owl:equivP aaa
 *    
 *    eqc: xxx owl:equivC yyy -&gt; yyy owl:equivC xxx
 *    
 *    same1: xxx owl:sameAs yyy -&gt; yyy owl:sameAs xxx
 *    
 *    same2: xxx owl:sameAs yyy + xxx aaa zzz -&gt; yyy aaa zzz
 *    
 *    same3: xxx owl:sameas yyy + zzz aaa zzz -&gt; zzz aaa yyy
 *    
 *    Filter out xxx owl:sameAs xxx for same2, same3.
 *    
 *    Best guess for &quot;fast&quot; integration:
 *  
 *  eqp: run to completion before step 2
 *  
 *  eqc: run to completion before step 7
 *  
 *  fix point the sameAs rules together at the very end.
 *  
 * </pre>
 * 
 * @todo Improve write efficiency for the proofs - they are slowing things way
 *       down. Note that using magic sets or a backward chainer will let us
 *       avoid writing proofs altogether since we can prove whether or not a
 *       statement is still entailed without recourse to reading proofs chains.
 * 
 * @todo provide option for "owl:sameAs" semantics using destructive merging
 *       (the terms are assigned the same term identifier, one of them is
 *       treated as a canonical, and there is no way to retract the sameAs
 *       assertion).
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
    public static final long NULL = IRawTripleStore.NULL;

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
     *       The {@link SPOBuffer} capacity.
     *       <p>
     *       The {@link InferenceEngine#readService} capacity?
     *       
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Options {

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
         * query time. Default is <code>false</code>.
         */
        public static final String FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE = "forwardChainRdfTypeRdfsResource";

        public static final String DEFAULT_FORWARD_RDF_TYPE_RDFS_RESOURCE = "false";

        /**
         * @todo document and implement. if we only support this by backward
         *       chaining then get rid of this option.
         * 
         * @todo option for destructive merging for owl:sameAs (collapes term
         *       identifers together)?
         */
        public static final String FORWARD_CHAIN_OWL_SAMEAS= "forwardChainOwlSameAs";

        public static final String DEFAULT_FORWARD_CHAIN_OWL_SAMEAS = "true";
        
    }

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
     * Return true iff the {@link InferenceEngine} is configured to forward
     * chain and store entailments of the form (x rdf:type rdfs:Resource). When
     * this returns false, those entailments are not computed and are not
     * stored.
     * 
     * @see Options#FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE
     */
    public boolean getForwardChainRdfTypeRdfsResource() {
        
        return forwardChainRdfTypeRdfsResource;
        
    }
    
    /**
     * Sets up the basic rule model for the inference engine.
     */
    private void setupRules() {

        rdf1 = new RuleRdf01(this);

        // @todo rdf2 (?x rdf:type rdf:XMLLiteral).
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
     * Compute the forward closure of the database using the algorithm selected
     * by {@link Options#FORWARD_CLOSURE}.
     * 
     * @return Statistics about the operation.
     * 
     * @deprecated by {@link #computeClosure(AbstractTripleStore)} which
     * supports incremental data loading.
     */
    public ClosureStats computeClosure() {

        return computeClosure(null);
        
    }

    /**
     * Compute the forward closure of a focusStore against the database using
     * the algorithm selected by {@link Options#FORWARD_CLOSURE}.
     * 
     * @param focusStore
     *            The data set that will be closed against the database
     *            (optional). When <code>null</code> the store will be closed
     *            against itself.
     * 
     * @return Statistics about the operation.
     * 
     * FIXME If the focusStore is given then the entailments will be asserted
     * against the focusStore. Either this method or the caller MUST copy the
     * focusStore onto the database using
     * {@link AbstractTripleStore#copyStatements(AbstractTripleStore, com.bigdata.rdf.spo.ISPOFilter)}
     */
    public ClosureStats computeClosure(AbstractTripleStore focusStore) {

        switch(forwardClosure) {

        case Fast:
            return fastForwardClosure(focusStore);
        
        case Full:
            return fullForwardClosure(focusStore);
        
        default: throw new UnsupportedOperationException();
        
        }
        
    }
    
    /**
     * Compute the complete forward closure of the store using a set-at-a-time
     * inference strategy.
     * <p>
     * The general approach is a series of rounds in which each rule is applied
     * to all data in turn. Entailments computed in each round are fed back into
     * the source against which the rules can match their preconditions, so
     * derived entailments may be computed in a succession of rounds. The
     * process halts when no new entailments are computed in a given round.
     * 
     * @param focusStore
     *            When non-<code>null</code> , the focusStore will be closed
     *            against the database with the entailments written into the
     *            database. When <code>null</code>, the entire database will
     *            be closed.
     */
    protected ClosureStats fullForwardClosure(AbstractTripleStore focusStore) {

        final long begin = System.currentTimeMillis();
       
        final ClosureStats closureStats = new ClosureStats();

        // The store where the entailments are being built up.
        final AbstractTripleStore closureStore = (focusStore != null ? focusStore
                : database);

        final int nbefore = closureStore.getStatementCount();
        
        /*
         * add RDF(S) axioms to the database.
         * 
         * Note: If you insert Statements using the Sesame object model into the
         * focusStore then it will have different term identifiers for those
         * assertions that the ones that are used by the database, which is
         * very, very bad.
         */
        RdfsAxioms.INSTANCE.addAxioms(database);

        // entailment buffer writes on the closureStore.
        final SPOBuffer buffer = new SPOBuffer(closureStore, doNotAddFilter,
                BUFFER_SIZE, distinct, isJustified());

        // compute the full forward closure.
        System.err.println(Rule.fixedPoint(closureStats, getRuleModel(),
                justify, focusStore, database, buffer).toString());
       
        final int nafter = closureStore.getStatementCount();
        
        final long elapsed = System.currentTimeMillis() - begin;
        
        closureStats.elapsed = elapsed;
        closureStats.nentailments = nafter - nbefore;
        
        return closureStats;        
        
    }

    /**
     * Fast forward closure of the store based on <a
     * href="http://www.cs.iastate.edu/~tukw/waim05.pdf">"An approach to RDF(S)
     * Query, Manipulation and Inference on Databases" by Lu, Yu, Tu, Lin, and
     * Zhang</a>.
     * 
     * @param focusStore
     *            When non-<code>null</code> , the focusStore will be closed
     *            against the database with the entailments written into the
     *            database. When <code>null</code>, the entire database will
     *            be closed.
     */
    protected ClosureStats fastForwardClosure(AbstractTripleStore focusStore) {

        /*
         * Note: The steps below are numbered with regard to the paper cited in
         * the javadoc above.
         * 
         * Most steps presume that the computed entailments have been added to
         * the database (vs the temp store).
         */

        final ClosureStats closureStats = new ClosureStats();

        // The store where the entailments are being built up.
        final AbstractTripleStore closureStore = (focusStore != null ? focusStore
                : database);
        
        // Entailment buffer writes on the closureStore.
        final SPOBuffer buffer = new SPOBuffer(closureStore, doNotAddFilter,
                BUFFER_SIZE, distinct, isJustified());
        
        final int firstStatementCount = closureStore.getStatementCount();

        final long begin = System.currentTimeMillis();

        log.debug("Closing kb with " + firstStatementCount
                + " statements");

        /*
         * 1. add RDF(S) axioms to the database.
         * 
         * Note: If you insert Statements using the Sesame object model into the
         * focusStore then it will have different term identifiers for those
         * assertions that the ones that are used by the database, which is
         * very, very bad.
         */ 
        RdfsAxioms.INSTANCE.addAxioms(database);
        
        // 2. Compute P (the set of possible sub properties).
        final Set<Long> P = getSubProperties(focusStore,database);

        // 3. (?x, P, ?y) -> (?x, rdfs:subPropertyOf, ?y)
        {
            Rule r = new RuleFastClosure3(this, P);
            RuleStats stats = r.apply(justify,focusStore,database,buffer);
            closureStats.add(stats);
            System.err.println("step3: " + stats);
            buffer.flush();
        }

        // 4. RuleRdfs05 until fix point (rdfs:subPropertyOf closure).
        System.err.println("rdfs5: "
                + Rule.fixedPoint(closureStats, new Rule[] { rdfs5 }, justify,
                        focusStore, database, buffer));

        // 4a. Obtain: D,R,C,T.
        final Set<Long> D = getSubPropertiesOf(focusStore,database, rdfsDomain.id);
        final Set<Long> R = getSubPropertiesOf(focusStore,database, rdfsRange.id);
        final Set<Long> C = getSubPropertiesOf(focusStore,database, rdfsSubClassOf.id);
        final Set<Long> T = getSubPropertiesOf(focusStore,database, rdfType.id);

        // 5. (?x, D, ?y ) -> (?x, rdfs:domain, ?y)
        {
            Rule r = new RuleFastClosure5(this, D);
            RuleStats stats = r.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            System.err.println("step5: " + stats);
            // Note: deferred buffer.flush() since the next step has no
            // dependency.
        }

        // 6. (?x, R, ?y ) -> (?x, rdfs:range, ?y)
        {
            Rule r = new RuleFastClosure6(this, R);
            RuleStats stats = r.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            System.err.println("step6: " + stats);
            // Note: deferred buffer.flush() since the next step has no
            // dependency.
        }

        // 7. (?x, C, ?y ) -> (?x, rdfs:subClassOf, ?y)
        {
            Rule r = new RuleFastClosure7(this, C);
            RuleStats stats = r.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            System.err.println("step7: " + stats);
            // Note: flush buffer before running rdfs11.
            buffer.flush();
        }

        // 8. RuleRdfs11 until fix point (rdfs:subClassOf closure).
        System.err.println("rdfs11: "
                + Rule.fixedPoint(closureStats, new Rule[] { rdfs11 }, justify,
                        focusStore, database, buffer));

        // 9. (?x, T, ?y ) -> (?x, rdf:type, ?y)
        {
            Rule r = new RuleFastClosure9(this, T);
            RuleStats stats = r.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            System.err.println("step9: " + stats);
            buffer.flush();
        }

        // 10. RuleRdfs02
        {
            RuleStats stats = rdfs2.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            System.err.println("rdfs2: " + stats);
            buffer.flush();
        }
        
        /*
         * 11. special rule w/ 3-part antecedent.
         * 
         * (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:domain, ?b) ->
         * (?x, rdf:type, ?b).
         */
        {
            Rule r = new RuleFastClosure11(this);
            RuleStats stats = r.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            System.err.println("step11: " + stats);
            buffer.flush();
        }
        
        // 12. RuleRdfs03
        {
            RuleStats stats = rdfs3.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            System.err.println("rdfs3: "+stats);
            buffer.flush();
        }
        
        /* 13. special rule w/ 3-part antecedent.
         * 
         * (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:range, ?b ) ->
         * (?x, rdf:type, ?b )
         */
        {
            Rule r = new RuleFastClosure13(this);
            RuleStats stats = r.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            System.err.println("step13: " + stats);
            buffer.flush();
        }
        
        if(forwardChainRdfTypeRdfsResource) {
            
            /*
             * 14-15. These steps correspond to rdfs4a and rdfs4b and generate
             * (?x rdf:type rdfs:Resource) entailments. We execute these steps
             * iff we are storing those entailments.
             */

            // 14-15. RuleRdf04
            {
                RuleStats stats = rdfs4a.apply(justify, focusStore, database, buffer);
                closureStats.add(stats);
                System.err.println("rdfs4a: "+stats);
            }
            {
                RuleStats stats = rdfs4b.apply(justify, focusStore, database, buffer);
                closureStats.add(stats);
                System.err.println("rdfs4b: "+stats);
            }
            buffer.flush();

        }

        // 16. RuleRdf01
        {
            RuleStats stats = rdf1.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            System.err.println("rdf1: "+stats);
            buffer.flush();
        }
        
        // 17. RuleRdfs09
        {
            RuleStats stats = rdfs9.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            System.err.println("rdfs9: "+stats);
            buffer.flush();
        }
        
        // 18. RuleRdfs10
        {
            RuleStats stats = rdfs10.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            System.err.println("rdfs10: "+stats);
            buffer.flush();
        }
        
        // 19. RuleRdfs08.
        {
            RuleStats stats = rdfs8.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            System.err.println("rdfs8: "+stats);
            buffer.flush();
        }

        // 20. RuleRdfs13.
        {
            RuleStats stats = rdfs13.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            System.err.println("rdfs13: " + stats);
            buffer.flush();
        }

        // 21. RuleRdfs06.
        {
            RuleStats stats = rdfs6.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            System.err.println("rdfs6: " + stats);
            buffer.flush();
        }

        // 22. RuleRdfs07.
        {
            RuleStats stats = rdfs7.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            System.err.println("rdfs7: " + stats);
            buffer.flush();
        }
        
        /*
         * Done.
         */
        
        final long elapsed = System.currentTimeMillis() - begin;

        final int lastStatementCount = closureStore.getStatementCount();

        final int inferenceCount = lastStatementCount - firstStatementCount;
        
        if(INFO) {
                        
            log.info("\n"+closureStats.toString());

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
        closureStats.nentailments = inferenceCount;
        
        return closureStats;
        
    }

    /**
     * Computes the set of possible sub properties of rdfs:subPropertyOf (<code>P</code>).
     * This is used by steps 2-4 in {@link #fastForwardClosure()}.
     * 
     * @param focusStore
     * @param database
     * 
     * @return A set containing the term identifiers for the members of P.
     */
    public Set<Long> getSubProperties(AbstractTripleStore focusStore, AbstractTripleStore database) {

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

                    final IAccessPath accessPath = (focusStore == null //
                            ? database.getAccessPath(NULL, p, NULL)//
                            : new AccessPathFusedView(focusStore.getAccessPath(
                                    NULL, p, NULL), //
                                    database.getAccessPath(NULL, p, NULL)//
                            ));

                    ISPOIterator itr = accessPath.iterator();

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
                
                terms.add(database.toString(id));
                
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
     * @param focusStore
     * @param database
     * @param p
     *            The term identifier for the property whose sub-properties will
     *            be obtain.
     * 
     * @return A set containing the term identifiers for the sub properties of
     *         <i>p</i>.
     */
    public Set<Long> getSubPropertiesOf(AbstractTripleStore focusStore,
            AbstractTripleStore database, final long p) {

        final IAccessPath accessPath = //
        (focusStore == null //
        ? database.getAccessPath(NULL/* x */, rdfsSubPropertyOf.id, p)//
                : new AccessPathFusedView(//
                        focusStore.getAccessPath(NULL/* x */,
                                rdfsSubPropertyOf.id, p), //
                        database.getAccessPath(NULL/* x */,
                                rdfsSubPropertyOf.id, p)//
                ));

        if(DEBUG) {
            
            log.debug("p="+database.toString(p));
            
        }
        
        final Set<Long> tmp = new HashSet<Long>();

        /*
         * query := (?x, rdfs:subPropertyOf, p).
         * 
         * Distinct ?x are gathered in [tmp].
         * 
         * Note: This query is two-bound on the POS index.
         */

        ISPOIterator itr = accessPath.iterator();

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
                
                terms.add(database.toString(id));
                
            }
            
            log.debug("sub properties: "+terms);
        
        }
        
        return tmp;

    }
    
}

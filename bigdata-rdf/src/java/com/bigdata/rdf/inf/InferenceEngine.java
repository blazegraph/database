/**

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
package com.bigdata.rdf.inf;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.inf.Rule.IConstraint;
import com.bigdata.rdf.inf.Rule.Var;
import com.bigdata.rdf.spo.ChunkedSPOIterator;
import com.bigdata.rdf.spo.ISPOFilter;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBlockingBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.IAccessPath;
import com.bigdata.rdf.store.IRawTripleStore;

import cutthecrap.utils.striterators.EmptyIterator;
import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

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
 *                                    pred :- pred*.
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
 *                                     triple(?v,rdf:type,?x) :-
 *                                        triple(?u,rdfs:subClassOf,?x),
 *                                        triple(?v,rdf:type,?u). 
 * </pre>
 * 
 * rdfs11 is represented as:
 * 
 * <pre>
 *    triple(?u,rdfs:subClassOf,?x) :-
 *      triple(?u,rdfs:subClassOf,?v),
 *      triple(?v,rdf:subClassOf,?x). 
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo update the javadoc on this class.
 * 
 * FIXME test backchain iterator at scale.
 * 
 * @todo verify the code in places where it tests against a specific term
 *       identifer defined in {@link RDFSHelper} and not against the closure of
 *       the subclasses of or the subproperties of that term.
 * 
 * @todo provide declarative rule models for forward chaining so that the rules
 *       may be extended without having to edit the code.
 * 
 * @todo consider support for owl:inverseFunctionalProperty. Are there any other
 *       low hanging fruit there?
 * 
 * @todo Improve write efficiency for the proofs - they are slowing things way
 *       down. Note that using magic sets or a backward chainer will let us
 *       avoid writing proofs altogether since we can prove whether or not a
 *       statement is still entailed without recourse to reading proofs chains.
 * 
 * @todo explore an option for "owl:sameAs" semantics using destructive merging
 *       (the terms are assigned the same term identifier, one of them is
 *       treated as a canonical, and there is no way to retract the sameAs
 *       assertion). If you take this approach then you must also re-write all
 *       existing assertions using the term whose term identifier is changed to
 *       be that of another term.
 * 
 * @todo if we are not storing rules or magic facts in the main statement
 *       indices then get rid of the leading byte used in all keys for the
 *       statement indices.
 */
public class InferenceEngine extends RDFSHelper {

    final static public Logger log = Logger.getLogger(InferenceEngine.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static public boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static public boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * The capacity of the {@link SPOAssertionBuffer} used when computing entailments.
     * 
     * @see Options#BUFFER_CAPACITY
     */
    private final int bufferCapacity;
    
    /**
     * True iff the Truth Maintenance strategy requires that we store
     * {@link Justification}s for entailments.
     */
    private final boolean justify;
    
    /**
     * The axiom model used by the inference engine.
     */
    private final BaseAxioms axiomModel;
    
    /**
     * The configured axioms.
     */
    public Axioms getAxioms() {
        
        return axiomModel;
        
    }
    
    /**
     * True iff the Truth Maintenance strategy requires that we store
     * {@link Justification}s for entailments.
     */
    final public boolean isJustified() {
        
        return justify;
        
    }

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
    RuleOwlEquivalentClass ruleOwlEquivalentClass;
    RuleOwlEquivalentProperty ruleOwlEquivalentProperty;
    RuleOwlSameAs1 ruleOwlSameAs1;
    RuleOwlSameAs1b ruleOwlSameAs1b;
    RuleOwlSameAs2 ruleOwlSameAs2;
    RuleOwlSameAs3 ruleOwlSameAs3;

    /**
     * A filter for keeping certain entailments out of the database. It is
     * configured based on how the {@link InferenceEngine} is configured.
     * 
     * @see DoNotAddFilter
     */
    public final DoNotAddFilter doNotAddFilter;
    
    /**
     * Choice of the forward closure algorithm.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static enum ForwardClosureEnum {
        
        /**
         * The "fast" algorithm breaks several cycles in the RDFS rules and is
         * significantly faster.
         * 
         * @see InferenceEngine#fastForwardClosure(AbstractTripleStore, boolean)
         */
        Fast(),

        /**
         * The "full" algorithm runs the rules as a set to fixed point.
         * 
         * @see InferenceEngine#fullForwardClosure(AbstractTripleStore, boolean)
         */
        Full();
        
    }
    
    /**
     * Options for the {@link InferenceEngine}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options {

        /**
         * Boolean option - when true the proofs for entailments will be generated and stored in
         * the database.  This option is required by some truth maintenance strategies.
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
         * When <code>true</code> (default <code>false</code>)
         * <code>(?x rdf:type rdfs:Resource)</code> entailments are computed
         * AND stored in the database. When <code>false</code>, rules that
         * produce those entailments are turned off such that they are neither
         * computed NOR stored and a backward chainer or magic sets technique
         * must be used to generate the entailments at query time.
         * <p>
         * Note: The default is <code>false</code> since eagerly materializing
         * those entailments takes a lot of time and space.
         * 
         * @see BackchainTypeResourceIterator
         */
        public static final String FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE = "forwardChainRdfTypeRdfsResource";

        public static final String DEFAULT_FORWARD_RDF_TYPE_RDFS_RESOURCE = "false";

        /**
         * When true the rule model will only run rules for RDFS model theory
         * (no OWL) and the OWL axioms will not be defined (default
         * <code>false</code>).
         */
        public static final String RDFS_ONLY = "rdfsOnly";

        public static final String DEFAULT_RDFS_ONLY = "false";
        
        /**
         * When <code>true</code> (default <code>true</code>) the reflexive
         * entailments for <code>owl:sameAs</code> are computed
         * by forward chaining and stored in the database unless
         * {@link #RDFS_ONLY} is used to completely disable those entailments.
         * When <code>false</code> those entailments are not computed and
         * <code>owl:sameAs</code> processing is disabled.
         */
        public static final String FORWARD_CHAIN_OWL_SAMEAS_CLOSURE = "forwardChainOwlSameAsClosure";

        public static final String DEFAULT_FORWARD_CHAIN_OWL_SAMEAS_CLOSURE = "true";

        /**
         * When <code>true</code> (default <code>false</code>) the
         * entailments that replication properties between instances that are
         * identified as "the same" using <code>owl:sameAs</code> will be
         * forward chained and stored in the database. When <code>false</code>,
         * rules that produce those entailments are turned off such that they
         * are neither computed NOR stored and the entailments may be accessed
         * at query time using the
         * {@link InferenceEngine#backchainIterator(ISPOIterator, long, long, long)}.
         * <p>
         * Note: The default is <code>false</code> since those entailments can
         * take up a LOT of space in the store and are expensive to compute
         * during data load. It is a lot easier to compute them dynamically when
         * presented with a specific triple pattern. While more computation is
         * performed if a fill triple scan is frequently requested, that is an
         * unusual case and significantly less data will be stored regardless.
         * 
         * @see InferenceEngine#backchainIterator(ISPOIterator, long, long,
         *      long)
         * 
         * FIXME Finish backchaining for {@link RuleOwlSameAs2} and
         * {@link RuleOwlSameAs3} and then change the default.
         */
        public static final String FORWARD_CHAIN_OWL_SAMEAS_PROPERTIES = "forwardChainOwlSameAsProperties";

        public static final String DEFAULT_FORWARD_CHAIN_OWL_SAMEAS_PROPERTIES = "false";

        /**
         * When <code>true</code> (default <code>true</code>) the
         * entailments for <code>owl:equivilantProperty</code> are computed by
         * forward chaining and stored in the database. When <code>false</code>,
         * rules that produce those entailments are turned off such that they
         * are neither computed NOR stored and a backward chainer or magic sets
         * technique must be used to generate the entailments at query time.
         * 
         * @todo implement backward chaining for owl:equivalentProperty and
         *       compare performance?
         */
        public static final String FORWARD_CHAIN_OWL_EQUIVALENT_PROPERTY = "forwardChainOwlEquivalentProperty";

        public static final String DEFAULT_FORWARD_CHAIN_OWL_EQUIVALENT_PROPERTY = "true";

        /**
         * When <code>true</code> (default <code>true</code>) the
         * entailments for <code>owl:equivilantClass</code> are computed by
         * forward chaining and stored in the database. When <code>false</code>,
         * rules that produce those entailments are turned off such that they
         * are neither computed NOR stored and a backward chainer or magic sets
         * technique must be used to generate the entailments at query time.
         * 
         * @todo implement backward chaining for owl:equivalentClass and compare
         *       performance?
         */
        public static final String FORWARD_CHAIN_OWL_EQUIVALENT_CLASS = "forwardChainOwlEquivalentClass";

        public static final String DEFAULT_FORWARD_CHAIN_OWL_EQUIVALENT_CLASS = "true";

//        /**
//         * Used by some unit tests to defer the load of the axioms into the
//         * database. This option MUST NOT be used by applications as inference
//         * depends on the axioms being available.
//         */
//        String NOAXIOMS = "noAxioms";
        
        /**
         * <p>
         * Sets the capacity of the {@link SPOAssertionBuffer} used to buffer
         * entailments generated by rules during eager closure for efficient
         * ordered writes using the batch API (default 200k).
         * </p>
         * <p>
         * Some results for comparison on a 45k triple data set:
         * </p>
         * 
         * <pre>
         *  1k    - Computed closure in 4469ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=17966
         *  10k   - Computed closure in 3250ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=24704
         *  50k   - Computed closure in 3187ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=25193
         *  100k  - Computed closure in 3359ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=23903
         *  1000k - Computed closure in 3954ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=20306
         * </pre>
         * 
         * <p>
         * Note that the actual performance will depend on the sustained
         * behavior of the JVM, including online performance tuning, parallism
         * in the implementation, and the characteristics of the specific
         * ontology, especially how it influences the #of entailments generated
         * by each rule.
         * </p>
         * 
         * @todo add a buffer capacity property for the
         *       {@link InferenceEngine#backchainIterator(ISPOIterator, long, long, long)}
         *       or use this capacity there as well.
         */
        public static final String BUFFER_CAPACITY = "bufferCapacity";

        /**
         * @todo experiment with different values for this capacity.
         */
        public static final String DEFAULT_BUFFER_CAPACITY = ""+200*Bytes.kilobyte32;
        
    }

    /**
     * Configure {@link InferenceEngine} using properties used to configure the
     * database.
     * 
     * @param database
     * 
     * @see AbstractTripleStore#getInferenceEngine()
     */
    public InferenceEngine(AbstractTripleStore database) {
    
        this(database.getProperties(), database);
        
    }
    
    /**
     * @param properties
     *            Configuration {@link Options}.
     * @param database
     *            The database for which this class will compute entailments.
     */
    public InferenceEngine(Properties properties, AbstractTripleStore database) {

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

        this.rdfsOnly = Boolean.parseBoolean(properties
                .getProperty(Options.RDFS_ONLY,
                        Options.DEFAULT_RDFS_ONLY));

        log.info(Options.RDFS_ONLY + "=" + rdfsOnly);
        
        if(rdfsOnly) {
            
            this.forwardChainOwlSameAsClosure = false;
            this.forwardChainOwlSameAsProperties = false;
            this.forwardChainOwlEquivalentProperty = false;
            this.forwardChainOwlEquivalentClass = false;
            
        } else {
            
            this.forwardChainOwlSameAsClosure = Boolean.parseBoolean(properties
                    .getProperty(Options.FORWARD_CHAIN_OWL_SAMEAS_CLOSURE,
                            Options.DEFAULT_FORWARD_CHAIN_OWL_SAMEAS_CLOSURE));

            log.info(Options.FORWARD_CHAIN_OWL_SAMEAS_CLOSURE + "="
                    + forwardChainOwlSameAsClosure);

            if(forwardChainOwlSameAsClosure) {

                this.forwardChainOwlSameAsProperties = Boolean.parseBoolean(properties
                    .getProperty(Options.FORWARD_CHAIN_OWL_SAMEAS_PROPERTIES,
                            Options.DEFAULT_FORWARD_CHAIN_OWL_SAMEAS_PROPERTIES));
                
            } else {
                
                this.forwardChainOwlSameAsProperties = false;
                
            }

            log.info(Options.FORWARD_CHAIN_OWL_SAMEAS_CLOSURE + "="
                    + forwardChainOwlSameAsClosure);

            this.forwardChainOwlEquivalentProperty = Boolean
                    .parseBoolean(properties
                            .getProperty(
                                    Options.FORWARD_CHAIN_OWL_EQUIVALENT_PROPERTY,
                                    Options.DEFAULT_FORWARD_CHAIN_OWL_EQUIVALENT_PROPERTY));

            log.info(Options.FORWARD_CHAIN_OWL_EQUIVALENT_PROPERTY + "="
                    + forwardChainOwlEquivalentProperty);

            this.forwardChainOwlEquivalentClass = Boolean
                    .parseBoolean(properties.getProperty(
                            Options.FORWARD_CHAIN_OWL_EQUIVALENT_CLASS,
                            Options.DEFAULT_FORWARD_CHAIN_OWL_EQUIVALENT_CLASS));

            log.info(Options.FORWARD_CHAIN_OWL_EQUIVALENT_CLASS + "="
                    + forwardChainOwlEquivalentClass);

        }
        
        bufferCapacity = Integer.parseInt(properties.getProperty(
                Options.BUFFER_CAPACITY, Options.DEFAULT_BUFFER_CAPACITY));
        
        log.info(Options.BUFFER_CAPACITY + "=" + bufferCapacity);

        // Note: used by the DoNotAddFilter.
        axiomModel = (rdfsOnly ? new RdfsAxioms(database) : new OwlAxioms(
                database));
        
        // Add axioms to the database (writes iff not defined).
        axiomModel.addAxioms();
        
        doNotAddFilter = new DoNotAddFilter(this, axiomModel,
                forwardChainRdfTypeRdfsResource);
        
        setupRules();

    }
    
//    final boolean noAxioms;
    
    /**
     * Set based on {@link Options#FORWARD_CLOSURE}. 
     */
    final protected ForwardClosureEnum forwardClosure;
    
    /**
     * Set based on {@link Options#RDFS_ONLY}. When set, owl:sameAs and friends
     * are disabled and only the RDFS MT entailments are used.
     */
    final protected boolean rdfsOnly;
    
    /**
     * Set based on {@link Options#RDFS_ONLY}. When set, owl:sameAs and friends
     * are disabled and only the RDFS MT entailments are used.
     */
    public final boolean isRdfsOnly() {
        
        return rdfsOnly;
        
    }
    
    /**
     * Set based on {@link Options#FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE}. When
     * <code>true</code> the {@link InferenceEngine} is configured to forward
     * chain and store entailments of the form
     * <code>(x rdf:type rdfs:Resource)</code>. When <code>false</code>,
     * those entailments are computed at query time by
     * {@link #backchainIterator(ISPOIterator, long, long, long)}.
     */
    final protected boolean forwardChainRdfTypeRdfsResource;

    /**
     * Set based on {@link Options#FORWARD_CHAIN_OWL_SAMEAS_CLOSURE}. When
     * <code>true</code> we will forward chain and store the reflexive and
     * transitive closure of <code>owl:sameAs</code> using
     * {@link RuleOwlSameAs1} and {@link RuleOwlSameAs2}.
     * <p>
     * Note: When <code>false</code>, NO owl:sameAs processing will be
     * performed since there is no privision for backward chaining the
     * owl:sameAs closure.
     */
    final protected boolean forwardChainOwlSameAsClosure;

    /**
     * Set based on {@link Options#FORWARD_CHAIN_OWL_SAMEAS_PROPERTIES}. When
     * <code>true</code>, we will forward chain {@link RuleOwlSameAs2} and
     * {@link RuleOwlSameAs3} which replicate properties on individuals
     * identified as the "same" by <code>owl:sameAs</code>. When
     * <code>false</code>, we will compute those entailments at query time in
     * {@link #backchainIterator(ISPOIterator, long, long, long)}.
     */
    final protected boolean forwardChainOwlSameAsProperties;

    /**
     * Set based on {@link Options#FORWARD_CHAIN_OWL_EQUIVALENT_PROPERTY}. When
     * <code>true</code>, we will forward chain and store those entailments.
     * When <code>false</code>, those entailments will NOT be available.
     */
    final protected boolean forwardChainOwlEquivalentProperty;

    /**
     * Set based on {@link Options#FORWARD_CHAIN_OWL_EQUIVALENT_CLASS}. When
     * <code>true</code>, we will forward chain and store those entailments.
     * When <code>false</code>, those entailments will NOT be available.
     */
    final protected boolean forwardChainOwlEquivalentClass;
    
    /**
     * Sets up the basic rule model for the inference engine.
     * 
     * @todo setup both variants of rdfs5 and rdfs11 and then use the self-join
     *       variant when we are closing the database against itself and the
     *       nested subquery variant when we are doing truth maintenance (or for
     *       extremely large data sets).
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

        ruleOwlEquivalentClass = new RuleOwlEquivalentClass(this);

        ruleOwlEquivalentProperty = new RuleOwlEquivalentProperty(this);
        
        ruleOwlSameAs1 = new RuleOwlSameAs1(this);

        ruleOwlSameAs1b = new RuleOwlSameAs1b(this);

        ruleOwlSameAs2 = new RuleOwlSameAs2(this);
        
        ruleOwlSameAs3 = new RuleOwlSameAs3(this);

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

        if(!rdfsOnly) {

            if (forwardChainOwlSameAsClosure) {

                rules.add(ruleOwlSameAs1);

                if (forwardChainOwlSameAsProperties) {

                    rules.add(ruleOwlSameAs1b);

                    rules.add(ruleOwlSameAs2);

                    rules.add(ruleOwlSameAs3);

                }

            }

            if (forwardChainOwlEquivalentProperty) {

                rules.add(ruleOwlEquivalentProperty);

            }

            if (forwardChainOwlEquivalentClass) {

                rules.add(ruleOwlEquivalentClass);

            }

        }
        
        return rules.toArray(new Rule[rules.size()]);

    }
    
    /**
     * Compute the forward closure of a focusStore against the database using
     * the algorithm selected by {@link Options#FORWARD_CLOSURE}.
     * <p>
     * Note: before calling this method, the caller SHOULD examine the
     * statements in the focusStore and then database. For each statement in the
     * focusStore, if this statement exists explicitly in the database then
     * remove it from the focusStore. If this statement exists implicitly in the
     * database. Regardless of whether the statement was explicit or inferred in
     * the database, remove it from the focusStore. This step prevents the
     * needless (and expensive) reapplication of the rules to data already known
     * to the database!
     * <p>
     * Note: If the <i>focusStore</i> is given, then the entailments will be
     * asserted against the focusStore. Either this method or the caller MUST
     * copy the <i>focusStore</i> onto the database using
     * {@link AbstractTripleStore#copyStatements(AbstractTripleStore, ISPOFilter,boolean)}.
     * If you are loading data from some kind of resource, then see
     * {@link DataLoader} which already knows how to do this.
     * <p>
     * See
     * {@link TruthMaintenance#assertAll(com.bigdata.rdf.store.TempTripleStore)},
     * which first handles statements already in the database, then calls this
     * method, and finally copies the remaining explicit statements in the
     * focusStore and the entailments into the database.
     * 
     * @param focusStore
     *            The data set that will be closed against the database
     *            (optional). When <code>null</code> the store will be closed
     *            against itself.
     * 
     * @return Statistics about the operation.
     */
    public ClosureStats computeClosure(AbstractTripleStore focusStore) {
        
        return computeClosure(focusStore, isJustified());
        
    }
    
    /**
     * This variant allows you to explicitly NOT generate {@link Justification}s
     * for the computed entailments. It is used by the {@link TruthMaintenance}
     * class as part of the algorithm for truth maintenance when retracting
     * statements from the database. It SHOULD NOT be used for any other purpose
     * or you may risk failing to generate justifications.
     * 
     * @param focusStore
     *            The data set that will be closed against the database.
     * @param justify
     *            {@link Justification}s will be generated iff this flag is
     *            <code>true</code>.
     * 
     * @return Statistics about the operation.
     * 
     * @see #computeClosure(AbstractTripleStore)
     */
    public ClosureStats computeClosure(AbstractTripleStore focusStore, boolean justify) {
        
        switch(forwardClosure) {

        case Fast:
            return fastForwardClosure(focusStore, justify);
        
        case Full:
            return fullForwardClosure(focusStore, justify);
        
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
    protected ClosureStats fullForwardClosure(AbstractTripleStore focusStore,
            boolean justify) {

        final long begin = System.currentTimeMillis();
       
        final ClosureStats closureStats = new ClosureStats();

        // The store where the entailments are being built up.
        final AbstractTripleStore closureStore = (focusStore != null ? focusStore
                : database);

        final long nbefore = closureStore.getStatementCount();
        
//        /*
//         * add RDF(S) axioms to the database.
//         * 
//         * Note: If you insert Statements using the Sesame object model into the
//         * focusStore then it will have different term identifiers for those
//         * assertions that the ones that are used by the database, which is
//         * very, very bad.
//         */
//        axiomModel.addAxioms();

        // entailment buffer writes on the closureStore.
        final SPOAssertionBuffer buffer = new SPOAssertionBuffer(closureStore,
                database, doNotAddFilter, bufferCapacity, justify);

        // compute the full forward closure.
        
        Rule.fixedPoint(closureStats, getRuleModel(), justify, focusStore,
                database, buffer).toString();
       
        if(INFO) {
            
            log.info(closureStats.toString());
            
        }
        
        final long nafter = closureStore.getStatementCount();
        
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
    protected ClosureStats fastForwardClosure(AbstractTripleStore focusStore,
            boolean justify) {

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
        final SPOAssertionBuffer buffer = new SPOAssertionBuffer(closureStore,
                database, doNotAddFilter, bufferCapacity, justify);
        
        final long firstStatementCount = closureStore.getStatementCount();

        final long begin = System.currentTimeMillis();

        log.debug("Closing kb with " + firstStatementCount
                + " statements");

//        /*
//         * 1. add RDF(S) axioms to the database (done at initialization).
//         * 
//         * Note: If you insert Statements using the Sesame object model into the
//         * focusStore then it will have different term identifiers for those
//         * assertions that the ones that are used by the database, which is
//         * very, very bad.
//         */ 
//        axiomModel.addAxioms();
        
        if(!rdfsOnly) {
            // owl:equivalentProperty
            if (forwardChainOwlEquivalentProperty) {
                Rule.fixedPoint(closureStats,
                        new Rule[] { ruleOwlEquivalentProperty }, justify,
                        focusStore, database, buffer);
            }
        }

        // 2. Compute P (the set of possible sub properties).
        final Set<Long> P = getSubProperties(focusStore,database);

        // 3. (?x, P, ?y) -> (?x, rdfs:subPropertyOf, ?y)
        {
            Rule r = new RuleFastClosure3(this, P);
            RuleStats stats = r.apply(justify,focusStore,database,buffer);
            closureStats.add(stats);
            if(DEBUG) log.debug("step3: " + stats);
            buffer.flush();
        }

        // 4. RuleRdfs05 until fix point (rdfs:subPropertyOf closure).
        Rule.fixedPoint(closureStats, new Rule[] { rdfs5 }, justify,
                focusStore, database, buffer);
        if (DEBUG)
            log.debug("rdfs5: " + closureStats);

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
            if(DEBUG) log.debug("step5: " + stats);
            // Note: deferred buffer.flush() since the next step has no
            // dependency.
        }

        // 6. (?x, R, ?y ) -> (?x, rdfs:range, ?y)
        {
            Rule r = new RuleFastClosure6(this, R);
            RuleStats stats = r.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            if(DEBUG) log.debug("step6: " + stats);
            // Note: deferred buffer.flush() since the next step has no
            // dependency.
        }

        if(!rdfsOnly) {
            // owl:equivalentClass
            if (forwardChainOwlEquivalentClass) {
                Rule.fixedPoint(closureStats,
                        new Rule[] { ruleOwlEquivalentClass }, justify, focusStore,
                        database, buffer);
                if (DEBUG)
                    log.debug("owlEquivalentClass: " + closureStats);
    
            }
        }
        
        // 7. (?x, C, ?y ) -> (?x, rdfs:subClassOf, ?y)
        {
            Rule r = new RuleFastClosure7(this, C);
            RuleStats stats = r.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            if(DEBUG) log.debug("step7: " + stats);
            // Note: flush buffer before running rdfs11.
            buffer.flush();
        }

        // 8. RuleRdfs11 until fix point (rdfs:subClassOf closure).
        Rule.fixedPoint(closureStats, new Rule[] { rdfs11 }, justify,
                focusStore, database, buffer);
        if(DEBUG) log.debug("rdfs11: " + closureStats);

        // 9. (?x, T, ?y ) -> (?x, rdf:type, ?y)
        {
            Rule r = new RuleFastClosure9(this, T);
            RuleStats stats = r.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            if(DEBUG) log.debug("step9: " + stats);
            buffer.flush();
        }

        // 10. RuleRdfs02
        {
            RuleStats stats = rdfs2.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            if(DEBUG) log.debug("rdfs2: " + stats);
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
            if(DEBUG) log.debug("step11: " + stats);
            buffer.flush();
        }
        
        // 12. RuleRdfs03
        {
            RuleStats stats = rdfs3.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            if(DEBUG) log.debug("rdfs3: "+stats);
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
            if(DEBUG) log.debug("step13: " + stats);
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
                if(DEBUG) log.debug("rdfs4a: "+stats);
            }
            {
                RuleStats stats = rdfs4b.apply(justify, focusStore, database, buffer);
                closureStats.add(stats);
                if(DEBUG) log.debug("rdfs4b: "+stats);
            }
            buffer.flush();

        }

        // 16. RuleRdf01
        {
            RuleStats stats = rdf1.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            if(DEBUG) log.debug("rdf1: "+stats);
            buffer.flush();
        }
        
        // 17. RuleRdfs09
        {
            RuleStats stats = rdfs9.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            if(DEBUG) log.debug("rdfs9: "+stats);
            buffer.flush();
        }
        
        // 18. RuleRdfs10
        {
            RuleStats stats = rdfs10.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            if(DEBUG) log.debug("rdfs10: "+stats);
            buffer.flush();
        }
        
        // 19. RuleRdfs08.
        {
            RuleStats stats = rdfs8.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            if(DEBUG) log.debug("rdfs8: "+stats);
            buffer.flush();
        }

        // 20. RuleRdfs13.
        {
            RuleStats stats = rdfs13.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            if(DEBUG) log.debug("rdfs13: " + stats);
            buffer.flush();
        }

        // 21. RuleRdfs06.
        {
            RuleStats stats = rdfs6.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            if(DEBUG) log.debug("rdfs6: " + stats);
            buffer.flush();
        }

        // 22. RuleRdfs07.
        {
            RuleStats stats = rdfs7.apply(justify, focusStore, database, buffer);
            closureStats.add(stats);
            if(DEBUG) log.debug("rdfs7: " + stats);
            buffer.flush();
        }
        
        if(!rdfsOnly) {
        // owl:sameAs
        if(forwardChainOwlSameAsClosure) {

            // reflexive closure over owl:sameAs.
            Rule.fixedPoint(closureStats, new Rule[] { ruleOwlSameAs1/*,
                    ruleOwlSameAs1b*/ }, justify, focusStore, database, buffer);            
            
            if(DEBUG) log.debug("owl:sameAs1,1b: "+closureStats);

            if (forwardChainOwlSameAsProperties) {
                
                // transitive closure over owl:sameAs.
                Rule.fixedPoint(closureStats, new Rule[] { /*ruleOwlSameAs1,*/
                        ruleOwlSameAs1b }, justify, focusStore, database, buffer);            
                
                /*
                 * Apply properties.
                 * 
                 * Note: owl:sameAs2,3 should exclude matches where the
                 * predicate in the head is owl:sameAs. This case is already
                 * covered by rules owl:sameas {1, 1b}. We specialize the rules
                 * here since we also use 2 and 3 in the full forward closure
                 * where all rules are brought to fix point together and we do
                 * NOT want to make that exclusion in that case.
                 */

                {
                    RuleStats stats = ruleOwlSameAs2.specialize(//
                            NULL, NULL, NULL, //
                            new IConstraint[] {
                                new NEConstant((Var)ruleOwlSameAs2.head.p,owlSameAs.id)
                            }).apply(justify, focusStore, database, buffer
                            );
                    closureStats.add(stats);
                    if (DEBUG)
                        log.debug("ruleOwlSameAs2: " + stats);
                    buffer.flush();
                }

                {
                    RuleStats stats = ruleOwlSameAs3.specialize(//
                            NULL, NULL, NULL, //
                            new IConstraint[] {
                                new NEConstant((Var)ruleOwlSameAs3.head.p,owlSameAs.id)
                            }).apply(justify, focusStore, database, buffer
                            );
                    closureStats.add(stats);
                    if (DEBUG)
                        log.debug("ruleOwlSameAs3: " + stats);
                    buffer.flush();
                }

            }

        }
        }
        
        /*
         * Done.
         */
        
        final long elapsed = System.currentTimeMillis() - begin;

        final long lastStatementCount = closureStore.getStatementCount();

        final long inferenceCount = lastStatementCount - firstStatementCount;
        
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
     * Return true iff the fully bound statement is an axiom.
     * 
     * @param s
     * @param p
     * @param o
     * 
     * @return
     */
    public boolean isAxiom(long s, long p, long o) {
        
        return axiomModel.isAxiom(s, p, o);
        
    }

    /**
     * Obtain an iterator that will read on the appropriate {@link IAccessPath}
     * for the database and also backchain any entailments for which forward
     * chaining has been turned off, including {@link RuleOwlSameAs2},
     * {@link RuleOwlSameAs3}, and <code>(x rdf:type rdfs:Resource)</code>.
     * 
     * @param s
     *            The subject in triple pattern for that access path.
     * @param p
     *            The predicate in triple pattern for that access path.
     * @param o
     *            The object in triple pattern for that access path.
     * 
     * @return An iterator that will visit the statements in database matching
     *         the triple pattern query plus any necessary entailments.
     */
    public ISPOIterator backchainIterator(long s, long p, long o) {
        
        return backchainIterator(s, p, o, null );
        
    }

    /**
     * Obtain an iterator that will read on the appropriate {@link IAccessPath}
     * for the database and also backchain any entailments for which forward
     * chaining has been turned off, including {@link RuleOwlSameAs2},
     * {@link RuleOwlSameAs3}, and <code>(x rdf:type rdfs:Resource)</code>.
     * 
     * @param s
     *            The subject in triple pattern for that access path.
     * @param p
     *            The predicate in triple pattern for that access path.
     * @param o
     *            The object in triple pattern for that access path.
     * 
     * @return An iterator that will visit the statements in database matching
     *         the triple pattern query plus any necessary entailments.
     * 
     * @todo configure buffer sizes.
     */
    public ISPOIterator backchainIterator(long s, long p, long o, ISPOFilter filter) {
        
        final ISPOIterator src = database.getAccessPath(s, p, o).iterator(filter);
        
        final ISPOIterator ret;

        if (rdfsOnly) {
            
            // no entailments.
            ret = null;
        
        } else if(forwardChainOwlSameAsClosure && !forwardChainOwlSameAsProperties) {
            
            ret = new BackchainOwlSameAsPropertiesIterator(//
                    src,//
                    s,p,o,//
                    database, //
                    owlSameAs.id
                    );

        } else {
            
            // no entailments.
            ret = null;
            
        }
        
        /*
         * Wrap it up as a chunked iterator.
         * 
         * Note: If we are not adding any entailments then we just use the
         * source iterator directly.
         */

        ISPOIterator itr = (ret == null ? src : new ChunkedSPOIterator(ret, 10000,
                filter));

        if (!forwardChainRdfTypeRdfsResource) {
            
            /*
             * Backchain (x rdf:type rdfs:Resource ).
             * 
             * @todo pass the filter in here also.
             */
            
            itr = new BackchainTypeResourceIterator(//
                    itr,//
                    s,p,o,//
                    database, //
                    rdfType.id, //
                    rdfsResource.id //
                    );
            
        }

        return itr;
        
    }
    
//    /**
//     * An abstract base class for an iterator that records the a term identifier
//     * from the visited {@link SPO}s on the caller's {@link BTree}. The keys
//     * of the btree will be a byte[] encoding the long integer. The values will
//     * be <code>null</code>.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    abstract static public class TermIdRecordingResolver extends Resolver {
//        
//        private final BTree btree;
//        
//        /**
//         * Used to build keys for the {@link #btree}.
//         */
//        private final KeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG);
//
//        public TermIdRecordingResolver(BTree btree) {
//        
//            this.btree = btree;
//            
//        }
//
//        public void add(long v) {
//
//            btree.insert(keyBuilder.reset().append(v).getKey(),null);
//
//            log.info("add: "+v+", size="+btree.getEntryCount());
//
//        }
//
//        abstract void add(SPO spo);
//        
//        protected Object resolve(Object arg0) {
//            
//            add((SPO)arg0);
//            
//            return arg0;
//            
//        }
//        
//    }
//
//    /**
//     * Visits the keys in the {@link BTree}, decoding each key as a
//     * {@link Long}.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class TermIdKeyIterator implements Iterator<Long> {
//
//        private final BTree btree;
//        
//        /**
//         * Note: this is initialized lazily so that the btree are fully
//         * populated 1st.
//         */
//        private ITupleIterator src;
//        
//        /**
//         * 
//         * @param btree
//         *            A {@link BTree} whose keys are byte[]s encoded as long
//         *            integers using {@link KeyBuilder#append(long)}
//         */
//        public TermIdKeyIterator(BTree btree) {
//
//            this.btree = btree;
//            
//        }
//        
//        public boolean hasNext() {
//
//            if(src==null) {
//
//                /*
//                 * Note: Lazily initialized so that the btree can be fully
//                 * populated 1st.
//                 */
//
//                log.info("Will visit "+btree.getEntryCount()+" entries");
//
//                src = btree.rangeIterator(null, null, 0/* capacity */,
//                        IRangeQuery.KEYS, null/*filter*/);
//                                
//            }
//            
//            return src.hasNext();
//            
//        }
//
//        public Long next() {
//            
//            ITuple tuple = src.next();
//            
//            long v = KeyBuilder.decodeLong(tuple.getKeyBuffer().array(), 0);
//
//            log.info("next="+v);
//            
//            return v;
//            
//        }
//
//        public void remove() {
//
//            throw new UnsupportedOperationException();
//            
//        }
//        
//    }
//    
//    /**
//     * Expand each distinct subject collected during some triple pattern query
//     * by constrained forward chaining of {@link RuleOwlSameAs2}.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class OwlSameAs2Expander extends Expander {
//
//        private static final long serialVersionUID = -3101713347977424002L;
//
//        private final long NULL = IRawTripleStore.NULL;
//        private final InferenceEngine inf;
//        private final AbstractTripleStore db;
//        private final long s, p, o;
//        
//        /**
//         * 
//         * @param inf
//         * @param s
//         * @param p
//         * @param o
//         */
//        public OwlSameAs2Expander(InferenceEngine inf,long s, long p, long o) {
//            
//            this.inf = inf;
//            
//            this.db = inf.database;
//            
//            this.s = s;
//            
//            this.p = p;
//            
//            this.o = o;
//            
//        }
//        
//        protected Iterator expand(Object arg0) {
//            
//            return expand(((Long)arg0).longValue());
//            
//        }
//
//        /**
//         * Applies a specialization of {@link RuleOwlSameAs2} where the triple
//         * pattern given to the ctor has been bound on the head of the rule to
//         * compute the entailments for the specified subject.
//         * 
//         * <pre>
//         * (x owl:sameAs y), (x a z) -&gt; (y a z).
//         * </pre>
//         * 
//         * @param subject
//         *            A subject visited in the course of a triple pattern query
//         *            reading on some access path. The <i>subject</i> is bound
//         *            on <code>x</code>. The triple pattern given to the ctor
//         *            is bound on the head of the rule (y, a, z). The rule is
//         *            then forward chained to generate the entailments.
//         * 
//         * @return An iterator visiting the entailments for that <i>subject</i>.
//         */
//        private Iterator<SPO> expand(long subject) {
//            
//            if(p==inf.owlSameAs.id) {
//
//                /*
//                 * No entailments when predicate in triple pattern is bound to
//                 * owl:sameAs
//                 */
//                
//                return EmptyIterator.DEFAULT;
//                
//            }
//
//            assert subject != NULL;
//            
//            log.info("subject="+db.toString(subject)+"("+subject+")");
//            
//            // owl:sameAs2: (x owl:sameAs y), (x a z) -&gt; (y a z).
//            Rule r = inf.ruleOwlSameAs2;
//
//            // setup bindings.
//            Map<Var,Long> bindings = new TreeMap<Var, Long>();
//            
//            // the current distinct subject.
//            if(s!=NULL) bindings.put((Var)r.head.s,subject);
//
//            // from the triple pattern.
//            if(p!=NULL) bindings.put((Var)r.head.p,p);
//            if(o!=NULL) bindings.put((Var)r.head.o,o);
//            
////            // the current subject on (x)
////            bindings.put((Var)r.body[0].s, subject);
//            
//            // specialize the rule.
//            final Rule r1 = r.specialize(//
//                    bindings,
//                    new IConstraint[] {//
//                        new NEConstant((Var) r.head.p, inf.owlSameAs.id) //
//                    });
//            
//            /*
//             * Buffer on which the rule will write.
//             * 
//             * @todo capacity parameter.
//             */
//            final SPOBlockingBuffer buffer = new SPOBlockingBuffer(db,
//                    null /* filter */, 10000/* capacity */);
//            
//            db.getThreadPool().submit(new Runnable() {
//
//                public void run() {
//
//                    try {
//                    
//                        // run the rule.
//                        r1.apply(false/* justify */, null/* focusStore */,
//                                db/* database */, buffer);
//                    } finally {
//
//                        // close the buffer
//                        buffer.close();
//
//                    }
//
//                }
//
//            });
//
//            return buffer.iterator();
//            
//        }
//        
//    }
//    
//    /**
//     * Expand each distinct object collected during some triple pattern query
//     * by constrained forward chaining of {@link RuleOwlSameAs3}.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class OwlSameAs3Expander extends Expander {
//
//        private static final long serialVersionUID = -1806816164484260451L;
//        private final long NULL = IRawTripleStore.NULL;
//        private final InferenceEngine inf;
//        private final AbstractTripleStore db;
//        private final long s, p, o;
//        
//        /**
//         * 
//         * @param inf
//         * @param s
//         * @param p
//         * @param o
//         */
//        public OwlSameAs3Expander(InferenceEngine inf,long s, long p, long o) {
//            
//            this.inf = inf;
//            
//            this.db = inf.database;
//            
//            this.s = s;
//            
//            this.p = p;
//            
//            this.o = o;
//            
//        }
//        
//        protected Iterator expand(Object arg0) {
//            
//            return expand(((Long)arg0).longValue());
//            
//        }
//
//        /**
//         * Applies a specialization of {@link RuleOwlSameAs3} where the triple
//         * pattern given to the ctor has been bound on the head of the rule to
//         * compute the entailments for the specified subject.
//         * 
//         * <pre>
//         * (x owl:sameAs y), (z a x) -&gt; (z a y).
//         * </pre>
//         * 
//         * @param object
//         *            An object visited in the course of a triple pattern query
//         *            reading on some access path. The <i>object</i> is bound
//         *            on <code>x</code>. The triple pattern given to the ctor
//         *            is bound on the head of the rule (y, a, z). The rule is
//         *            then forward chained to generate the entailments.
//         * 
//         * @return An iterator visiting the entailments for that <i>object</i>.
//         */
//        private Iterator<SPO> expand(long object) {
//
//            if(p==inf.owlSameAs.id) {
//
//                /*
//                 * No entailments when predicate in triple pattern is bound to
//                 * owl:sameAs
//                 */
//                
//                return EmptyIterator.DEFAULT;
//                
//            }
//            
//            assert object != NULL;
//            
//            log.info("object="+db.toString(object)+"("+object+")");
//            
//            // owl:sameAs3: (x owl:sameAs y), (z a x) -&gt; (z a y).
//            Rule r = inf.ruleOwlSameAs3;
//
//            // setup bindings.
//            Map<Var,Long> bindings = new TreeMap<Var, Long>();
//            
//            if(s!=NULL) bindings.put((Var)r.head.s,s);
//            if(p!=NULL) bindings.put((Var)r.head.p,p);
//            if(o!=NULL) bindings.put((Var)r.head.o,o);
//            
//            // bind the current object on (x)
//            bindings.put((Var)r.body[0].s, object);
//            
//            // specialize the rule.
//            final Rule r1 = r.specialize(//
//                    bindings,
//                    new IConstraint[] {//
//                        new NEConstant((Var) r.head.p, inf.owlSameAs.id) //
//                    });
//            
//            /*
//             * Buffer on which the rule will write.
//             * 
//             * @todo capacity parameter.
//             */
//            final SPOBlockingBuffer buffer = new SPOBlockingBuffer(db,
//                    null /* filter */, 10000/* capacity */);
//            
//            db.getThreadPool().submit(new Runnable() {
//
//                public void run() {
//
//                    try {
//
//                        // run the rule.
//                        r1.apply(false/* justify */, null/* focusStore */,
//                                db/* database */, buffer);
//
//                    } finally {
//
//                        // close the buffer
//                        buffer.close();
//
//                    }
//                    
//                }
//
//            });
//
//            return buffer.iterator();
//            
//        }
//        
//    }
    
}

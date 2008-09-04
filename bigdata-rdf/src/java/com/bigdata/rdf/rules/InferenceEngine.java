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
package com.bigdata.rdf.rules;

import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.axioms.RdfsAxioms;
import com.bigdata.rdf.inf.BackchainTypeResourceIterator;
import com.bigdata.rdf.inf.ClosureStats;
import com.bigdata.rdf.inf.Justification;
import com.bigdata.rdf.inf.TruthMaintenance;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.Program;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.DefaultEvaluationPlanFactory2;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;

/**
 * Flyweight object encapsulates some configuration state and provides methods
 * to compute or update the closure of the database. An instance of this is
 * obtained from {@link AbstractTripleStore#getInferenceEngine()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Improve write efficiency for the proofs - they are slowing things way
 *       down. Perhaps turn off the range count metadata inside of the B+Tree
 *       for that index? Note that using magic sets or a backward chainer will
 *       let us avoid writing proofs altogether since we can prove whether or
 *       not a statement is still entailed without recourse to reading proofs
 *       chains.
 * 
 * @todo explore an option for "owl:sameAs" semantics using destructive merging
 *       (the terms are assigned the same term identifier, one of them is
 *       treated as a canonical, and there is no way to retract the sameAs
 *       assertion). If you take this approach then you must also re-write all
 *       existing assertions using the term whose term identifier is changed to
 *       be that of another term.
 */
public class InferenceEngine {

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
     * The database whose closure is being maintained.
     */
    final public AbstractTripleStore database;
    
    /**
     * A filter for keeping certain entailments out of the database. It is
     * configured based on how the {@link InferenceEngine} is configured.
     * 
     * @see DoNotAddFilter
     */
    public final DoNotAddFilter doNotAddFilter;

    /**
     * Options for the {@link InferenceEngine}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options {

        /**
         * When <code>true</code> (default
         * {@value #DEFAULT_FORWARD_RDF_TYPE_RDFS_RESOURCE})
         * <code>(?x rdf:type rdfs:Resource)</code> entailments are computed
         * AND stored in the database. When <code>false</code>, rules that
         * produce those entailments are turned off such that they are neither
         * computed NOR stored and a backward chainer or magic sets technique
         * must be used to generate the entailments at query time.
         * <p>
         * Note: Eagerly materializing those entailments takes a lot of time and
         * space but it reduces time during query IF you are asking for these
         * entailments (many realistic queries do not). Therefore it is
         * generally a win to turn this option off.
         * 
         * @see BackchainAccessPath
         * @see BackchainTypeResourceIterator
         */
        String FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE = "forwardChainRdfTypeRdfsResource";

        String DEFAULT_FORWARD_RDF_TYPE_RDFS_RESOURCE = "false";

        /**
         * When <code>true</code> (default
         * {@value #DEFAULT_FORWARD_CHAIN_OWL_SAMEAS_CLOSURE}) the reflexive
         * entailments for <code>owl:sameAs</code> are computed by forward
         * chaining and stored in the database unless
         * {@link com.bigdata.rdf.store.AbstractTripleStore.Options#AXIOMS_CLASS}
         * is used to completely disable those entailments, e.g., by specifying
         * either {@link NoAxioms} or {@link RdfsAxioms}. When
         * <code>false</code> those entailments are not computed and
         * <code>owl:sameAs</code> processing is disabled.
         */
        String FORWARD_CHAIN_OWL_SAMEAS_CLOSURE = "forwardChainOwlSameAsClosure";

        String DEFAULT_FORWARD_CHAIN_OWL_SAMEAS_CLOSURE = "true";

        /**
         * When <code>true</code> (default
         * {@value #DEFAULT_FORWARD_CHAIN_OWL_SAMEAS_PROPERTIES}) the
         * entailments that replication properties between instances that are
         * identified as "the same" using <code>owl:sameAs</code> will be
         * forward chained and stored in the database. When <code>false</code>,
         * rules that produce those entailments are turned off such that they
         * are neither computed NOR stored and the entailments may be accessed
         * at query time using the {@link BackchainAccessPath}.
         * <p>
         * Note: The default is <code>false</code> since those entailments can
         * take up a LOT of space in the store and are expensive to compute
         * during data load. It is a lot easier to compute them dynamically when
         * presented with a specific triple pattern. While more computation is
         * performed if a fill triple scan is frequently requested, that is an
         * unusual case and significantly less data will be stored regardless.
         * 
         * @see BackchainAccessPath
         */
        String FORWARD_CHAIN_OWL_SAMEAS_PROPERTIES = "forwardChainOwlSameAsProperties";

        String DEFAULT_FORWARD_CHAIN_OWL_SAMEAS_PROPERTIES = "false";

        /**
         * When <code>true</code> (default
         * {@value #DEFAULT_FORWARD_CHAIN_OWL_EQUIVALENT_PROPERTY}) the
         * entailments for <code>owl:equivilantProperty</code> are computed by
         * forward chaining and stored in the database. When <code>false</code>,
         * rules that produce those entailments are turned off such that they
         * are neither computed NOR stored and a backward chainer or magic sets
         * technique must be used to generate the entailments at query time.
         * 
         * @todo implement backward chaining for owl:equivalentProperty and
         *       compare performance?
         */
        String FORWARD_CHAIN_OWL_EQUIVALENT_PROPERTY = "forwardChainOwlEquivalentProperty";

        String DEFAULT_FORWARD_CHAIN_OWL_EQUIVALENT_PROPERTY = "true";

        /**
         * When <code>true</code> (default
         * {@value #DEFAULT_FORWARD_CHAIN_OWL_EQUIVALENT_CLASS}) the
         * entailments for <code>owl:equivilantClass</code> are computed by
         * forward chaining and stored in the database. When <code>false</code>,
         * rules that produce those entailments are turned off such that they
         * are neither computed NOR stored and a backward chainer or magic sets
         * technique must be used to generate the entailments at query time.
         * 
         * @todo implement backward chaining for owl:equivalentClass and compare
         *       performance?
         */
        String FORWARD_CHAIN_OWL_EQUIVALENT_CLASS = "forwardChainOwlEquivalentClass";

        String DEFAULT_FORWARD_CHAIN_OWL_EQUIVALENT_CLASS = "true";

        /**
         * When <code>true</code> (default
         * {@value #FORWARD_CHAIN_OWL_INVERSE_OF}) the entailments for
         * <code>owl:InverseOf</code> are computed by forward chaining and
         * stored in the database. When <code>false</code>, rules that
         * produce those entailments are turned off such that they are neither
         * computed NOR stored and a backward chainer or magic sets technique
         * must be used to generate the entailments at query time.
         */
        String FORWARD_CHAIN_OWL_INVERSE_OF = "forwardChainOwlInverseOf";

        String DEFAULT_FORWARD_CHAIN_OWL_INVERSE_OF = "false";

        /**
         * When <code>true</code> (default
         * {@value #DEFAULT_FORWARD_CHAIN_OWL_TRANSITIVE_PROPERY}) the
         * entailments for <code>owl:TransitiveProperty</code> are computed by
         * forward chaining and stored in the database. When <code>false</code>,
         * rules that produce those entailments are turned off such that they
         * are neither computed NOR stored and a backward chainer or magic sets
         * technique must be used to generate the entailments at query time.
         */
        String FORWARD_CHAIN_OWL_TRANSITIVE_PROPERY = "forwardChainOwlTransitiveProperty";

        String DEFAULT_FORWARD_CHAIN_OWL_TRANSITIVE_PROPERY = "false";

    }

    /**
     * Configure {@link InferenceEngine} using the properties used to configure
     * the database.
     * 
     * @param database
     *            The database.
     * 
     * @see AbstractTripleStore#getInferenceEngine()
     */
    public InferenceEngine(AbstractTripleStore database) {

        if (database == null)
            throw new IllegalArgumentException();

        this.database = database;
        
        final Properties properties = database.getProperties();

        this.forwardChainRdfTypeRdfsResource = Boolean.parseBoolean(properties
                .getProperty(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,
                        Options.DEFAULT_FORWARD_RDF_TYPE_RDFS_RESOURCE));

        log.info(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE + "="
                + forwardChainRdfTypeRdfsResource);

        final boolean rdfsOnly = database.getAxioms().isRdfSchema()
                && !database.getAxioms().isOwlSameAs();

        if (rdfsOnly) {
            
            this.forwardChainOwlSameAsClosure = false;
            this.forwardChainOwlSameAsProperties = false;
            this.forwardChainOwlEquivalentProperty = false;
            this.forwardChainOwlEquivalentClass = false;
            this.forwardChainOwlInverseOf = false;
            this.forwardChainOwlTransitiveProperty = false;
            
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

            this.forwardChainOwlInverseOf = Boolean
                    .parseBoolean(properties.getProperty(
                            Options.FORWARD_CHAIN_OWL_INVERSE_OF,
                            Options.DEFAULT_FORWARD_CHAIN_OWL_INVERSE_OF));
        
            log.info(Options.FORWARD_CHAIN_OWL_INVERSE_OF + "="
                    + forwardChainOwlInverseOf);

            this.forwardChainOwlTransitiveProperty = Boolean
                    .parseBoolean(properties.getProperty(
                            Options.FORWARD_CHAIN_OWL_TRANSITIVE_PROPERY,
                            Options.DEFAULT_FORWARD_CHAIN_OWL_TRANSITIVE_PROPERY));
            
            log.info(Options.FORWARD_CHAIN_OWL_TRANSITIVE_PROPERY + "="
                    + forwardChainOwlTransitiveProperty);

        }
        
        doNotAddFilter = new DoNotAddFilter(database.getVocabulary(), database
                .getAxioms(), forwardChainRdfTypeRdfsResource);

    }
    
    /**
     * The object that generates the {@link Program}s that we use to maintain
     * the closure of the database.
     * <p>
     * Note: This is lazily instantiated in order to avoid a cyclid
     * initialization dependency betweeen the {@link InferenceEngine} and the
     * {@link BaseClosure} constructors.
     */
    private BaseClosure baseClosure = null;
    
    /**
     * Set based on {@link Options#FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE}. When
     * <code>true</code> the {@link InferenceEngine} is configured to forward
     * chain and store entailments of the form
     * <code>(x rdf:type rdfs:Resource)</code>. When <code>false</code>,
     * those entailments are computed at query time by
     * {@link #backchainIterator(long, long, long)}.
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
     * {@link #backchainIterator(long, long, long)}.
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
     * Set based on {@link Options#FORWARD_CHAIN_OWL_INVERSE_OF}. When
     * <code>true</code>, we will forward chain and store those entailments.
     * When <code>false</code>, those entailments will NOT be available.
     */
    final protected boolean forwardChainOwlInverseOf;
    
    /**
     * Set based on {@link Options#FORWARD_CHAIN_OWL_TRANSITIVE_PROPERTY}. When
     * <code>true</code>, we will forward chain and store those entailments.
     * When <code>false</code>, those entailments will NOT be available.
     */
    final protected boolean forwardChainOwlTransitiveProperty;
    
    /**
     * Compute the forward closure of a focusStore against the database using
     * the algorithm selected by
     * {@link AbstractTripleStore.Options#CLOSURE_CLASS}.
     * <p>
     * Note: before calling this method with a non-<code>null</code>
     * <i>focusStore</i>, the caller SHOULD examine the statements in the
     * <i>focusStore</i> and then database. For each statement in the
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
     * {@link AbstractTripleStore#copyStatements(AbstractTripleStore, IElementFilter, boolean)}.
     * If you are loading data from some kind of resource, then see
     * {@link DataLoader} which already knows how to do this.
     * <p>
     * See {@link TruthMaintenance#assertAll(TempTripleStore)}, which first
     * handles statements already in the database, then calls this method, and
     * finally copies the remaining explicit statements in the focusStore and
     * the entailments into the database.
     * 
     * @param focusStore
     *            The data set that will be closed against the database
     *            (optional). When <code>null</code> the store will be closed
     *            against itself.
     * 
     * @return Statistics about the operation.
     */
    public ClosureStats computeClosure(AbstractTripleStore focusStore) {
        
        return computeClosure(focusStore, database.isJustify());
        
    }

    /**
     * This variant allows you to explicitly NOT generate {@link Justification}s
     * for the computed entailments. It is used by the {@link TruthMaintenance}
     * class as part of the algorithm for truth maintenance when retracting
     * statements from the database. It SHOULD NOT be used for any other purpose
     * or you may risk failing to generate justifications.
     * <p>
     * Note: While this is synchronized, there is a stronger constraint on truth
     * maintenance -- only one process can safely update the closure of the
     * database at a time. Concurrent closure updates will result in an
     * incoherent knowledge base (the closure will not be at fixed point but the
     * underlying indices will be coherent). Closure operations MUST be
     * serialized to avoid inconsistency in the knowledge base.
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
    public synchronized ClosureStats computeClosure(
            AbstractTripleStore focusStore, boolean justify) {

        if (baseClosure == null) {

            baseClosure = database.getClosureInstance();
            
        }

        final MappedProgram program = baseClosure.getProgram(
                database.getSPORelation().getNamespace(),//
                (focusStore == null ? null : focusStore.getSPORelation()
                        .getNamespace()) //
                );

        if(log.isInfoEnabled()) {
            
            log.info("\n\nforwardClosure=" + baseClosure.getClass().getName()
                    + ", program=" + program);
            
        }
        
        try {

            final long begin = System.currentTimeMillis();

            /*
             * FIXME remove IJoinNexus.RULE once we no longer need the rule to
             * generate the justifications (esp. for scale-out).
             */
            final int solutionFlags = IJoinNexus.ELEMENT//
                    | (justify ? IJoinNexus.RULE | IJoinNexus.BINDINGS : 0)//
//                  | IJoinNexus.RULE  // iff debugging.
                  ;
          
            final RuleContextEnum ruleContext = focusStore == null
            	? RuleContextEnum.DatabaseAtOnceClosure
            	: RuleContextEnum.TruthMaintenance
            	;
            
            final IJoinNexusFactory joinNexusFactory = database
                    .newJoinNexusFactory(ruleContext, ActionEnum.Insert,
                            solutionFlags, doNotAddFilter, justify,
                            false/* backchain */,
                            DefaultEvaluationPlanFactory2.INSTANCE);

            final IJoinNexus joinNexus = joinNexusFactory.newInstance(database
                    .getIndexManager());

            final long mutationCount = joinNexus.runMutation(program);

            final long elapsed = System.currentTimeMillis() - begin;

            return new ClosureStats(mutationCount, elapsed);

        } catch (Exception ex) {

            throw new RuntimeException(ex);
            
        }
        
    }
    
}

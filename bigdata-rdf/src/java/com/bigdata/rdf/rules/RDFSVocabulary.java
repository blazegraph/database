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
/*
 * Created on Oct 28, 2007
 */

package com.bigdata.rdf.rules;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.rule.ArrayBindingSet;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.NEConstant;
import com.bigdata.relation.rule.Rule;

/**
 * Resolves or defines well-known RDF values against an {@link AbstractTripleStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RDFSVocabulary {

    /**
     * Value used for a "NULL" term identifier.
     */
    public final long NULL = IRawTripleStore.NULL;
    
    final static public Logger log = Logger.getLogger(RDFSVocabulary.class);

    /**
     * The database that is the authority for the defined terms and term
     * identifiers.
     */
    final public AbstractTripleStore database;
    
    /*
     * Identifiers for well-known RDF values. 
     */
    public final IConstant<Long> rdfType;
    public final IConstant<Long> rdfProperty;
    public final IConstant<Long> rdfsSubClassOf;
    public final IConstant<Long> rdfsSubPropertyOf;
    public final IConstant<Long> rdfsDomain;
    public final IConstant<Long> rdfsRange;
    public final IConstant<Long> rdfsClass;
    public final IConstant<Long> rdfsResource;
    public final IConstant<Long> rdfsCMP;
    public final IConstant<Long> rdfsDatatype;
    public final IConstant<Long> rdfsMember;
    public final IConstant<Long> rdfsLiteral;
    
    public final IConstant<Long> owlSameAs;
    public final IConstant<Long> owlEquivalentClass;
    public final IConstant<Long> owlEquivalentProperty;

    /**
     * Resolves or defines well-known RDF values.
     * 
     * @see #rdfType and friends which are initialized by this method.
     */
    public RDFSVocabulary(AbstractTripleStore store) {

        if (store == null)
            throw new IllegalArgumentException();

        this.database = store;
        
        final BigdataValueFactory f = store.getValueFactory();
        
        final BigdataValue rdfType = f.asValue(RDF.TYPE);
        final BigdataValue rdfProperty = f.asValue(RDF.PROPERTY);
        final BigdataValue rdfsSubClassOf = f.asValue(RDFS.SUBCLASSOF);
        final BigdataValue rdfsSubPropertyOf = f.asValue(RDFS.SUBPROPERTYOF);
        final BigdataValue rdfsDomain = f.asValue(RDFS.DOMAIN);
        final BigdataValue rdfsRange = f.asValue(RDFS.RANGE);
        final BigdataValue rdfsClass = f.asValue(RDFS.CLASS);
        final BigdataValue rdfsResource = f.asValue(RDFS.RESOURCE);
        final BigdataValue rdfsCMP = f.asValue(RDFS.CONTAINERMEMBERSHIPPROPERTY);
        final BigdataValue rdfsDatatype = f.asValue(RDFS.DATATYPE);
        final BigdataValue rdfsMember = f.asValue(RDFS.MEMBER);
        final BigdataValue rdfsLiteral = f.asValue(RDFS.LITERAL);
        
        final BigdataValue owlSameAs = f.asValue(OWL.SAMEAS);
        final BigdataValue owlEquivlentClass = f.asValue(OWL.EQUIVALENTCLASS);
        final BigdataValue owlEquivlentProperty = f.asValue(OWL.EQUIVALENTPROPERTY);
        
        final BigdataValue[] terms = new BigdataValue[]{
        
                rdfType,
                rdfProperty,
                rdfsSubClassOf,
                rdfsSubPropertyOf,
                rdfsDomain,
                rdfsRange,
                rdfsClass,
                rdfsResource,
                rdfsCMP,
                rdfsDatatype,
                rdfsMember,
                rdfsLiteral,
                
                owlSameAs,
                owlEquivlentClass,
                owlEquivlentProperty
                
        };

        store.getLexiconRelation()
                .addTerms(terms, terms.length, false/* readOnly */);

        this.rdfType = new Constant<Long>(rdfType.getTermId());
        this.rdfProperty = new Constant<Long>(rdfProperty.getTermId());
        this.rdfsSubClassOf = new Constant<Long>(rdfsSubClassOf.getTermId());
        this.rdfsSubPropertyOf = new Constant<Long>(rdfsSubPropertyOf.getTermId());
        this.rdfsDomain = new Constant<Long>(rdfsDomain.getTermId());
        this.rdfsRange = new Constant<Long>(rdfsRange.getTermId());
        this.rdfsClass = new Constant<Long>(rdfsClass.getTermId());
        this.rdfsResource = new Constant<Long>(rdfsResource.getTermId());
        this.rdfsCMP = new Constant<Long>(rdfsCMP.getTermId());
        this.rdfsDatatype = new Constant<Long>(rdfsDatatype.getTermId());
        this.rdfsMember = new Constant<Long>(rdfsMember.getTermId());
        this.rdfsLiteral = new Constant<Long>(rdfsLiteral.getTermId());

        this.owlSameAs = new Constant<Long>(owlSameAs.getTermId());
        this.owlEquivalentClass = new Constant<Long>(owlEquivlentClass.getTermId());
        this.owlEquivalentProperty = new Constant<Long>(owlEquivlentProperty.getTermId());
        
    }

    /**
     * Return the {@link IProgram} used to compute the full-forward closure of
     * the database.
     * 
     * @param db
     * @param forwardChainRdfTypeRdfsResource
     * @param rdfsOnly
     * @param forwardChainOwlSameAsClosure
     * @param forwardChainOwlSameAsProperties
     * @param forwardChainOwlEquivalentProperty
     * @param forwardChainOwlEquivalentClass
     * 
     * @return
     * 
     * @todo the returned program can be cached for a given database and
     *       focusStore (or for the database if no focusStore is used).
     */
    public MappedProgram getFullClosureProgram(//
            String db,//
            String focusStore,//
            boolean forwardChainRdfTypeRdfsResource,//
            boolean rdfsOnly,//
            boolean forwardChainOwlSameAsClosure,//
            boolean forwardChainOwlSameAsProperties,
            boolean forwardChainOwlEquivalentProperty,
            boolean forwardChainOwlEquivalentClass
            ) {

        final MappedProgram program = new MappedProgram("fullForwardClosure",
                focusStore, true/* parallel */, true/* closure */);

//        if(true) {
//            
//            log.error("********* ONLY RUNNING RDFS11 *************");
//            
//            program.addStep(new RuleRdfs11(db,this));
//
//            return program;
//            
//        }
        
        program.addStep(new RuleRdf01(db,this));
        
        /*
         * Note: skipping rdf2: (?u ?a ?l) -> ( _:n rdf:type rdf:XMLLiteral),
         * where ?l is a well-typed XML Literal.
         * 
         * @todo should this be included?
         */

        program.addStep(new RuleRdfs02(db,this));
        
        program.addStep(new RuleRdfs03(db,this));
        
        if(forwardChainRdfTypeRdfsResource) {

            /*
             * Note: skipping rdfs4a (?u ?a ?x) -> (?u rdf:type rdfs:Resource)
             * 
             * Note: skipping rdfs4b (?u ?a ?v) -> (?v rdf:type rdfs:Resource)
             */

            program.addStep(new RuleRdfs04a(db,this));
            
            program.addStep(new RuleRdfs04b(db,this));
            
        }
        
        program.addStep(new RuleRdfs05(db,this));
        
        program.addStep(new RuleRdfs06(db,this));
        
        program.addStep(new RuleRdfs07(db,this));
        
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
        program.addStep(new RuleRdfs08(db,this));
        
        program.addStep(new RuleRdfs09(db,this));
        
        program.addStep(new RuleRdfs10(db,this));
        
        program.addStep(new RuleRdfs11(db,this));
        
        program.addStep(new RuleRdfs12(db,this));
        
        program.addStep(new RuleRdfs13(db,this));

        /*
         * Note: The datatype entailment rules are being skipped.
         * 
         * @todo make sure that they are back-chained or add them in here.
         */

        if(!rdfsOnly) {

            if (forwardChainOwlSameAsClosure) {

                program.addStep(new RuleOwlSameAs1(db,this));

                if (forwardChainOwlSameAsProperties) {

                    program.addStep(new RuleOwlSameAs1b(db,this));

                    program.addStep(new RuleOwlSameAs2(db,this));

                    program.addStep(new RuleOwlSameAs3(db,this));

                }

            }

            if (forwardChainOwlEquivalentProperty) {

                program.addStep(new RuleOwlEquivalentProperty(db,this));

            }

            if (forwardChainOwlEquivalentClass) {

                program.addStep(new RuleOwlEquivalentClass(db,this));

            }

        }
        
        return program;

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
     * 
     * @todo write unit test to verify the automatic program rewrites for (a)
     *       selecting the P,D,R,D,T sets; (b) for fix point closure of
     *       sub-programs; and (c) for mapping the rules across truth
     *       maintenance.
     */
    public MappedProgram getFastForwardClosureProgram(//
            String db,//
            String focusStore,//
            boolean forwardChainRdfTypeRdfsResource,//
            boolean rdfsOnly,//
            boolean forwardChainOwlSameAsClosure,//
            boolean forwardChainOwlSameAsProperties,
            boolean forwardChainOwlEquivalentProperty,
            boolean forwardChainOwlEquivalentClass
            ) {

        /*
         * Note: The steps below are numbered with regard to the paper cited in
         * the javadoc above.
         * 
         * Most steps presume that the computed entailments have been added to
         * the database (vs the temp store).
         */

        final MappedProgram program = new MappedProgram("fastForwardClosure",
                focusStore, false/* parallel */, false/* closure */);

        if (!rdfsOnly) {

            // owl:equivalentProperty
            if (forwardChainOwlEquivalentProperty) {

                program.addClosureOf(new RuleOwlEquivalentProperty(db, this));

            }

        }

        /*
         * @todo The P,D,R,C, and T collections could be utilized to generate
         * sub-programs appearing in their respective places in the overall
         * sequential execution of the fast closure program and those
         * sub-programs could generate [NAMED RESULT SETS] that were then
         * accessed by later steps in the program and auto-magically dropped
         * when the program was finished. (Currently, a Callable exists that
         * directly generates the P,D,R,C or T set and then consumes that set
         * producing the appropriate entailments - this is effecient because
         * each result is used only by a single set vs being reused by multiple
         * steps).
         */
        
        {
//            // 2. Compute P (the set of possible sub properties).
//            final Set<Long> P = getSubProperties(focusStore, database);

            // 3. (?x, P, ?y) -> (?x, rdfs:subPropertyOf, ?y)
            program.addStep(new RuleFastClosure3(db,focusStore,this));//, P));
            
        }

        // 4. RuleRdfs05 until fix point (rdfs:subPropertyOf closure).
        program.addClosureOf(new IRule[] { new RuleRdfs05(db, this) });

        // 4a. Obtain: D,R,C,T.
//        final Set<Long> D = getSubPropertiesOf(focusStore, database, rdfsDomain.get());
//        final Set<Long> R = getSubPropertiesOf(focusStore, database, rdfsRange.get());
//        final Set<Long> C = getSubPropertiesOf(focusStore, database, rdfsSubClassOf.get());
//        final Set<Long> T = getSubPropertiesOf(focusStore, database, rdfType.get());

        {
            /*
             * Note: steps 5 and 6 are executed in parallel since they have no
             * mutual dependency.
             */

            final MappedProgram subProgram = new MappedProgram("fastClosure{5 6}",
                    focusStore, true/* parallel */, false/* closure */);

            // 5. (?x, D, ?y ) -> (?x, rdfs:domain, ?y)
            subProgram.addStep(new RuleFastClosure5(db,focusStore,this));//, D));

            // 6. (?x, R, ?y ) -> (?x, rdfs:range, ?y)
            subProgram.addStep(new RuleFastClosure6(db,focusStore,this));//, R));

            program.addStep(subProgram);

        }

        if (!rdfsOnly) {

            // owl:equivalentClass
            if (forwardChainOwlEquivalentClass) {

                program.addClosureOf(new RuleOwlEquivalentClass(db, this));

            }

        }

        // 7. (?x, C, ?y ) -> (?x, rdfs:subClassOf, ?y)
        {
            program.addStep(new RuleFastClosure7(db,focusStore,this));//, C));
        }

        // 8. RuleRdfs11 until fix point (rdfs:subClassOf closure).
        program.addClosureOf(new RuleRdfs11(db, this));

        // 9. (?x, T, ?y ) -> (?x, rdf:type, ?y)
        {
            program.addStep(new RuleFastClosure9(db,focusStore,this));//, T));
        }

        // 10. RuleRdfs02
        program.addStep(new RuleRdfs02(db, this));

        /*
         * 11. special rule w/ 3-part antecedent.
         * 
         * (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:domain, ?b) ->
         * (?x, rdf:type, ?b).
         */

        program.addStep(new RuleFastClosure11(db, this));

        // 12. RuleRdfs03
        program.addStep(new RuleRdfs03(db, this));

        /*
         * 13. special rule w/ 3-part antecedent.
         * 
         * (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:range, ?b ) ->
         * (?x, rdf:type, ?b )
         */

        program.addStep(new RuleFastClosure13(db, this));

        if (forwardChainRdfTypeRdfsResource) {

            /*
             * 14-15. These steps correspond to rdfs4a and rdfs4b and generate
             * (?x rdf:type rdfs:Resource) entailments. We execute these steps
             * iff we are storing those entailments.
             */

            // 14-15. RuleRdf04
            MappedProgram subProgram = new MappedProgram("rdfs04", focusStore,
                    true/* parallel */, false/* closure */);

            subProgram.addStep(new RuleRdfs04a(db, this));

            subProgram.addStep(new RuleRdfs04b(db, this));

            program.addStep(subProgram);

        }

        // 16. RuleRdf01
        program.addStep(new RuleRdf01(db, this));

        // 17. RuleRdfs09
        program.addStep(new RuleRdfs09(db, this));

        // 18. RuleRdfs10
        program.addStep(new RuleRdfs10(db, this));

        // 19. RuleRdfs08.
        program.addStep(new RuleRdfs08(db, this));

        // 20. RuleRdfs13.
        program.addStep(new RuleRdfs13(db, this));

        // 21. RuleRdfs06.
        program.addStep(new RuleRdfs06(db, this));

        // 22. RuleRdfs07.
        program.addStep(new RuleRdfs07(db, this));

        if (!rdfsOnly) {
            
            // owl:sameAs
            if (forwardChainOwlSameAsClosure) {

                // reflexive closure over owl:sameAs.
                program.addClosureOf(new RuleOwlSameAs1(db, this));

                if (forwardChainOwlSameAsProperties) {

                    // transitive closure over owl:sameAs.
                    program.addClosureOf(new RuleOwlSameAs1b(db, this));

                    /*
                     * Apply properties.
                     * 
                     * Note: owl:sameAs2,3 should exclude matches where the
                     * predicate in the head is owl:sameAs. This case is already
                     * covered by rules owl:sameas {1, 1b}. We specialize the
                     * rules here since we also use 2 and 3 in the full forward
                     * closure where all rules are brought to fix point together
                     * and we do NOT want to make that exclusion in that case.
                     */

                    // empty binding set since we only specialize the
                    // constraints.
                    final IBindingSet noBindings = new ArrayBindingSet(0/* capacity */);
                    {

                        final Rule tmp = new RuleOwlSameAs2(db, this);

                        // the variable in the predicate position of the head of
                        // the rule.
                        final IVariable<Long> p = (IVariable<Long>) tmp
                                .getHead().get(1/* p */);

                        program.addStep(tmp.specialize(//
                                noBindings,//
                                new IConstraint[] { //
                                new NEConstant(p, owlSameAs) //
                                }//
                                ));

                    }

                    {

                        final Rule tmp = new RuleOwlSameAs3(db, this);

                        // the variable in the predicate position of the head of
                        // the rule.
                        final IVariable<Long> p = (IVariable<Long>) tmp
                                .getHead().get(1/* p */);

                        program.addStep(tmp.specialize(//
                                noBindings,//
                                new IConstraint[] { //
                                new NEConstant(p, owlSameAs) //
                                }//
                                ));

                    }

                }

            }

        }
        
        return program;

    }

}

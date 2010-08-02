package com.bigdata.rdf.rules;

import java.util.LinkedList;
import java.util.List;

import org.openrdf.model.vocabulary.OWL;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.rule.ArrayBindingSet;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.NEConstant;
import com.bigdata.relation.rule.Rule;

/**
 * Fast forward closure of the store based on <a
 * href="http://www.cs.iastate.edu/~tukw/waim05.pdf">"An approach to RDF(S)
 * Query, Manipulation and Inference on Databases" by Lu, Yu, Tu, Lin, and Zhang</a>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FastClosure extends BaseClosure {
    
    public FastClosure(AbstractTripleStore db) {
        
        super( db );
        
    }
    
    public MappedProgram getProgram(String db, String focusStore) {

        /*
         * Note: The steps below are numbered with regard to the paper cited
         * in the javadoc above.
         * 
         * Most steps presume that the computed entailments have been added
         * to the database (vs the temp store).
         */

        final MappedProgram program = new MappedProgram("fastForwardClosure",
                focusStore, false/* parallel */, false/* closure */);

        if (!rdfsOnly) {

            // owl:equivalentProperty
            if (forwardChainOwlEquivalentProperty) {

                program.addClosureOf(new RuleOwlEquivalentProperty(db,
                        vocab));

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
         * producing the appropriate entailments - vocab is efficient because
         * each result is used only by a single set vs being reused by multiple
         * steps).
         */
        
        {
//                // 2. Compute P (the set of possible sub properties).
//                final Set<IV> P = getSubProperties(focusStore, database);

            // 3. (?x, P, ?y) -> (?x, rdfs:subPropertyOf, ?y)
            program.addStep(new RuleFastClosure3(db,focusStore,vocab));//, P));
            
        }

        // 4. RuleRdfs05 until fix point (rdfs:subPropertyOf closure).
        program.addClosureOf(new IRule[] { new RuleRdfs05(db, vocab) });

        // 4a. Obtain: D,R,C,T.
//            final Set<IV> D = getSubPropertiesOf(focusStore, database, rdfsDomain.get());
//            final Set<IV> R = getSubPropertiesOf(focusStore, database, rdfsRange.get());
//            final Set<IV> C = getSubPropertiesOf(focusStore, database, rdfsSubClassOf.get());
//            final Set<IV> T = getSubPropertiesOf(focusStore, database, rdfType.get());

        {
            /*
             * Note: steps 5 and 6 are executed in parallel since they have no
             * mutual dependency.
             */

            final MappedProgram subProgram = new MappedProgram("fastClosure{5 6}",
                    focusStore, true/* parallel */, false/* closure */);

            // 5. (?x, D, ?y ) -> (?x, rdfs:domain, ?y)
            subProgram.addStep(new RuleFastClosure5(db,focusStore,vocab));//, D));

            // 6. (?x, R, ?y ) -> (?x, rdfs:range, ?y)
            subProgram.addStep(new RuleFastClosure6(db,focusStore,vocab));//, R));

            program.addStep(subProgram);

        }

        if (!rdfsOnly) {

            // owl:equivalentClass
            if (forwardChainOwlEquivalentClass) {

                program.addClosureOf(new RuleOwlEquivalentClass(db, vocab));

            }

        }

        // 7. (?x, C, ?y ) -> (?x, rdfs:subClassOf, ?y)
        {
            program.addStep(new RuleFastClosure7(db,focusStore,vocab));//, C));
        }

        // 8. RuleRdfs11 until fix point (rdfs:subClassOf closure).
        program.addClosureOf(new RuleRdfs11(db, vocab));

        // 9. (?x, T, ?y ) -> (?x, rdf:type, ?y)
        {
            program.addStep(new RuleFastClosure9(db,focusStore,vocab));//, T));
        }

        // 10. RuleRdfs02
        program.addStep(new RuleRdfs02(db, vocab));

        /*
         * 11. special rule w/ 3-part antecedent.
         * 
         * (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:domain, ?b) ->
         * (?x, rdf:type, ?b).
         */

        program.addStep(new RuleFastClosure11(db, vocab));

        // 12. RuleRdfs03
        program.addStep(new RuleRdfs03(db, vocab));

        /*
         * 13. special rule w/ 3-part antecedent.
         * 
         * (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:range, ?b ) ->
         * (?x, rdf:type, ?b )
         */

        program.addStep(new RuleFastClosure13(db, vocab));

        if (forwardChainRdfTypeRdfsResource) {

            /*
             * 14-15. These steps correspond to rdfs4a and rdfs4b and generate
             * (?x rdf:type rdfs:Resource) entailments. We execute these steps
             * iff we are storing those entailments.
             */

            // 14-15. RuleRdf04
            MappedProgram subProgram = new MappedProgram("rdfs04", focusStore,
                    true/* parallel */, false/* closure */);

            subProgram.addStep(new RuleRdfs04a(db, vocab));

            subProgram.addStep(new RuleRdfs04b(db, vocab));

            program.addStep(subProgram);

        }

        // 16. RuleRdf01
        program.addStep(new RuleRdf01(db, vocab));

        // 17. RuleRdfs09
        program.addStep(new RuleRdfs09(db, vocab));

        // 18. RuleRdfs10
        program.addStep(new RuleRdfs10(db, vocab));

        // 19. RuleRdfs08.
        program.addStep(new RuleRdfs08(db, vocab));

        // 20. RuleRdfs13.
        program.addStep(new RuleRdfs13(db, vocab));

        // 21. RuleRdfs06.
        program.addStep(new RuleRdfs06(db, vocab));

        // 22. RuleRdfs07.
        program.addStep(new RuleRdfs07(db, vocab));

        if (!rdfsOnly) {
            
            { 
                /*
                 * Combines the rules into a set and computes the closure of
                 * that set.
                 */
                
                final List<IRule> tmp = new LinkedList<IRule>();
    
                if (forwardChainOwlTransitiveProperty) {
    
                    tmp.add(new RuleOwlTransitiveProperty1(db, vocab));
    
                    tmp.add(new RuleOwlTransitiveProperty2(db, vocab));
                    
                }
    
                if (forwardChainOwlInverseOf) {
    
                    tmp.add(new RuleOwlInverseOf1(db, vocab));
    
                    tmp.add(new RuleOwlInverseOf2(db, vocab));
    
                }
    
                if (forwardChainOwlHasValue) {
                    
                    tmp.add(new RuleOwlHasValue(db, vocab));
    
                }
    
                if (!tmp.isEmpty()) {
    
                    /*
                     * Fix point whatever set of rules were selected above.
                     */
    
                    program.addClosureOf(tmp.toArray(new IRule[] {}));
    
                }
                
            }

            // owl:sameAs
            if (forwardChainOwlSameAsClosure) {

                final IConstant<IV> owlSameAs = vocab.getConstant(OWL.SAMEAS);
                
                // reflexive closure over owl:sameAs.
                program.addClosureOf(new RuleOwlSameAs1(db, vocab));

                // transitive closure over owl:sameAs.
                program.addClosureOf(new RuleOwlSameAs1b(db, vocab));

                if (forwardChainOwlSameAsProperties) {

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

                        final Rule tmp = new RuleOwlSameAs2(db, vocab);

                        // the variable in the predicate position of the head of
                        // the rule.
                        final IVariable<IV> p = (IVariable<IV>) tmp
                                .getHead().get(1/* p */);

                        program.addStep(tmp.specialize(//
                                noBindings,//
                                new IConstraint[] { //
                                new NEConstant(p, owlSameAs) //
                                }//
                                ));

                    }

                    {

                        final Rule tmp = new RuleOwlSameAs3(db, vocab);

                        // the variable in the predicate position of the head of
                        // the rule.
                        final IVariable<IV> p = (IVariable<IV>) tmp
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

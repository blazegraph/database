package com.bigdata.rdf.rules;

import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * A program that uses the fix point of the configured rules to compute the
 * forward closure of the database. Since there is no inherent order among
 * the rules in a fix point program, this program can be easily extended by
 * adding additional rules.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FullClosure extends BaseClosure {

    public FullClosure(AbstractTripleStore db) {
        
        super( db );
        
    }
    
    public MappedProgram getProgram(String db, String focusStore) {

        final MappedProgram program = new MappedProgram("fullForwardClosure",
                focusStore, true/* parallel */, true/* closure */);

        program.addStep(new RuleRdf01(db,vocab));
        
        /*
         * Note: skipping rdf2: (?u ?a ?l) -> ( _:n rdf:type rdf:XMLLiteral),
         * where ?l is a well-typed XML Literal.
         * 
         * @todo should this be included?
         */

        program.addStep(new RuleRdfs02(db,vocab));
        
        program.addStep(new RuleRdfs03(db,vocab));
        
        if(forwardChainRdfTypeRdfsResource) {

            /*
             * Note: skipping rdfs4a (?u ?a ?x) -> (?u rdf:type rdfs:Resource)
             * 
             * Note: skipping rdfs4b (?u ?a ?v) -> (?v rdf:type rdfs:Resource)
             */

            program.addStep(new RuleRdfs04a(db,vocab));
            
            program.addStep(new RuleRdfs04b(db,vocab));
            
        }
        
        program.addStep(new RuleRdfs05(db,vocab));
        
        program.addStep(new RuleRdfs06(db,vocab));
        
        program.addStep(new RuleRdfs07(db,vocab));
        
        /*
         * @todo Should we run vocab rule or backchain?
         * 
         * [MikeP] I would say generate, but you can backchain (I think?) as
         * long as you have the full closure of the type hierarchy, specifically
         * of ? type Class. If you don't have that (i.e. if you are not
         * calculating the domain and range inferences), then you'd need to
         * recursively backchain the tail too. That is why I did not do more
         * backchaining - the backchain for ? type Resource is super easy
         * because it's never longer than 1 move back.
         */
        program.addStep(new RuleRdfs08(db,vocab));
        
        program.addStep(new RuleRdfs09(db,vocab));
        
        program.addStep(new RuleRdfs10(db,vocab));
        
        program.addStep(new RuleRdfs11(db,vocab));
        
        program.addStep(new RuleRdfs12(db,vocab));
        
        program.addStep(new RuleRdfs13(db,vocab));

        /*
         * Note: The datatype entailment rules are being skipped.
         * 
         * @todo make sure that they are back-chained or add them in here.
         */

        if(!rdfsOnly) {

            if (forwardChainOwlTransitiveProperty) {
            
                program.addStep(new RuleOwlTransitiveProperty1(db,vocab));
                
                program.addStep(new RuleOwlTransitiveProperty2(db,vocab));
                
            }
            
            if (forwardChainOwlInverseOf) {
            
                program.addStep(new RuleOwlInverseOf1(db,vocab));
            
                program.addStep(new RuleOwlInverseOf2(db,vocab));
                
            }
            
            if (forwardChainOwlHasValue) {
                
                program.addStep(new RuleOwlHasValue(db, vocab));

            }
            if (forwardChainOwlSameAsClosure) {

                program.addStep(new RuleOwlSameAs1(db,vocab));

                program.addStep(new RuleOwlSameAs1b(db,vocab));

                if (forwardChainOwlSameAsProperties) {

                    program.addStep(new RuleOwlSameAs2(db,vocab));

                    program.addStep(new RuleOwlSameAs3(db,vocab));

                }

            }

            if (forwardChainOwlEquivalentProperty) {

                program.addStep(new RuleOwlEquivalentProperty(db,vocab));

            }

            if (forwardChainOwlEquivalentClass) {

                program.addStep(new RuleOwlEquivalentClass(db,vocab));

            }

        }
        
        return program;

    }
    
}
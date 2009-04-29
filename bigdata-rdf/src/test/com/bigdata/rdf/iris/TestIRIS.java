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
 * Created on Apr 28, 2009
 */

package com.bigdata.rdf.iris;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import org.deri.iris.api.IProgramOptimisation.Result;
import org.deri.iris.api.basics.IAtom;
import org.deri.iris.api.basics.ILiteral;
import org.deri.iris.api.basics.IQuery;
import org.deri.iris.api.basics.IRule;
import org.deri.iris.api.basics.ITuple;
import org.deri.iris.api.factory.IBasicFactory;
import org.deri.iris.api.factory.ITermFactory;
import org.deri.iris.api.terms.ITerm;
import org.deri.iris.basics.BasicFactory;
import org.deri.iris.optimisations.magicsets.MagicSets;
import org.deri.iris.terms.TermFactory;
import org.openrdf.model.vocabulary.RDFS;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.inf.TruthMaintenance;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rules.AbstractInferenceEngineTestCase;
import com.bigdata.rdf.rules.BaseClosure;
import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.rules.MappedProgram;
import com.bigdata.rdf.rules.RuleContextEnum;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IStep;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.DefaultEvaluationPlanFactory2;
import com.bigdata.relation.rule.eval.IEvaluationPlanFactory;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Test suite for IRIS-based truth maintenance on delete.
 *
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestIRIS extends AbstractInferenceEngineTestCase {

    final IBasicFactory BASIC = BasicFactory.getInstance();
    
    final ITermFactory TERM = TermFactory.getInstance();
    

    
    /**
     * 
     */
    public TestIRIS() {
        super();
    }

    /**
     * @param name
     */
    public TestIRIS(String name) {
        super(name);
    }

    /**
     * We are trying to eliminate the use of justification chains inside the
     * database.  These justification chains are used to determine whether or
     * not a statement is grounded by other facts in the database.  To determine 
     * this without justifications, we could use a magic sets optimization
     * against the normal inference rules using the statement to test as the
     * query.
     * 
     * @fixme variables for bigdata rules are assigned locally to the rule. will
     * this create problems for the iris program, where all the rules are mushed
     * together?
     * 
     * @fixme what do we do about the IConstraints on bigdata rules?  do they
     * get promoted to full-fledged IRIS rules in the IRIS program?
     */
    public void testRetractionWithIRIS() {
        
        
        Properties properties = getProperties();
        
        // override the default axiom model.
        properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        AbstractTripleStore store = getStore(properties);
        
        try {

            final BigdataValueFactory f = store.getValueFactory();
            final BigdataURI U = f.createURI("http://www.bigdata.com/U");
            final BigdataURI V = f.createURI("http://www.bigdata.com/V");
            final BigdataURI X = f.createURI("http://www.bigdata.com/X");
            final BigdataURI sco = f.asValue(RDFS.SUBCLASSOF);
            
            // set the stage
            // U sco V and V sco X implies U sco X
            // let's pretend all three statements were originally in the
            // database, and { U sco X } is retracted, leaving only the other
            // two.
            // we should be able to create an adorned program based on our
            // normal inference rules using the "retracted" statement as the
            // query.
            
            store.addStatement(U, sco, V);
            
            store.addStatement(V, sco, X);
            
            if (log.isInfoEnabled())
                log.info("\n\nstore:\n"
                        + store.dumpStore(store,
                                true, true, true, true));
            
            // now get the program from the inference engine
            
            final InferenceEngine inference = store.getInferenceEngine();
            
            final TruthMaintenance tm = new TruthMaintenance(inference);
            
            final TempTripleStore focusStore = tm.newTempTripleStore();

            BaseClosure closure = store.getClosureInstance();
            
            MappedProgram program = closure.getProgram(
                    store.getSPORelation().getNamespace(),
                    focusStore.getSPORelation().getNamespace()
                    );
            
            // now we convert the bigdata program into an IRIS program

            Collection<IRule> rules = new LinkedList<IRule>();
            
            convertToIRISProgram(program, rules);
            
            // then we create a query for the fact we are looking for
            
            IAtom atom = BASIC.createAtom(
                BASIC.createPredicate("stmt", 3),
                BASIC.createTuple(
                    TERM.createString(String.valueOf(U.getTermId())), 
                    TERM.createString(String.valueOf(sco.getTermId())), 
                    TERM.createString(String.valueOf(X.getTermId()))
                    )
                );
            
            IQuery query = BASIC.createQuery(
                BASIC.createLiteral(
                    true, // positive 
                    atom
                    )
                );
            
            // create the magic sets optimizer
            
            MagicSets magicSets = new MagicSets();
            
            Result result = magicSets.optimise(rules, query);
            
            for (IRule rule : result.rules) {
                
                System.err.println("rule: " + rule);
                
            }
            
            // now we take the optimized set of rules and convert it back to a
            // bigdata program
            
            MappedProgram magicProgram = program; // convertToBigdata(result.rules);
            
            // then we somehow run the magic program and see if the fact in
            // question exists in the resulting closure, if it does, then the
            // statement is supported by other facts in the database
            
            final long begin = System.currentTimeMillis();
            
            // run the query as a native rule.
            final IEvaluationPlanFactory planFactory =
                    DefaultEvaluationPlanFactory2.INSTANCE;
            
            final IJoinNexusFactory joinNexusFactory =
                    store.newJoinNexusFactory(RuleContextEnum.HighLevelQuery,
                            ActionEnum.Query, IJoinNexus.ELEMENT, null, // filter
                            false, // justify 
                            false, // backchain
                            planFactory);
            
            final IJoinNexus joinNexus =
                    joinNexusFactory.newInstance(store.getIndexManager());
            
            IChunkedOrderedIterator<ISolution> solutions = joinNexus.runQuery(magicProgram);

            while (solutions.hasNext()) {

                ISolution<ISPO> solution = solutions.next();

                System.err.println(solution.get().toString(store));

            }
            
        } catch( Exception ex ) {
            
            throw new RuntimeException(ex);
            
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }
    
    /**
     * Turn a bigdata IStep (either a program or a rule) into a set of IRIS
     * rules.  Uses recursion where needed.
     * 
     * @param step 
     *              the bigdata step
     * 
     * @param rules 
     *              the set of IRIS rules
     */
    private void convertToIRISProgram(IStep step, Collection<IRule> rules) {

        if (step.isRule()) {
            
            com.bigdata.relation.rule.IRule bigdataRule =
                (com.bigdata.relation.rule.IRule) step;

            rules.add(convertToIRISRule(bigdataRule));
            
        } else {
            
            IProgram program = (IProgram) step;
            
            Iterator<IStep> substeps = program.steps();
    
            while (substeps.hasNext()) {
                
                IStep substep = substeps.next();
                
                convertToIRISProgram(substep, rules);
                
            }

        }
        
    }
    
    /**
     * Convert a bigdata rule into an IRIS rule.
     * 
     * @param bigdataRule
     *                  the bigdata rule
     * @return
     *                  the IRIS rule
     */
    private IRule convertToIRISRule(
        com.bigdata.relation.rule.IRule bigdataRule) {
        
        ILiteral[] head = new ILiteral[1];
        
        head[0] = convertToIRISLiteral(bigdataRule.getHead());
        
        int tailCount = bigdataRule.getTailCount();
        
        ILiteral[] body = new ILiteral[tailCount];
        
        for (int i = 0; i < tailCount; i++) {
            
            body[i] = convertToIRISLiteral(bigdataRule.getTail(i));
            
        }
        
        return BASIC.createRule(
            Arrays.asList(head), 
            Arrays.asList(body)
            );
        
    }
    
    /**
     * Convert a bigdata predicate to an IRIS literal.
     * 
     * @param bigdataPred
     *                  the bigdata predicate
     * @return
     *                  the IRIS literal
     */
    private ILiteral convertToIRISLiteral(IPredicate bigdataPred) {

        ITerm[] terms = new ITerm[bigdataPred.arity()];
        
        for (int i = 0; i < terms.length; i++) {
            
            IVariableOrConstant<Long> bigdataTerm = bigdataPred.get(i);
            
            if (bigdataTerm.isConstant()) {

                terms[i] = TERM.createString(String.valueOf(bigdataTerm.get()));
                
            } else {
                
                terms[i] = TERM.createVariable(bigdataTerm.getName());
                
            }
            
        }
        
        return BASIC.createLiteral(
            true, 
            BASIC.createPredicate("stmt", terms.length),
            BASIC.createTuple(terms)
            );
        
    }
    
}

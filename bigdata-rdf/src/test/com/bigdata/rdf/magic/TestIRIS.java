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

package com.bigdata.rdf.magic;

import java.util.Iterator;
import java.util.Properties;
import org.deri.iris.api.factory.IBasicFactory;
import org.deri.iris.api.factory.IBuiltinsFactory;
import org.deri.iris.api.factory.ITermFactory;
import org.deri.iris.basics.BasicFactory;
import org.deri.iris.builtins.BuiltinsFactory;
import org.deri.iris.terms.TermFactory;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.inf.ClosureStats;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rules.AbstractInferenceEngineTestCase;
import com.bigdata.rdf.rules.BaseClosure;
import com.bigdata.rdf.rules.DoNotAddFilter;
import com.bigdata.rdf.rules.RuleContextEnum;
import com.bigdata.rdf.rules.RuleRdfs11;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IStep;
import com.bigdata.relation.rule.Program;
import com.bigdata.relation.rule.QueryOptions;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.Var;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.DefaultEvaluationPlanFactory2;
import com.bigdata.relation.rule.eval.IEvaluationPlanFactory;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Test suite for IRIS-based truth maintenance on delete.
 * <p>
 * I would use a one up counter to assign the variable names during the
 * conversion and append it to the variable names in our rules.
 * 
 * The constraints are really just additional conditions on the rule. E.g.,
 * {@link RuleRdfs11} looks like this in prolog:
 * 
 * <pre>
 * triple(U,uri(rdfs:subClassOf),X) :-
 *  triple(U,uri(rdfs:subClassOf),V),
 *  triple(V,uri(rdfs:subClassOf),X),
 *  U != V,
 *  V != X.
 * </pre>
 * 
 * The RDF values will be expressed a atoms with the follow arity: uri/1,
 * literal/3, and bnode/1.
 * 
 * <pre>
 * uri(stringValue)
 * literal(stringValue,languageCode,datatTypeUri)
 * bnode(id)
 * </pre>
 * 
 * All for the values for those atoms will be string values.
 * 
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestIRIS extends AbstractInferenceEngineTestCase {

    final IBasicFactory BASIC = BasicFactory.getInstance();
    
    final ITermFactory TERM = TermFactory.getInstance();
    
    final IBuiltinsFactory BUILTINS = BuiltinsFactory.getInstance();
    
    final org.deri.iris.api.basics.IPredicate EQUAL = BASIC.createPredicate( "EQUAL", 2 );
    
    final org.deri.iris.api.basics.IPredicate NOT_EQUAL = BASIC.createPredicate( "NOT_EQUAL", 2 );
    
    final org.deri.iris.api.basics.IPredicate TRIPLE = BASIC.createPredicate("triple", 3);
    

    
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
    public void testMagicSets() {
        
        Properties properties = getProperties();
        
        // override the default axiom model.
        properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        // override the default closure.
        properties.setProperty(Options.CLOSURE_CLASS, SimpleClosure.class.getName());
        
        AbstractTripleStore store = getStore(properties);
        
        try {

            final BigdataValueFactory f = store.getValueFactory();
            final BigdataURI U = f.createURI("http://www.bigdata.com/U");
            final BigdataURI V = f.createURI("http://www.bigdata.com/V");
            final BigdataURI W = f.createURI("http://www.bigdata.com/W");
            final BigdataURI X = f.createURI("http://www.bigdata.com/X");
            final BigdataURI Y = f.createURI("http://www.bigdata.com/Y");
            final BigdataURI Z = f.createURI("http://www.bigdata.com/Z");
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
            
            store.addStatement(V, sco, W);
            
            store.addStatement(X, sco, Y);
            
            store.addStatement(Y, sco, Z);
            
            log.info("U: " + U.getIV());
            log.info("V: " + V.getIV());
            log.info("W: " + W.getIV());
            log.info("X: " + X.getIV());
            log.info("Y: " + Y.getIV());
            log.info("Z: " + Z.getIV());
            log.info("sco: " + sco.getIV());
            
            if (log.isInfoEnabled())
                log.info("database contents:\n"
                        + store.dumpStore(store,
                                true, true, true, true));
            
            log.info("creating a TempMagicStore focus store...");
            
            final Properties tmp = store.getProperties();
            tmp.setProperty(AbstractTripleStore.Options.LEXICON, "false");

            final TempMagicStore tempStore = new TempMagicStore(
                    store.getIndexManager().getTempStore(), tmp, store);
            
            // now get the program from the inference engine
            BaseClosure closure = store.getClosureInstance();
            
            Program program = closure.getProgram(
                    store.getSPORelation().getNamespace(),
                    null
                    );
            
            log.info("bigdata program before magic sets:");
            
            Iterator<IStep> steps = program.steps();
            
            while (steps.hasNext()) {
                
                IRule rule = (IRule) steps.next();
                
                log.info(rule);
                
            }

            IRule query = new Rule(
                    "my query",
                    null, // head
                    new SPOPredicate[] {
                        new SPOPredicate(
                            new String[] {
                                store.getSPORelation().getNamespace(),
                                tempStore.getSPORelation().getNamespace()
                            },
                            new Constant<IV>(U.getIV()),
                            Var.var("p"),
                            Var.var("o"))
                    },
                    QueryOptions.NONE,
                    null // constraints
                    );
                    
            // now we take the optimized set of rules and convert it back to a
            // bigdata program
            
            log.info("converting the datalog program back to a bigdata program...");
            
            Program magicProgram =  
                IRISUtils.magicSets(program, query, store, tempStore);
            
            log.info("bigdata program after magic sets:");
            
            // log.info("bigdata program converted back from prolog program:");
            
            steps = magicProgram.steps();
            
            while (steps.hasNext()) {
                
                com.bigdata.relation.rule.IRule rule = 
                    (com.bigdata.relation.rule.IRule) steps.next();
                
                log.info(rule);
                
            }

            // then we somehow run the magic program and see if the fact in
            // question exists in the resulting closure, if it does, then the
            // statement is supported by other facts in the database
            
            log.info("executing bigdata program...");
            
            computeClosure(store, tempStore, magicProgram);
            
            log.info("done.");
            log.info("database contents\n"+store.dumpStore(store, true, true, true).toString());
            log.info("focus store contents\n"+tempStore.dumpStore(store, true, true, true).toString());

            log.info("running query...");
            
            IChunkedOrderedIterator<ISolution> solutions = 
                runQuery(store, query);
            
            int i = 0;
            while (solutions.hasNext()) {

                ISolution solution = solutions.next();

                IBindingSet b = solution.getBindingSet();
                
                log.info(b);

            }
            
        } catch( Exception ex ) {
            
            ex.printStackTrace();
            
            throw new RuntimeException(ex);
            
        } finally {
            
            store.__tearDownUnitTest();
            
        }
        
    }

    public ClosureStats computeClosure(
            AbstractTripleStore database, AbstractTripleStore focusStore, 
            Program program) {

        final boolean justify = false;
        
        final DoNotAddFilter doNotAddFilter = new DoNotAddFilter(
                database.getVocabulary(), database.getAxioms(), 
                true /*forwardChainRdfTypeRdfsResource*/);
        
        try {

            final long begin = System.currentTimeMillis();

            /*
             * FIXME remove IJoinNexus.RULE once we we can generate the
             * justifications from just the bindings and no longer need the rule
             * to generate the justifications (esp. for scale-out).
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
    
    /**
     * Run a rule based on a {@link TupleExpr} as a query.
     * 
     * @param rule
     *            The rule to execute.
     * 
     * @return The Sesame 2 iteration that visits the {@link BindingSet}s that
     *         are the results for the query.
     * 
     * @throws QueryEvaluationException
     */
    public IChunkedOrderedIterator<ISolution> runQuery(
            final AbstractTripleStore database,
            final IRule rule)
            throws QueryEvaluationException {

        log.info(rule);
        
        // run the query as a native rule.
        final IChunkedOrderedIterator<ISolution> itr1;
        try {

            final IEvaluationPlanFactory planFactory = 
                DefaultEvaluationPlanFactory2.INSTANCE;

            final IJoinNexusFactory joinNexusFactory = database
                    .newJoinNexusFactory(RuleContextEnum.HighLevelQuery,
                            ActionEnum.Query, IJoinNexus.BINDINGS,
                            null, // filter
                            false, // justify 
                            false, // backchain
                            planFactory//
                            );

            final IJoinNexus joinNexus = joinNexusFactory.newInstance(database
                    .getIndexManager());
    
            itr1 = joinNexus.runQuery(rule);

        } catch (Exception ex) {
            
            throw new QueryEvaluationException(ex);
            
        }
        
        return itr1;
        
    }
    
}

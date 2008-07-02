/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 19, 2008
 */

package com.bigdata.relation.rdf;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import com.bigdata.journal.ITx;
import com.bigdata.relation.FederationRelationLocator;
import com.bigdata.relation.IRelationLocator;
import com.bigdata.relation.IRelationName;
import com.bigdata.relation.accesspath.ChunkedArrayIterator;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.rule.AbstractRuleTestCase;
import com.bigdata.relation.rule.ArrayBindingSet;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.Var;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.DefaultEvaluationPlan;
import com.bigdata.relation.rule.eval.IEvaluationPlan;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.RuleState;
import com.bigdata.service.DataServiceIndex;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.LocalDataServiceClient;
import com.bigdata.service.LocalDataServiceClient.Options;

/**
 * Test ability to insert, update, or remove elements from a relation and the
 * ability to select the right access path given a predicate for that relation
 * and query for those elements (we have to test all this stuff together since
 * testing query requires us to have some data in the relation).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSPORelation extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestSPORelation() {
    }

    /**
     * @param name
     */
    public TestSPORelation(String name) {
        
        super(name);
        
    }

    File dataDir;
    IBigdataClient client;
    IBigdataFederation fed;
    ExecutorService service;
    TestTripleStore kb;
    final String namespace = "test.";
    IRelationName relationName;
    IRelationLocator relationLocator;

    /**
     * FIXME Also do setup using simple Journal without concurrency control
     * layer so that we can continue to compare the performance for the (more or
     * less) single-threaded case. There is enough overhead introduced by the
     * {@link DataServiceIndex} that the TempTripleStore should continue to use
     * a local Journal and therefore we can easily maintain the LocalTripleStore
     * as well.
     */
    protected void setUp() throws Exception {

        super.setUp();
        
        Properties properties = new Properties(getProperties());

        dataDir = File.createTempFile(getName(), ".tmp");
        
        dataDir.delete();
        
        dataDir.mkdirs();
        
        properties.setProperty(Options.DATA_DIR, dataDir.toString());
        
        // use a temporary store.
//        properties.setProperty(Options.BUFFER_MODE,BufferMode.Temporary.toString());

        client = new LocalDataServiceClient(properties);
        
        fed = client.connect();

        service = fed.getThreadPool();
        
        kb = new TestTripleStore(fed, namespace, ITx.UNISOLATED);

        kb.create();
        
        relationName = new SPORelationName(namespace);
        
        relationLocator = new FederationRelationLocator(
                fed, new SPORelationFactory()); 
        
        
    }

    protected void tearDown() throws Exception {

        client.getFederation().destroy();
        
        client.disconnect(true/*immediateShutdown*/);
        
        super.tearDown();
        
    }

    /**
     * Test the ability to obtain the correct {@link IAccessPath} given a
     * {@link IPredicate} and an empty {@link SPORelation}. The choice of the
     * {@link IAccessPath} is made first based on the binding pattern and only
     * ties are broken based on range counts. This allows us to test the choice
     * of the access path in the absence of any data in the {@link SPORelation}.
     * 
     * @todo There are some variable combinations that are not being tested.
     * 
     * @todo These tests assume that all access paths are available (that the
     *       {@link SPORelation} is fully indexed). This is always true for now.
     */
    public void test_ruleState() {

        /*
         * rdfs9 uses a constant in the [p] position of the for both tails and
         * the other positions are unbound, so the correct index is POS for both
         * tails.
         */
        {
        
            final IRule rule = new TestRuleRdfs9(relationName);

            final IJoinNexus joinNexus = new RDFJoinNexus(service,
                    relationLocator, ITx.READ_COMMITTED/* writeTime */,
                    ITx.READ_COMMITTED/* readTime */, false/* elementOnly */);
            
            final RuleState ruleState = new RuleState(rule, joinNexus);

            final IBindingSet bindingSet = ruleState.getJoinNexus()
                    .newBindingSet(rule);

            // (u rdfs:subClassOf x)
            assertEquals(SPOKeyOrder.POS, ruleState
                    .getAccessPath(0, bindingSet).getKeyOrder());

            // (v rdfs:subClassOf u)
            assertEquals(SPOKeyOrder.POS, ruleState
                    .getAccessPath(1, bindingSet).getKeyOrder());

        }

        /*
         * Verify that a rule with a single tail predicate that has no constants
         * will select the SPO index.
         */
        {

            final IRule rule = new Rule("testRule",
                    // head
                    new SPOPredicate(relationName, Var.var("x"), Var.var("y"),
                            Var.var("z")),
                    // tail
                    new SPOPredicate[] { new SPOPredicate(relationName, Var
                            .var("x"), Var.var("y"), Var.var("z")) },
                    // constraints
                    new IConstraint[] {});

            final IJoinNexus joinNexus = new RDFJoinNexus(service,
                    relationLocator, ITx.READ_COMMITTED/* writeTime */,
                    ITx.READ_COMMITTED/* readTime */, false/* elementOnly */);
            
            final RuleState ruleState = new RuleState(rule, joinNexus);

            final IBindingSet bindingSet = ruleState.getJoinNexus()
                    .newBindingSet(rule);

            // (x y z)
            assertEquals(SPOKeyOrder.SPO, ruleState
                    .getAccessPath(0, bindingSet).getKeyOrder());

        }

        /*
         * Verify selection of the OSP and SPO access path based on one bound
         * predicates.
         */
        {

            final IRule rule = new Rule("testRule",
                    // head
                    new SPOPredicate(relationName, Var.var("x"), Var.var("y"),
                            Var.var("z")),
                    // tail
                    new SPOPredicate[] {//
                        new SPOPredicate(relationName, new Constant<Long>(
                                    2L), Var.var("y"), Var.var("z")),//
                        new SPOPredicate(relationName, Var.var("x"), Var
                                    .var("y"), new Constant<Long>(1L))//
                        },
                    // constraints
                    new IConstraint[] {});

            final IJoinNexus joinNexus = new RDFJoinNexus(service,
                    relationLocator, ITx.UNISOLATED/* writeTime */,
                    ITx.READ_COMMITTED/* readTime */, false/* elementOnly */);
            
            final RuleState ruleState = new RuleState(rule, joinNexus);

            final IBindingSet bindingSet = ruleState.getJoinNexus()
                    .newBindingSet(rule);

            // (1L y z)
            assertEquals(SPOKeyOrder.SPO, ruleState
                    .getAccessPath(0, bindingSet).getKeyOrder());

            // (x y 1L)
            assertEquals(SPOKeyOrder.OSP, ruleState
                    .getAccessPath(1, bindingSet).getKeyOrder());

        }

    }

    /**
     * Test the ability insert data into a relation and pull back that data
     * using a variety of access paths. The test also checks the the correct
     * evaluation orders are computed based on the data actually in the relation
     * and that those evaluation orders change as we add data to the relation.
     * Finally, the test simulates how an {@link ISolution} would be computed
     * based on incremental binding of variables.
     */
    public void test_insertQuery() {

        // define some vocabulary.
        final IConstant<Long> U1 = new Constant<Long>(11L);
        final IConstant<Long> U2 = new Constant<Long>(12L);
        final IConstant<Long> V1 = new Constant<Long>(21L);
        final IConstant<Long> V2 = new Constant<Long>(22L);
        final IConstant<Long> X1 = new Constant<Long>(31L);
        //final IConstant<Long> X2 = new Constant<Long>(32L);

        // (?u,rdfs:subClassOf,?x), (?v,rdf:type,?u) -> (?v,rdf:type,?x)
        final Rule rule = new TestRuleRdfs9(relationName);

        final IJoinNexus joinNexus = new RDFJoinNexus(service, relationLocator,
                ITx.UNISOLATED/* writeTime */,
                ITx.READ_COMMITTED/* readTime */, false/* elementOnly */);

        /*
         * Note: This is the original evaluation order based on NO data in the
         * relation.
         * 
         * Note: Since both tails are 2-unbound and there is NO data for either
         * tail there is no preference in the evaluation order.
         */
        {

            final IEvaluationPlan plan = new DefaultEvaluationPlan(joinNexus,rule);
            
            log.info("original plan=" + plan);
            
        }
        
        /*
         * Obtain the access paths corresponding to each predicate in the body
         * of the rule. Each access path is parameterized by the triple pattern
         * described by the corresponding predicate in the body of the rule.
         * 
         * Note: even when using the same access paths the range counts CAN
         * differ based on what constants are bound in each predicate and on
         * what positions are variables.
         * 
         * Note: When there are shared variables the range count generally will
         * be different after those variable(s) become bound.
         */
        {

            final RuleState ruleState = new RuleState(rule, joinNexus);

            final IBindingSet bindingSet = ruleState.getJoinNexus()
                    .newBindingSet(rule);
            
            for (int i = 0; i < rule.getTailCount(); i++) {

                assertEquals(0, ruleState.getAccessPath(i, bindingSet)
                        .rangeCount(true/* exact */));

                assertEquals(0, ruleState.getAccessPath(i, bindingSet)
                        .rangeCount(false/* exact */));

            }
            
        }

        /*
         * Add some data into the store where it is visible to those access
         * paths and notice the change in the range count.
         */
        final SPORelation spoRelation = kb.getSPORelation();
        {

            final SPO[] a = new SPO[] {

                    // (u rdf:subClassOf x)
                    new SPO(U1, rdfsSubClassOf, X1, StatementEnum.Explicit),
            
                    // (v rdf:type u)
                    new SPO(V1, rdfType, U1, StatementEnum.Explicit),
                    new SPO(V2, rdfType, U2, StatementEnum.Explicit)

            };
            
            assertEquals(3,spoRelation
                    .insert(new ChunkedArrayIterator<SPO>(
                            a.length, a, null/* keyOrder */)));
            
            if (log.isInfoEnabled()) {

                log.info("KB Dump:\n"+kb.dump());
                
            }

            assertEquals(3, spoRelation.getElementCount(true/*exact*/));

        }

        /*
         * Verify range counts for the access paths for each predicate in the
         * tail. These counts reflect the data that we just wrote onto the
         * relation.
         */
        {

            // (u rdf:subClassOf x)
            assertEquals(1, spoRelation.getAccessPath(rule.getTail(0))
                    .rangeCount(false/* exact */));

            // (v rdf:type u)
            assertEquals(2, spoRelation.getAccessPath(rule.getTail(1))
                    .rangeCount(false/* exact */));
            
        }

        /*
         * Verify the evaluation plan now that one of the tail predicates is
         * more selective than the other based on their range counts (they have
         * the same #of unbound variables).
         */
        {
            
            final IEvaluationPlan plan = new DefaultEvaluationPlan(joinNexus,
                    rule);
            
            log.info("updated plan=" + plan);

            // (u rdf:subClassOf x)
            assertEquals("order", new int[] { 0, 1 }, plan.getOrder());

        }

        /*
         * Incrementally binding the variables in the rule.
         * 
         * First bind variables for (u rdf:subClassOf x) to known values from
         * the statement in the database that matches the predicate.
         */
        {
            
            final IBindingSet bindings = joinNexus.newBindingSet(rule);

            final RuleState ruleState = new RuleState(rule, joinNexus);

            ruleState.set(Var.var("u"), U1, bindings);

            ruleState.set(Var.var("x"), X1, bindings);

            assertTrue(rule.isFullyBound(0, bindings));

            /*
             * Now bind the last variable.
             */
            ruleState.set(Var.var("v"), V1, bindings);

            assertTrue(rule.isFullyBound(1, bindings));

            // emit the entailment
            final ISolution<SPO> solution = joinNexus.newSolution(rule, bindings);

            // verify the entailed statement.
            assertEquals(V1.get().longValue(), solution.get().s);
            assertEquals(rdfType.get().longValue(), solution.get().p);
            assertEquals(X1.get().longValue(), solution.get().o);

            // verify rule is reported.
            assertTrue(rule == solution.getRule());
            
            // verify correct bindings are reported.
            assertTrue(bindings.equals(solution.getBindingSet()));
            
            // verify that a copy was made of the bindings.
            assertTrue(bindings!=solution.getBindingSet());
        }
        
    }

    /**
     * A simple test of rule execution.
     * 
     * @throws Exception
     * 
     * @todo test insert. it would be especially nice if I could get some
     *       entailments from the rule and then insert them into the relation
     *       and read them back out.
     * 
     * @todo delete is not implemented for SPORelation so update the test when I
     *       re-integrate with the RDF KB module.
     * 
     */
    public void test_runRule() throws Exception {

        // define some vocabulary.
        final IConstant<Long> U1 = new Constant<Long>(11L);
        final IConstant<Long> U2 = new Constant<Long>(12L);
        final IConstant<Long> V1 = new Constant<Long>(21L);
        final IConstant<Long> V2 = new Constant<Long>(22L);
        final IConstant<Long> X1 = new Constant<Long>(31L);
        // final IConstant<Long> X2 = new Constant<Long>(32L);

        // (?u,rdfs:subClassOf,?x), (?v,rdf:type,?u) -> (?v,rdf:type,?x)
        final Rule rule = new TestRuleRdfs9(relationName);

        /*
         * Verify Query with no data in the KB. 
         */
        {

            log.info("\n\nQuery w/o data in KB\n");
            
            final IJoinNexus joinNexus = new RDFJoinNexus(service,
                    relationLocator, ITx.READ_COMMITTED/* writeTime */,
                    ITx.READ_COMMITTED/* readTime */, false/* elementOnly */);

            final IChunkedOrderedIterator<ISolution> itr = joinNexus
                    .runQuery(rule);
    
            try {

                assertFalse(itr.hasNext());

            } finally {

                itr.close();

            }

        }
        
        /*
         * Add some data into the store where it is visible to the access paths
         * in use by the rule and notice the change in the range count.
         * 
         * Note: Given the rule and the data that we add into the KB.
         * 
         * (?u,rdfs:subClassOf,?x), (?v,rdf:type,?u) -> (?v,rdf:type,?x)
         * 
         * there should be one solution:
         * 
         * (U1,rdfs:subClassOf,X1), (V1,rdf:type,U1) -> (V1,rdf:type,X1)
         * 
         * This is checked below.
         */
        final SPORelation spoRelation = kb.getSPORelation();
        {

            final SPO[] a = new SPO[] {

                    // (u rdf:subClassOf x)
                    new SPO(U1, rdfsSubClassOf, X1, StatementEnum.Explicit),
            
                    // (v rdf:type u)
                    new SPO(V1, rdfType, U1, StatementEnum.Explicit),
                    new SPO(V2, rdfType, U2, StatementEnum.Explicit)

            };
            
            assertEquals(3,spoRelation
                    .insert(new ChunkedArrayIterator<SPO>(
                            a.length, a, null/* keyOrder */)));
            
            if (log.isInfoEnabled()) {

                log.info("KB Dump:\n"+kb.dump());
                
            }

            assertEquals(3, spoRelation.getElementCount(true/*exact*/));
            
        }

        /*
         * Verify range counts for the access paths for each predicate in the
         * tail. These counts reflect the data that we just wrote onto the
         * relation.
         */
        {

            // (u rdf:subClassOf x)
            assertEquals(1, spoRelation.getAccessPath(rule.getTail(0))
                    .rangeCount(false/* exact */));

            // (v rdf:type u)
            assertEquals(2, spoRelation.getAccessPath(rule.getTail(1))
                    .rangeCount(false/* exact */));
            
        }

        /*
         * Execute the rule again (Query) and see what we get.
         */
        {

            log.info("\n\nQuery with data in KB\n");

            final IJoinNexus joinNexus = new RDFJoinNexus(service,
                    relationLocator, ITx.READ_COMMITTED/* writeTime */,
                    ITx.READ_COMMITTED/* readTime */, false/* elementOnly */);


            final IChunkedOrderedIterator<ISolution> itr = joinNexus
                    .runQuery(rule);
            
            // (U1,rdfs:subClassOf,X1), (V1,rdf:type,U1) -> (V1,rdf:type,X1)
            
            final SPO expectedSPO = new SPO(V1, rdfType, X1, StatementEnum.Inferred);
            
            final IBindingSet expectedBindingSet = new ArrayBindingSet(rule.getVariableCount());
            expectedBindingSet.set(Var.var("u"), U1);
            expectedBindingSet.set(Var.var("v"), V1);
            expectedBindingSet.set(Var.var("x"), X1);

            try {

                assertTrue(itr.hasNext());

                final ISolution solution = itr.next();
                
                if(!solution.get().equals(expectedSPO)) {
                    
                    fail("expected: "+expectedSPO+", actual="+solution.get());
                    
                }

                assertTrue(solution.getRule() == rule);

                if(!solution.getBindingSet().equals(expectedBindingSet)) {
                    
                    fail("expected="+expectedBindingSet+", actual="+solution.getBindingSet());
                    
                }

            } finally {

                itr.close();

            }

        }
        
        /*
         * Execute the rule as a mutation (insert) and then verify the mutation
         * count (1L) and the data actually written on the relation (the SPO
         * from the solution that we verified above).
         * 
         * @todo test delete.
         * 
         * @todo test fixed point.
         */
        {

            log.info("\n\nRun rules as insert operations\n");

            final IJoinNexus joinNexus = new RDFJoinNexus(service,
                    relationLocator, ITx.UNISOLATED/* writeTime */,
                    ITx.READ_COMMITTED/* readTime */, false/* elementOnly */);

            final long mutationCount = joinNexus.runMutation(ActionEnum.Insert,
                    rule);
            
            assertEquals("mutationCount", 1L, mutationCount);
            
        }
        
    }
    
}

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

package com.bigdata.join.rdf;

import java.io.File;
import java.util.Properties;

import com.bigdata.join.AbstractRuleTestCase;
import com.bigdata.join.ChunkedArrayIterator;
import com.bigdata.join.Constant;
import com.bigdata.join.IAccessPath;
import com.bigdata.join.IBindingSet;
import com.bigdata.join.IConstant;
import com.bigdata.join.IConstraint;
import com.bigdata.join.IEvaluationPlan;
import com.bigdata.join.IJoinNexus;
import com.bigdata.join.IPredicate;
import com.bigdata.join.IRelationName;
import com.bigdata.join.IRule;
import com.bigdata.join.Rule;
import com.bigdata.join.RuleState;
import com.bigdata.join.Var;
import com.bigdata.journal.ITx;
import com.bigdata.service.DataServiceIndex;
import com.bigdata.service.IBigdataClient;
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
    TestTripleStore kb;
    final String namespace = "test.";
    IRelationName relationName;

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
        
        client.connect();

        kb = new TestTripleStore(client, namespace, ITx.UNISOLATED);

        kb.create();
        
        relationName = new SPORelationName(namespace);
        
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

            final RuleState ruleState = new RuleState(rule, new SPOJoinNexus(
                    false/* elementOnly */, new SPORelationLocator(kb)));

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

            final RuleState ruleState = new RuleState(rule, new SPOJoinNexus(
                    false/* elementOnly */, new SPORelationLocator(kb)));

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

            final RuleState ruleState = new RuleState(rule, new SPOJoinNexus(
                    false/* elementOnly */, new SPORelationLocator(kb)));

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

//    /**
//     * Basic test of the ability insert data into a relation and pull back that
//     * data using an unbound query. The use of an unbound query lets the
//     * relation select whatever index it pleases and should normally select the
//     * "clustered" index for that relation. @todo write test.
//     */
//    public void test_insertQuery() {
//        
//    }
//    

    /**
     * Test the ability to choose the more selective access path, that the
     * selected path changes as predicates become bound, and that the resulting
     * entailment reflects the current variable bindings.
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

        final IJoinNexus joinNexus = new SPOJoinNexus(false/* elementOnly */,
                new SPORelationLocator(kb));

        /*
         * Note: This is the original evaluation order based on NO data in the
         * relation.
         * 
         * Note: Since both tails are 2-unbound and there is NO data for either
         * tail there is no preference in the evaluation order.
         */
        {

            final IEvaluationPlan plan = joinNexus.newEvaluationPlan(rule);
            
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
            
        }

        if (log.isInfoEnabled()) {

            log.info("KB Dump:\n"+kb.dump());
            
        }

        assertEquals(3, spoRelation.getElementCount(true/*exact*/));

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
            
            final IEvaluationPlan plan = joinNexus.newEvaluationPlan(rule);
            
            log.info("updated plan=" + plan);

            // (u rdf:subClassOf x)
            assertEquals("order", new int[] { 0, 1 }, plan.getOrder());

        }

//        /*
//         * bind variables for (u rdf:subClassOf x) to known values from the
//         * statement in the database that matches the predicate.
//         */
//
//        assertNotSame(NULL, store.getTermId(U1));
//        ruleState.set(Rule.var("u"), store.getTermId(U1));
//
//        assertNotSame(NULL, store.getTermId(X1));
//        ruleState.set(Rule.var("x"), store.getTermId(X1));
//
//        assertTrue(ruleState.isFullyBound(0));
//
//        // (v rdf:type u)
//        assertEquals(1, ruleState.getMostSelectiveAccessPathByRangeCount());
//
//        // bind the last variable.
//        assertNotSame(NULL, store.getTermId(V1));
//        ruleState.set(Rule.var("v"), store.getTermId(V1));
//
//        assertTrue(ruleState.isFullyBound(1));
//
//        // verify no access path is recommended since the rule is fully bound.
//        assertEquals(-1, ruleState.getMostSelectiveAccessPathByRangeCount());
//
//        // emit the entailment
//        ruleState.emit();
//
//        assertEquals(1, ruleState.buffer.size());
//        assertEquals(justify ? 1 : 0, ruleState.buffer.getJustificationCount());
//
//        // verify bindings on the emitted entailment.
//        SPO entailment = ruleState.buffer.get(0);
//        assertEquals(entailment.s, store.getTermId(V1));
//        assertEquals(entailment.p, store.getTermId(URIImpl.RDF_TYPE));
//        assertEquals(entailment.o, store.getTermId(X1));
//
//        // @todo verify the full bindings, not just the entailed SPOs.

    }
    
}

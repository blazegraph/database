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
/*
 * Created on Oct 22, 2007
 */

package com.bigdata.rdf.inf;

import com.bigdata.rdf.inf.Rule.State;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite for {@link AbstractRuleFastClosure_11_13}.
 * 
 * @see RuleFastClosure11
 * @see RuleFastClosure13
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRuleFastClosure_11_13 extends AbstractInferenceEngineTestCase {

    /**
     * 
     */
    public TestRuleFastClosure_11_13() {
    }

    /**
     * @param name
     */
    public TestRuleFastClosure_11_13(String name) {
        super(name);
    }

    /**
     * Tests {@link RuleFastClosure11} with the minimum data required to compute
     * a single entailment.
     */
    public void test_RuleFastForwardClosure11() {
        
        AbstractTripleStore store = getStore();

        try {

            final long a = store.addTerm(new _URI("http://www.bigdata.com/a"));
            final long b = store.addTerm(new _URI("http://www.bigdata.com/b"));
            final long y = store.addTerm(new _URI("http://www.bigdata.com/y"));
            final long x = store.addTerm(new _URI("http://www.bigdata.com/x"));
            final long z = store.addTerm(new _URI("http://www.bigdata.com/z"));

            InferenceEngine inf = new InferenceEngine(store);

            // told:
            {

                SPO[] told = new SPO[] {
                        new SPO(x, y, z, StatementEnum.Explicit),
                        new SPO(y, inf.rdfsSubPropertyOf.id, a,
                                StatementEnum.Explicit),
                        new SPO(a, inf.rdfsDomain.id, b, StatementEnum.Explicit) };

                store.addStatements(told, told.length);

            }

            // entails:
            // store.addStatement(x, inf.rdfType.id, b);

            store.commit();

            store.dumpStore();

            /*
             * (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:domain, ?b) ->
             * (?x, rdf:type, ?b).
             */
            RuleFastClosure11 rule = new RuleFastClosure11(inf);

            /*
             * Test run the rule.
             */

            SPOBuffer buffer = new SPOBuffer(store, null/* filter */,
                    100/* capacity */, false/* distinct */, inf.isJustified()/* justifications */);

            State state = rule.newState(inf.isJustified(), store, buffer);
            
            rule.apply(state);

            assertEquals(1, state.stats.numComputed);

            assertEquals(1, buffer.size());

            System.err.println("entailment: " + buffer.get(0).toString(store));

            assertEquals(new SPO(x, inf.rdfType.id, b, StatementEnum.Inferred),
                    buffer.get(0));

            buffer.flush();

            // check entailments.
            assertTrue(store.containsStatement(x, inf.rdfType.id, b));

            store.commit();

        } finally {

            store.closeAndDelete();

        }
        
    }
    
    /**
     * Tests {@link RuleFastClosure13} with the minimum data required to compute
     * a single entailment.
     */
    public void test_RuleFastForwardClosure13() {
        
        AbstractTripleStore store = getStore();

        try {

            final long a = store.addTerm(new _URI("http://www.bigdata.com/a"));
            final long b = store.addTerm(new _URI("http://www.bigdata.com/b"));
            final long y = store.addTerm(new _URI("http://www.bigdata.com/y"));
            final long x = store.addTerm(new _URI("http://www.bigdata.com/x"));
            final long z = store.addTerm(new _URI("http://www.bigdata.com/z"));

            InferenceEngine inf = new InferenceEngine(store);

            // told:
            {

                SPO[] told = new SPO[] {
                        new SPO(x, y, z, StatementEnum.Explicit),
                        new SPO(y, inf.rdfsSubPropertyOf.id, a,
                                StatementEnum.Explicit),
                        new SPO(a, inf.rdfsRange.id, b, StatementEnum.Explicit) };

                store.addStatements(told, told.length);

            }

            // entails:
            // store.addStatement(z, inf.rdfType.id, b);

            store.commit();

            store.dumpStore();

            /*
             * (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:domain, ?b) ->
             * (?z, rdf:type, ?b).
             */
            RuleFastClosure13 rule = new RuleFastClosure13(inf);

            /*
             * Test run the rule.
             */

            SPOBuffer buffer = new SPOBuffer(store, null/* filter */,
                    100/* capacity */, false/* distinct */, inf.isJustified()/* justifications */);

            State state = rule.newState(inf.isJustified(), store, buffer);
            
            rule.apply(state);

            assertEquals(1, state.stats.numComputed);

            assertEquals(1, buffer.size());

            System.err.println("entailment: " + buffer.get(0).toString(store));

            assertEquals(new SPO(z, inf.rdfType.id, b, StatementEnum.Inferred),
                    buffer.get(0));

            buffer.flush();

            // check entailments.
            assertTrue(store.containsStatement(z, inf.rdfType.id, b));

            store.commit();

        } finally {

            store.closeAndDelete();

        }
        
    }
    
}

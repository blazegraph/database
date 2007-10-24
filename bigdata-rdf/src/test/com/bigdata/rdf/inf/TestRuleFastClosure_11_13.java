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

import com.bigdata.rdf.inf.InferenceEngine.AbstractRuleFastClosure_11_13;
import com.bigdata.rdf.inf.InferenceEngine.RuleFastClosure11;
import com.bigdata.rdf.inf.InferenceEngine.RuleFastClosure13;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite for {@link AbstractRuleFastClosure_11_13}.
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
     * <P>
     * Note: The logic {@link RuleFastClosure13} 100% shared by the same base
     * class as {@link RuleFastClosure11}.
     */
    public void test_RuleFastForwardClosure11() {
        
        AbstractTripleStore store = getStore();
                
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
                    new SPO(y, inf.rdfsSubPropertyOf.id, a, StatementEnum.Explicit),
                    new SPO(a, inf.rdfsDomain.id, b, StatementEnum.Explicit)
            };

            store.addStatements(told, told.length);
            
        }
        
        // entails:
        // store.addStatement(x, inf.rdfType.id, b);
        
        store.commit();
        
        store.dumpStore();
        
        /*
         * (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:domain, ?b)
         * -> (?x, rdf:type, ?b).
         */
        AbstractRuleFastClosure_11_13 rule = new RuleFastClosure11(inf, inf
                .nextVar(), inf.nextVar(), inf.nextVar(), inf.nextVar(), inf
                .nextVar());
        
        /*
         * Match: (?y, rdfs:subPropertyOf, ?a)
         */
        
        SPO[] stmts1 = rule.getStmts1();
        
        assertEquals(1,stmts1.length);
        
        SPO stmt1 = new SPO(y, inf.rdfsSubPropertyOf.id, a,
                StatementEnum.Explicit);
        
        assertEquals(stmt1, stmts1[0]);
        
        /*
         * Match: (?a, propertyId, ?b) where ?a is stmt1.o and propertyId is the
         * value specified to the rule ctor (rdfs:Domain).
         */
        
        SPO[] stmts2 = rule.getStmts2(stmt1);
        
        assertEquals(1, stmts2.length);
        
        assertEquals(new SPO(a, inf.rdfsDomain.id, b, StatementEnum.Explicit),
                stmts2[0]);
        
        /*
         * Match: (?x, ?y, ?z) with ?y bound to stmt1.s.
         */
        SPO[] stmts3 = rule.getStmts3(stmt1);
        assertEquals(1,stmts3.length);
        assertEquals(new SPO(x, y, z, StatementEnum.Explicit),stmts3[0]);
        
        /*
         * Test run the rule.
         */
        
        SPOBuffer buffer = new SPOBuffer(store, null/* filter */,
                100/* capacity */, false/* distinct */, inf.isJustified()/* justifications */);
        
        RuleStats stats = rule.apply(new RuleStats(), buffer);

        assertEquals(1,stats.numComputed);
        
        assertEquals(1,buffer.size());

        System.err.println("entailment: "+buffer.get(0).toString(store));

        assertEquals(new SPO(x, inf.rdfType.id, b, StatementEnum.Inferred),
                buffer.get(0));
        
        buffer.flush();
        
        // check entailments.
        assertTrue(store.containsStatement(x, inf.rdfType.id, b));

        store.commit();
        
        store.closeAndDelete();
        
    }
    
}

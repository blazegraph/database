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
 * Created on Oct 26, 2007
 */

package com.bigdata.rdf.inf;

import java.util.Set;

import com.bigdata.rdf.inf.Rule.Var;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite for basic {@link Rule} mechanisms.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRule extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRule() {
        super();
    }

    /**
     * @param name
     */
    public TestRule(String name) {
        super(name);
    }

    /**
     * Test the singleton factory for {@link Var}s.
     */
    public void test_variableSingletonFactory() {
        
        Var u = Rule.var("u");

        // same instance.
        assertTrue(u == Rule.var("u"));
        
        // different instance.
        assertTrue(u != Rule.var("x"));
        
    }
    
    /**
     * Verify construction of a simple rule.
     */
    public void test_ctor() {
        
        AbstractTripleStore store = getStore();
        
        try {
        
            InferenceEngine inf = new InferenceEngine(store);
        
            Var u = Rule.var("u");
            
            Triple head = new Triple(u,inf.rdfsSubClassOf,inf.rdfsResource);
            
            Pred[] body = new Pred[] { new Triple(u, inf.rdfType, inf.rdfsClass) };
            
            Rule r = new MyRule(inf, head, body);

            // write out the rule on the console.
            System.err.println(r.toString());

            // check bindings -- should be Ok.
            assertTrue(r.checkBindings());
            
            // verify you can overwrite a variable in the tail.
            r.set(u, 1);
            assertTrue(r.checkBindings()); // no complaints.
            r.resetBindings(); // restore the bindings.
            assertTrue(r.checkBindings()); // verify no complaints.

            /*
             * verify no binding for u.
             */
            assertEquals(NULL,r.get(u));
            assertEquals(NULL,r.get(Rule.var("u")));
            
            try {
                r.get(Rule.var("v"));
                fail("Expecting: "+IllegalArgumentException.class);
            } catch(IllegalArgumentException ex) {
                log.info("Ignoring expected exception: "+ex);
            }
            
            // set a binding and check things out.
            r.resetBindings(); // clean slate.
            assertEquals(NULL,r.get(u)); // no binding.
            assertEquals(NULL,r.get(Rule.var("u"))); // no binding.
            r.set(u, inf.rdfsClass.id);
            assertTrue(r.checkBindings()); // verify we did not overwrite constants.
            // write on the console.
            System.err.println(r.toString(true));
            // verify [u] is now bound.
            assertEquals(inf.rdfsClass.id,r.get(u));
            assertEquals(inf.rdfsClass.id,r.get(Rule.var("u")));
            // verify [u] is now bound.
            assertEquals(inf.rdfsClass.id,r.get(u));
            assertEquals(inf.rdfsClass.id,r.get(Rule.var("u")));

            /*
             * Compute the shared variables.
             */
            {

                Set<Var> shared = r.getSharedVars(head, body[0]);
                
                System.err.println("shared: "+shared);
                
                assertEquals(1,shared.size());
                
                assertTrue(shared.contains(u));
                
                assertTrue(u.equals(u));
                assertTrue(u.equals(Rule.var("u")));
                assertFalse(u.equals(Rule.var("v")));
                assertTrue(Rule.var("u").equals(u));
                assertFalse(u.equals(Rule.var("v")));
                
                assertEquals(u.hashCode(), u.hashCode() );
                assertEquals(u.hashCode(), Rule.var("u").hashCode() );
                
                assertFalse(shared.contains(Rule.var("x")));
                assertTrue(shared.contains(Rule.var("u")));
                
            }
            
        } finally {
            
            store.closeAndDelete();
            
        }

    }
    
    private static class MyRule extends Rule {

        public MyRule(InferenceEngine inf, Triple head, Pred[] body) {

            super(inf, head, body);

        }

        public RuleStats apply(RuleStats stats, SPOBuffer buffer) {

            return null;
            
        }

    }

}

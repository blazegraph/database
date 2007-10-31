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
 * Created on Oct 25, 2007
 */

package com.bigdata.rdf.inf;

import java.util.HashSet;
import java.util.Set;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.vocabulary.RDFS;

import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite for {@link AbstractRuleFastClosure_3_5_6_7_9}.
 * 
 * @see RuleFastClosure3
 * @see RuleFastClosure5
 * @see RuleFastClosure6
 * @see RuleFastClosure7
 * @see RuleFastClosure9
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRuleFastClosure_3_5_6_7_9 extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRuleFastClosure_3_5_6_7_9() {
        super();
    }

    /**
     * @param name
     */
    public TestRuleFastClosure_3_5_6_7_9(String name) {
        super(name);
    }

    /**
     * Unit test for
     * {@link InferenceEngine#getSubProperties(AbstractTripleStore)}, which is
     * used to setup the pre-conditions for {@link RuleFastClosure3}.
     */
    public void test_getSubProperties() {
       
        URI A = new _URI("http://www.foo.org/A");
        URI B = new _URI("http://www.foo.org/B");
        URI C = new _URI("http://www.foo.org/C");

        URI rdfsSubPropertyOf = new _URI(RDFS.SUBPROPERTYOF);

        AbstractTripleStore store = getStore();
        
        store.addStatement(A, rdfsSubPropertyOf, rdfsSubPropertyOf);
        store.addStatement(B, rdfsSubPropertyOf, A);

        assertTrue(store.hasStatement(A, rdfsSubPropertyOf, rdfsSubPropertyOf));
        assertTrue(store.hasStatement(B, rdfsSubPropertyOf, A));
        
        InferenceEngine inf = new InferenceEngine(store);

        Set<Long> subProperties = inf.getSubProperties(null/*focusStore*/,store);
        
        assertTrue(subProperties.contains(store.getTermId(rdfsSubPropertyOf)));
        assertTrue(subProperties.contains(store.getTermId(A)));
        assertTrue(subProperties.contains(store.getTermId(B)));

        assertEquals(3,subProperties.size());

        store.addStatement(C, A, A);
        
        assertTrue(store.hasStatement(C, A, A));

        subProperties = inf.getSubProperties(null/*focusStore*/,store);
        
        assertTrue(subProperties.contains(store.getTermId(rdfsSubPropertyOf)));
        assertTrue(subProperties.contains(store.getTermId(A)));
        assertTrue(subProperties.contains(store.getTermId(B)));
        assertTrue(subProperties.contains(store.getTermId(C)));

        assertEquals(4,subProperties.size());

        store.closeAndDelete();
        
    }
    
    /**
     * Unit test of {@link RuleFastClosure6} where the data allow the rule to
     * fire exactly, where the predicate is <code>rdfs:Range</code> and once
     * where the predicate is an
     * <code>rdfs:subPropertyOf</code> <code>rdfs:Range</code>, and tests
     * that the rule correctly filters out a possible entailment that would
     * simply conclude its own support.
     * 
     * <pre>
     *      (?x, P, ?y) -&gt; (?x, propertyId, ?y)
     * </pre>
     * 
     * where <i>propertyId</i> is rdfs:Range
     */
    public void test_rule() {
        
        AbstractTripleStore store = getStore();
        
        try {
            
            StatementBuffer buffer = new StatementBuffer(store,100/*capacity*/,true/*distinct*/);

            URI A = new URIImpl("http://www.foo.org/A");
            URI B = new URIImpl("http://www.foo.org/B");
            URI C = new URIImpl("http://www.foo.org/C");
            URI D = new URIImpl("http://www.foo.org/D");
            
            URI MyRange = new URIImpl("http://www.foo.org/MyRange");

            buffer.add(A, URIImpl.RDFS_RANGE, B);
            
            buffer.add(C, MyRange, D);
            
            // write on the store.
            buffer.flush();
            
            // verify the database.
            
            assertTrue(store.hasStatement(A,URIImpl.RDFS_RANGE,B));
            
            assertTrue(store.hasStatement(C,MyRange,D));

            assertEquals(2,store.getStatementCount());
            
            /*
             * Setup the closure of the rdfs:subPropertyOf for rdfs:Range.
             * 
             * Note: This includes both rdfs:Range and the MyRange URI. The
             * latter is not declared by the ontology to be a subPropertyOf
             * rdfs:Range since that is not required by this test, but we are
             * treating it as such for the purpose of this test.
             */

            Set<Long> R = new HashSet<Long>();
            
            R.add(store.getTermId(URIImpl.RDFS_RANGE));
            
            R.add(store.getTermId(MyRange));

            /*
             * setup the rule execution.
             */
            
            InferenceEngine inf = new InferenceEngine(store);
            
            Rule rule = new RuleFastClosure6(inf,R);
            
            applyRule(inf, rule, 1/*numComputed*/);

            // told.
            
            assertTrue(store.hasStatement(A, URIImpl.RDFS_RANGE, B));

            assertTrue(store.hasStatement(C, MyRange, D));

            /*
             * entailed
             * 
             * Note: The 2nd possible entailment is (A rdfs:Range B), which is
             * already an explicit statement in the database. The rule refuses
             * to consider triple patterns where the predicate is the same as
             * the predicate on the entailment since the support would then
             * entail itself.
             */

            assertTrue(store.hasStatement(C, URIImpl.RDFS_RANGE, D));
            
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }
    
}

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
 * Created on Apr 18, 2007
 */

package com.bigdata.rdf.inf;

import org.openrdf.model.URI;
import org.openrdf.vocabulary.RDF;

import com.bigdata.rdf.model.OptimizedValueFactory._URI;

/**
 * Test suite for {@link RuleRdf01}.
 * 
 * <pre>
 *   triple(?v rdf:type rdf:Property) :-
 *      triple( ?u ?v ?x ).
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRuleRdf01 extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRuleRdf01() {
    }

    /**
     * @param name
     */
    public TestRuleRdf01(String name) {
        super(name);
    }

    /**
     * Basic of rule semantics.
     */
    public void test_rdf01() {
        
        URI A = new _URI("http://www.foo.org/A");
        URI B = new _URI("http://www.foo.org/B");
        URI C = new _URI("http://www.foo.org/C");

        URI rdfType = new _URI(RDF.TYPE);
        URI rdfProperty = new _URI(RDF.PROPERTY);

        store.addStatement(A, B, C);

        assertTrue(store.containsStatement(A, B, C));
        assertFalse(store.containsStatement(B, rdfType, rdfProperty ));

        applyRule(store.rdf1, 1/* numComputed */, 1/* numCopied */);
        
        /*
         * validate the state of the primary store.
         */
        assertTrue(store.containsStatement(A, B, C));
        assertTrue(store.containsStatement(B, rdfType, rdfProperty ));
        
    }

    /**
     * Test that can be used to verify that we are doing an efficient scan for
     * the distinct predicates (distinct key prefix scan).
     */
    public void test_rdf01_distinctPrefixScan() {
        
        URI A = new _URI("http://www.foo.org/A");
        URI B = new _URI("http://www.foo.org/B");
        URI C = new _URI("http://www.foo.org/C");
        URI D = new _URI("http://www.foo.org/D");
        URI E = new _URI("http://www.foo.org/E");

        URI rdfType = new _URI(RDF.TYPE);
        URI rdfProperty = new _URI(RDF.PROPERTY);

        /*
         * Three statements that will trigger the rule, but two statements share
         * the same predicate. When it does the minimum amount of work, the rule
         * will fire for each distinct predicate in the KB -- for this KB that
         * is only twice.
         */
        store.addStatement(A, B, C);
        store.addStatement(C, B, D);
        store.addStatement(A, E, C);

        assertTrue(store.containsStatement(A, B, C));
        assertTrue(store.containsStatement(C, B, D));
        assertTrue(store.containsStatement(A, E, C));
        assertFalse(store.containsStatement(B, rdfType, rdfProperty ));
        assertFalse(store.containsStatement(E, rdfType, rdfProperty ));

        applyRule(store.rdf1, 2/* numComputed */, 2/* numCopied */);
        
        /*
         * validate the state of the primary store.
         */
        assertTrue(store.containsStatement(A, B, C));
        assertTrue(store.containsStatement(C, B, D));
        assertTrue(store.containsStatement(A, E, C));
        assertTrue(store.containsStatement(B, rdfType, rdfProperty ));
        assertTrue(store.containsStatement(E, rdfType, rdfProperty ));
     
    }
    
}

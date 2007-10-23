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
 * Created on Apr 13, 2007
 */

package com.bigdata.rdf.inf;

import org.openrdf.model.URI;
import org.openrdf.vocabulary.RDFS;

import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * @see RuleRdfs11
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRuleRdfs11 extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRuleRdfs11() {
    }

    /**
     * @param name
     */
    public TestRuleRdfs11(String name) {
        super(name);
    }

    /**
     * Simple test verifies inference of a subclassof entailment.
     */
    public void test_rdfs11() {

        AbstractTripleStore store = getStore();
        
        InferenceEngine inf = new InferenceEngine(store);
        
        URI A = new _URI("http://www.foo.org/A");
        URI B = new _URI("http://www.foo.org/B");
        URI C = new _URI("http://www.foo.org/C");

        URI rdfsSubClassOf = new _URI(RDFS.SUBCLASSOF);

        store.addStatement(A, rdfsSubClassOf, B);
        store.addStatement(B, rdfsSubClassOf, C);

        assertTrue(store.containsStatement(A, rdfsSubClassOf, B));
        assertTrue(store.containsStatement(B, rdfsSubClassOf, C));
        assertFalse(store.containsStatement(A, rdfsSubClassOf, C));

        applyRule(inf.rdfs11, 1/* numComputed */);
        
        /*
         * validate the state of the primary store.
         */
        assertTrue(store.containsStatement(A, rdfsSubClassOf, B));
        assertTrue(store.containsStatement(B, rdfsSubClassOf, C));
        assertTrue(store.containsStatement(A, rdfsSubClassOf, C));

        store.closeAndDelete();
        
    }
    
}

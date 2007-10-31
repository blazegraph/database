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

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;

import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test for {@link RuleRdfs03}. Also see {@link TestRuleRdfs07}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRuleRdfs03 extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRuleRdfs03() {
        super();
    }

    /**
     * @param name
     */
    public TestRuleRdfs03(String name) {
        super(name);
    }
    
    /**
     * Literals may not appear in the subject position, but an rdfs4b entailment
     * can put them there unless you explicitly filter it out.
     * <P>
     * Note: {@link RuleRdfs04b} is the other way that literals can be entailed
     * into the subject position.
     */
    public void test_rdfs3_filterLiterals() {
        
        AbstractTripleStore store = getStore();

        try {
        
            InferenceEngine inf = new InferenceEngine(getProperties(),store);

            URI A = new URIImpl("http://www.foo.org/A");
            URI X = new URIImpl("http://www.foo.org/X");
            URI U = new URIImpl("http://www.foo.org/U");
            Literal V1 = new LiteralImpl("V1"); // a literal.
            URI V2 = new URIImpl("http://www.foo.org/V2"); // not a literal.
            URI rdfRange = new URIImpl(RDFS.RANGE);
            URI rdfType = new URIImpl(RDF.TYPE);

            store.addStatement(A, rdfRange, X);
            store.addStatement(U, A, V1);
            store.addStatement(U, A, V2);

            assertTrue(store.hasStatement(A, rdfRange, X));
            assertTrue(store.hasStatement(U, A, V1));
            assertTrue(store.hasStatement(U, A, V2));
            assertEquals(3,store.getStatementCount());

            /*
             * Note: The rule computes the entailment but it gets whacked by the
             * DoNotAddFilter on the InferenceEngine, so it is counted here but
             * does not show in the database.
             */
            applyRule(inf,inf.rdfs3, 2/* numComputed */);

            /*
             * validate the state of the primary store.
             */
            assertTrue(store.hasStatement(A, rdfRange, X));
            assertTrue(store.hasStatement(U, A, V1));
            assertTrue(store.hasStatement(U, A, V2));
            assertTrue(store.hasStatement(V2, rdfType, X));
            assertEquals(4,store.getStatementCount());

        } finally {

            store.closeAndDelete();

        }

    }

    /**
     * 
     */
    public void test_rdfs3_01() {
        
        AbstractTripleStore store = getStore();

        try {
        
            InferenceEngine inf = new InferenceEngine(getProperties(),store);

            URI A = new URIImpl("http://www.foo.org/A");
            URI B = new URIImpl("http://www.foo.org/B");
            URI rdfsRange = new URIImpl(RDFS.RANGE);
            URI rdfsClass= new URIImpl(RDFS.CLASS);
            URI rdfType = new URIImpl(RDF.TYPE);

            store.addStatement(A, rdfType, B);
            store.addStatement(rdfType, rdfsRange, rdfsClass);

            assertTrue(store.hasStatement(A, rdfType, B));
            assertTrue(store.hasStatement(rdfType, rdfsRange, rdfsClass));
            assertEquals(2,store.getStatementCount());
            
            applyRule(inf,inf.rdfs3, -1/* numComputed */);

            /*
             * validate the state of the primary store.
             */
            assertTrue(store.hasStatement(A, rdfType, B));
            assertTrue(store.hasStatement(rdfType, rdfsRange, rdfsClass));
            assertTrue(store.hasStatement(B, rdfType, rdfsClass));
            assertEquals(3,store.getStatementCount());

        } finally {

            store.closeAndDelete();

        }

    }

}

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

import java.util.Properties;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;

import com.bigdata.rdf.inf.InferenceEngine.Options;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite for {@link RuleRdfs04a} and {@link RuleRdfs04b}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRuleRdfs04 extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRuleRdfs04() {
        super();
    }

    /**
     * @param name
     */
    public TestRuleRdfs04(String name) {
        super(name);
    }

    /**
     * Extended to explicitly turn on
     * {@link Options#FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE} for testing
     * {@link RuleRdfs04}.
     */
    public Properties getProperties() {

        Properties properties = new Properties(super.getProperties());
    
        properties.setProperty(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE, "true");
        
        return properties;
        
    }
    
    /**
     * Test of the basic semantics.
     */
    public void test_rdfs4a() {
        
        AbstractTripleStore store = getStore();

        try {
        
            InferenceEngine inf = new InferenceEngine(getProperties(),store);

            URI U = new URIImpl("http://www.foo.org/U");
            URI A = new URIImpl("http://www.foo.org/A");
            URI X = new URIImpl("http://www.foo.org/X");
            URI rdfType = new URIImpl(RDF.TYPE);
            URI rdfsResource = new URIImpl(RDFS.RESOURCE);

            store.addStatement(U, A, X);

            assertTrue(store.containsStatement(U, A, X));
            assertEquals(1,store.getStatementCount());

            applyRule(inf,inf.rdfs4a, 1/* numComputed */);

            /*
             * validate the state of the primary store.
             * 
             * Note: There is no entailment for (A rdf:type rdfsResource) since
             * it was not used in either a subject or object position.
             */

            assertTrue(store.containsStatement(U, A, X));
            assertTrue(store.containsStatement(U, rdfType, rdfsResource));
            assertFalse(store.containsStatement(X, rdfType, rdfsResource));
            assertEquals(2,store.getStatementCount());

        } finally {

            store.closeAndDelete();

        }

    }
    
    /**
     * Test of the basic semantics.
     */
    public void test_rdfs4b() {
        
        AbstractTripleStore store = getStore();

        try {
        
            InferenceEngine inf = new InferenceEngine(getProperties(),store);

            URI U = new URIImpl("http://www.foo.org/U");
            URI A = new URIImpl("http://www.foo.org/A");
            URI V = new URIImpl("http://www.foo.org/V");
            URI rdfType = new URIImpl(RDF.TYPE);
            URI rdfsResource = new URIImpl(RDFS.RESOURCE);

            store.addStatement(U, A, V);

            assertTrue(store.containsStatement(U, A, V));
            assertEquals(1,store.getStatementCount());

            applyRule(inf,inf.rdfs4b, 1/* numComputed */);

            /*
             * validate the state of the primary store.
             * 
             * Note: There is no entailment for (A rdf:type rdfsResource) since
             * it was not used in either a subject or object position.
             */
            
            assertTrue(store.containsStatement(U, A, V));
            assertFalse(store.containsStatement(U, rdfType, rdfsResource));
            assertTrue(store.containsStatement(V, rdfType, rdfsResource));
            assertEquals(2,store.getStatementCount());

        } finally {

            store.closeAndDelete();

        }

    }
    
    /**
     * Literals may not appear in the subject position, but an rdfs4b entailment
     * can put them there unless you explicitly filter it out.
     * <P>
     * Note: {@link RuleRdfs03} is the other way that literals can be entailed
     * into the subject position.
     */
    public void test_rdfs4b_filterLiterals() {
        
        AbstractTripleStore store = getStore();

        try {
        
            InferenceEngine inf = new InferenceEngine(getProperties(),store);

            URI A = new URIImpl("http://www.foo.org/A");
            Literal C = new LiteralImpl("C");
            URI rdfType = new URIImpl(RDF.TYPE);
            URI rdfsResource = new URIImpl(RDFS.RESOURCE);

            store.addStatement(A, rdfType, C);

            assertTrue(store.containsStatement(A, rdfType, C));
            assertEquals(1,store.getStatementCount());

            /*
             * Note: The rule computes the entailment but it gets whacked by the
             * DoNotAddFilter on the InferenceEngine, so it is counted here but
             * does not show in the database.
             */
            applyRule(inf,inf.rdfs4b, 1/* numComputed */);

            /*
             * validate the state of the primary store - there is no entailment
             * for (A rdf:type rdfs:Resource) since that would allow a literal
             * into the subject position. 
             */
            
            assertTrue(store.containsStatement(A, rdfType, C));
            assertFalse(store.containsStatement(A, rdfType, rdfsResource));
            assertEquals(1,store.getStatementCount());

        } finally {

            store.closeAndDelete();

        }

    }

}

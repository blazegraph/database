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
 * Test suite for {@link RuleRdfs04}.
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
    public void test_rdfs4() {
        
        AbstractTripleStore store = getStore();

        try {
        
            InferenceEngine inf = new InferenceEngine(getProperties(),store);

            URI A = new URIImpl("http://www.foo.org/A");
            URI B = new URIImpl("http://www.foo.org/B");
            URI C = new URIImpl("http://www.foo.org/C");
            URI rdfType = new URIImpl(RDF.TYPE);
            URI rdfsResource = new URIImpl(RDFS.RESOURCE);

            store.addStatement(A, B, C);

            assertTrue(store.containsStatement(A, B, C));
            assertEquals(1,store.getStatementCount());

            applyRule(inf.rdfs4, 2/* numComputed */);

            /*
             * validate the state of the primary store.
             * 
             * Note: There is no entailment for (B rdf:type rdfsResource) since
             * it was not used in either a subject or object position.
             */
            assertTrue(store.containsStatement(A, B, C));
            assertTrue(store.containsStatement(A, rdfType, rdfsResource));
            assertTrue(store.containsStatement(C, rdfType, rdfsResource));
            assertEquals(3,store.getStatementCount());

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
    public void test_rdfs4_filterLiterals() {
        
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
            applyRule(inf.rdfs4, 2/* numComputed */);

            /*
             * validate the state of the primary store.
             * 
             * Note: There is no entailment for (C rdf:type rdfsResource) since
             * C is a Literal and literals are not allowed into the subject
             * position.
             */
            assertTrue(store.containsStatement(A, rdfType, C));
            assertTrue(store.containsStatement(A, rdfType, rdfsResource));
            assertEquals(2,store.getStatementCount());

        } finally {

            store.closeAndDelete();

        }

    }

}

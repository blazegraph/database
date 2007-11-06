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
 * Created on Nov 6, 2007
 */

package com.bigdata.rdf.inf;

import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite for the {@link Axioms}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAxiomModel extends AbstractInferenceEngineTestCase {

    /**
     * 
     */
    public TestAxiomModel() {
        super();
       
    }

    /**
     * @param name
     */
    public TestAxiomModel(String name) {
        super(name);
       
    }

    public void test_isAxiom() {
        
        AbstractTripleStore store = getStore();
        
        try {

            // store is empty.
            assertEquals(0,store.getStatementCount());
            
            BaseAxioms axioms = new OwlAxioms(store);

            // store is still empty (lazy assertion of axioms - at least for now).
            assertEquals(0,store.getStatementCount());

            assertTrue(axioms.isAxiom(new StatementImpl(URIImpl.RDF_TYPE,
                    URIImpl.RDF_TYPE, URIImpl.RDF_PROPERTY)));
            
            // add axioms to the store.
            axioms.addAxioms();
            
            assertTrue(axioms.isAxiom(store.getTermId(URIImpl.RDF_TYPE), store
                    .getTermId(URIImpl.RDF_TYPE), store
                    .getTermId(URIImpl.RDF_PROPERTY)));
            
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }
    
}

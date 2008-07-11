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
 * Created on Jul 10, 2008
 */

package com.bigdata.rdf.store;

import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.spo.SPORelation;

/**
 * Test suite for locating an {@link AbstractTripleStore}, locating the
 * {@link LexiconRelation} and {@link SPORelation} from the
 * {@link AbstractTripleStore}, and locating the {@link AbstractTripleStore}
 * from its contained relations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRelationLocator extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestRelationLocator() {
    }

    /**
     * @param name
     */
    public TestRelationLocator(String name) {
        super(name);
    }

    public void test_locators() {
        
        final AbstractTripleStore store = getStore();
        
        try {

            // we can locate the store.
            final AbstractTripleStore foundStore = (AbstractTripleStore) store
                    .getIndexManager()
                    .getResourceLocator()
                    .locate(store.getResourceIdentifier(), store.getTimestamp());
            
            assertNotNull(foundStore);
            
            {
                // the located store can find its relations.
                final SPORelation foundSPORelation = (SPORelation) foundStore
                        .getSPORelation();

                assertNotNull(foundSPORelation);

                assertNotNull(foundSPORelation.getContainerName());

                foundSPORelation.getContainerName().equals(
                        store.getResourceIdentifier());

                assertNotNull(foundSPORelation.getContainer());

                assertTrue(foundStore == foundSPORelation.getContainer());
            }

            {
                // the located store can find its relations.
                final LexiconRelation foundLexiconRelation = (LexiconRelation) foundStore
                        .getLexiconRelation();

                assertNotNull(foundLexiconRelation);

                assertNotNull(foundLexiconRelation.getContainerName());

                foundLexiconRelation.getContainerName().equals(
                        store.getResourceIdentifier());

                assertNotNull(foundLexiconRelation.getContainer());

                assertTrue(foundStore == foundLexiconRelation.getContainer());
            }

        } finally {

            store.close();
            
        }
        
    }
    
}

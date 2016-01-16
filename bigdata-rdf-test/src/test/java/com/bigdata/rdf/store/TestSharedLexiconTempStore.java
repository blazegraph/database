/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Jun 6, 2011
 */

package com.bigdata.rdf.store;

import java.util.Properties;
import java.util.Set;

import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * Test suite for {@link TempTripleStore}s sharing the same
 * {@link LexiconRelation} as the primary {@link AbstractTripleStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSharedLexiconTempStore extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestSharedLexiconTempStore() {
    }

    /**
     * @param name
     */
    public TestSharedLexiconTempStore(String name) {
        super(name);
    }

    /**
     * Test when the {@link TempTripleStore} does not have a
     * {@link LexiconRelation}.
     */
    public void test_TempTripleStore_noLexicon() {
        
        final String uriString1 = "http://www.bigdata.com/foo";
//        final String uriString2 = "http://www.bigdata.com/bar";
        final String uriString3 = "http://www.bigdata.com/goo";
        
        AbstractTripleStore store = getStore();
        
        try {

            final BigdataValueFactory vf = store.getValueFactory();

            final BigdataURI uri1 = vf.createURI(uriString1);
//            final BigdataURI uri2 = vf.createURI(uriString2);
            final BigdataURI uri3 = vf.createURI(uriString3);
            
            // add term to lexicon.
            store.addTerm(uri1);

            // verify lookup against lexicon.
            assertNotNull(store.getTerm(uri1.getIV()));
            
            {

                final Properties properties = new Properties();

                properties.setProperty(AbstractTripleStore.Options.LEXICON,
                        "false");
                // properties.setProperty(AbstractTripleStore.Options.ONE_ACCESS_PATH,
                // "true");

                final TempTripleStore tempTripleStore = new TempTripleStore(
                        store.getIndexManager().getTempStore(), properties,
                        store);

                try {

                    // verify lookup of term already known to the main kb.
                    assertNotNull(store.getTerm(uri1.getIV()));

                    // the temp triple store does not have its own lexicon.
                    assertFalse(tempTripleStore.lexicon);

                    // there is no lexicon associated with the temp kb.
                    assertNull(tempTripleStore.getLexiconRelation());

//                    // add 2nd term to lexicon of tmp kb.
//                    tempTripleStore.addTerm(uri2);
//
//                    // verify lookup of 2nd term against lexicon of tmp store.
//                    assertNotNull(tempTripleStore.getTerm(uri2.getIV()));

                } finally {

                    tempTripleStore.destroy();

                }

            }
            
//            // verify lookup of 2nd term against lexicon of main store.
//            assertNotNull(store.getTerm(uri2.getIV()));

            // add third term to main store to verify lexicon still valid.
            store.addTerm(uri3);

            // verify lookup against lexicon.
            assertNotNull(store.getTerm(uri3.getIV()));

        } finally {
            
            store.__tearDownUnitTest();
            
        }
        
    }
    
    /**
     * Test when the {@link TempTripleStore} uses a single access path (SPO).
     */
    public void test_TempTripleStore_oneAccessPath() {
        
        AbstractTripleStore store = getStore();

        try {

            {

                final Properties properties = new Properties();

                properties.setProperty(
                        AbstractTripleStore.Options.ONE_ACCESS_PATH, "true");

                final TempTripleStore tempTripleStore = new TempTripleStore(
                        store.getIndexManager().getTempStore(), properties,
                        store);

                try {

                    assertTrue(tempTripleStore.getSPORelation().oneAccessPath);
                    
                    final Set<String> names = tempTripleStore.getSPORelation()
                            .getIndexNames();

                    final int nexpected = tempTripleStore.isJustify() ? 2 : 1;
                    
                    assertEquals(nexpected, names.size());

                } finally {

                    tempTripleStore.destroy();

                }

            }

        } finally {
            
            store.__tearDownUnitTest();
            
        }
        
    }
    
}

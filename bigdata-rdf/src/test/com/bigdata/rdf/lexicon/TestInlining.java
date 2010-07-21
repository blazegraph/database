/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
 * Created on Nov 6, 2007
 */

package com.bigdata.rdf.lexicon;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.openrdf.model.vocabulary.RDF;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * Test suite for adding terms to the lexicon.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestInlining extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestInlining() {
        super();
       
    }

    /**
     * @param name
     */
    public TestInlining(String name) {
        super(name);
       
    }

    /**
     * Unsigned numerics should not be inlined at this time.
     */
    public void test_unsigned() {

        final Properties properties = getProperties();
        
        // test w/o predefined vocab.
        properties.setProperty(Options.VOCABULARY_CLASS, NoVocabulary.class
                .getName());

        // test w/o axioms - they imply a predefined vocab.
        properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        // test w/o the full text index.
        properties.setProperty(Options.TEXT_INDEX, "false");

        AbstractTripleStore store = getStore(properties);
        
        try {

            final Collection<BigdataValue> terms = new HashSet<BigdataValue>();

            // lookup/add some values.
            final BigdataValueFactory f = store.getValueFactory();

            terms.add(f.createLiteral("10", f
                    .createURI(XSD.UNSIGNED_INT.toString())));

            terms.add(f.createLiteral("12", f
                    .createURI(XSD.UNSIGNED_SHORT.toString())));

            terms.add(f.createLiteral("14", f
                    .createURI(XSD.UNSIGNED_LONG.toString())));

            /*
             * Note: Blank nodes will not round trip through the lexicon unless
             * the "told bnodes" is enabled.
             */
//            terms.add(f.createBNode());
//            terms.add(f.createBNode("a"));

            final Map<IV, BigdataValue> ids = doAddTermsTest(store, terms);

            if (store.isStable()) {
                
                store.commit();
                
                store = reopenStore(store);

                // verify same reverse mappings.

                final Map<IV, BigdataValue> ids2 = store.getLexiconRelation()
                        .getTerms(ids.keySet());

                assertEquals(ids.size(),ids2.size());
                
                for (IV id : ids.keySet()) {

                    assertTrue(id.isTermId());
                    
                    assertEquals("Id mapped to a different term? : termId="
                            + id, ids.get(id), ids2.get(id));

                }

            }

        } finally {
            
            store.__tearDownUnitTest();
            
        }

    }

    /**
     * The "told bnodes" mode uses the blank node ID as specified rather than
     * assigning one based on a UUID. For this case, we need to store the blank
     * nodes in the reverse index (T2ID) so we can translate a blank node back
     * to a specific identifier.
     */
    public void test_inlineBNodes() {

        final Properties properties = getProperties();
        
        // test w/o predefined vocab.
        properties.setProperty(Options.VOCABULARY_CLASS, NoVocabulary.class
                .getName());

        // test w/o axioms - they imply a predefined vocab.
        properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        // test w/o the full text index.
        properties.setProperty(Options.TEXT_INDEX, "false");

        // this is the "told bnodes" mode.
        properties.setProperty(Options.STORE_BLANK_NODES, "true");

        // inline the bnodes
        properties.setProperty(Options.INLINE_BNODES, "true");

        AbstractTripleStore store = getStore(properties);
        
        try {

            if (!store.isStable()) {

                /*
                 * We need a restart safe store to test this since otherwise a
                 * term cache could give us a false positive.
                 */

                return;
                
            }

            final Collection<BigdataValue> terms = new HashSet<BigdataValue>();

            // lookup/add some values.
            final BigdataValueFactory f = store.getValueFactory();

            final BigdataBNode b1 = f.createBNode("1");
            final BigdataBNode b01 = f.createBNode("01");
            final BigdataBNode b2 = f.createBNode(UUID.randomUUID().toString());
            final BigdataBNode b3 = f.createBNode("foo");
            final BigdataBNode b4 = f.createBNode("foo12345");

            terms.add(b1);
            terms.add(b01);
            terms.add(b2);
            terms.add(b3);
            terms.add(b4);

            final Map<IV, BigdataValue> ids = doAddTermsTest(store, terms);

            assertTrue(b1.getIV().isInline());
            assertFalse(b01.getIV().isInline());
            assertTrue(b2.getIV().isInline());
            assertFalse(b3.getIV().isInline());
            assertFalse(b4.getIV().isInline());
            
            if (store.isStable()) {
                
                store.commit();
                
                store = reopenStore(store);

                // verify same reverse mappings.

                final Map<IV, BigdataValue> ids2 = store.getLexiconRelation()
                        .getTerms(ids.keySet());

                assertEquals(ids.size(),ids2.size());
                
                for (IV iv : ids.keySet()) {

                    System.err.println(iv);
                    
                    assertEquals("Id mapped to a different term? : iv="
                            + iv, ids.get(iv), ids2.get(iv));

                }

            }

        } finally {
            
            store.__tearDownUnitTest();
            
        }

    }

    /**
     * @param store
     * @param terms
     * @return
     */
    private Map<IV, BigdataValue> doAddTermsTest(
            final AbstractTripleStore store,
            final Collection<BigdataValue> terms) {

        final int size = terms.size();

        final BigdataValue[] a = terms.toArray(new BigdataValue[size]);
        
        // resolve term ids.
        store
                .getLexiconRelation()
                .addTerms(a, size, false/* readOnly */);

        // populate map w/ the assigned term identifiers.
        final Collection<IV> ids = new ArrayList<IV>();

        for(BigdataValue t : a) {
            
            ids.add(t.getIV());
            
        }
        
        /*
         * Resolve the assigned term identifiers against the lexicon.
         */
        final Map<IV, BigdataValue> tmp = store.getLexiconRelation()
                .getTerms(ids);
        
        assertEquals(size,tmp.size());

        /*
         * Verify that the lexicon reports the same RDF Values for those term
         * identifiers (they will be "equals()" to the values that we added to
         * the lexicon).
         */
        for(BigdataValue expected : a) {

            assertNotSame("Did not assign internal value? : " + expected,
                    null, expected.getIV());

            final BigdataValue actual = tmp.get(expected.getIV());

            if (actual == null) {

                fail("Lexicon does not have value: iv="
                        + expected.getIV() + ", term=" + expected);
                
            }
            
            assertEquals("IV mapped to a different term? iv="
                    + expected.getIV(), expected, actual);

        }
        
        return tmp;

    }
        
}

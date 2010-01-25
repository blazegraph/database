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

import org.openrdf.model.vocabulary.RDF;

import com.bigdata.rdf.axioms.NoAxioms;
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
public class TestAddTerms extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestAddTerms() {
        super();
       
    }

    /**
     * @param name
     */
    public TestAddTerms(String name) {
        super(name);
       
    }

    public void test_addTerms() {

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

            terms.add(f.asValue(RDF.TYPE));
            terms.add(f.asValue(RDF.PROPERTY));
            terms.add(f.createLiteral("test"));
            terms.add(f.createLiteral("test", "en"));
            terms.add(f.createLiteral("10", f
                    .createURI("http://www.w3.org/2001/XMLSchema#int")));

            terms.add(f.createLiteral("12", f
                    .createURI("http://www.w3.org/2001/XMLSchema#float")));

            terms.add(f.createLiteral("12.", f
                    .createURI("http://www.w3.org/2001/XMLSchema#float")));

            terms.add(f.createLiteral("12.0", f
                    .createURI("http://www.w3.org/2001/XMLSchema#float")));

            terms.add(f.createLiteral("12.00", f
                    .createURI("http://www.w3.org/2001/XMLSchema#float")));

            /*
             * @todo Blank nodes will not round trip through the lexicon without
             * reference to a blank node cache. However, even a blank node cache
             * will not survive restart.
             */
//            terms.add(f.createBNode());
//            terms.add(f.createBNode("a"));

            final Map<Long, BigdataValue> ids = doAddTermsTest(store, terms);

            if (store.isStable()) {
                
                store.commit();
                
                store = reopenStore(store);

                // verify same reverse mappings.

                final Map<Long, BigdataValue> ids2 = store.getLexiconRelation()
                        .getTerms(ids.keySet());

                assertEquals(ids.size(),ids2.size());
                
                for (Long id : ids.keySet()) {

                    assertEquals("Id mapped to a different term? : termId="
                            + id, ids.get(id), ids2.get(id));

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
    private Map<Long, BigdataValue> doAddTermsTest(
            final AbstractTripleStore store,
            final Collection<BigdataValue> terms) {

        final int size = terms.size();

        final BigdataValue[] a = terms.toArray(new BigdataValue[size]);
        
        // resolve term ids.
        store
                .getLexiconRelation()
                .addTerms(a, size, false/* readOnly */);

        // populate map w/ the assigned term identifiers.
        final Collection<Long> ids = new ArrayList<Long>();

        for(BigdataValue t : a) {
            
            ids.add(t.getTermId());
            
        }
        
        /*
         * Resolve the assigned term identifiers against the lexicon.
         */
        final Map<Long, BigdataValue> tmp = store.getLexiconRelation()
                .getTerms(ids);
        
        assertEquals(size,tmp.size());

        /*
         * Verify that the lexicon reports the same RDF Values for those term
         * identifiers (they will be "equals()" to the values that we added to
         * the lexicon).
         */
        for(BigdataValue expected : a) {

            assertNotSame("Did not assign term identifier? : " + expected,
                    NULL, expected.getTermId());

            final BigdataValue actual = tmp.get(expected.getTermId());

            if (actual == null) {

                fail("Lexicon does not have value: termId="
                        + expected.getTermId() + ", term=" + expected);
                
            }
            
            assertEquals("Id mapped to a different term? id="
                    + expected.getTermId(), expected, actual);

        }
        
        return tmp;

    }
        
}

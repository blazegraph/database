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
 * Created on Nov 6, 2007
 */

package com.bigdata.rdf.lexicon;

import java.util.Properties;

import org.openrdf.model.vocabulary.RDF;

import com.bigdata.io.SerializerUtil;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.rdf.vocab.NoVocabulary;
import com.bigdata.rdf.vocab.RDFSVocabulary;
import com.bigdata.rdf.vocab.Vocabulary;

/**
 * Test suite for the {@link Vocabulary} implementations in the context of
 * an {@link AbstractTripleStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestVocabulary extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestVocabulary() {
        super();
       
    }

    /**
     * @param name
     */
    public TestVocabulary(String name) {
        super(name);
       
    }

    public void test_NoVocabulary() {

        final Properties properties = getProperties();
        
        // override the default.
        properties.setProperty(Options.VOCABULARY_CLASS, NoVocabulary.class
                .getName());

        properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class
                .getName());
        
        AbstractTripleStore store = getStore(properties);
        
        try {
            
            final Vocabulary vocab = store.getVocabulary();

            assertTrue(vocab instanceof NoVocabulary);
            
            final int nvalues = vocab.size();

            // the vocabulary should be empty.
            assertEquals(0, nvalues);

            // verify (de-)serialization.
            doRoundTripTest(vocab);

            // lookup/add some values.
            final BigdataValueFactory f = store.getValueFactory();

            // Must be using the same namespace.
            assertTrue(vocab.getNamespace()==f.getNamespace());
            
            final BigdataURI rdfType = f.asValue(RDF.TYPE);
            final BigdataURI rdfProperty = f.asValue(RDF.PROPERTY);
            final BigdataURI unknownURI = f.createURI("http://www.bigdata.com/unknown");
            
            // resolve term ids.
            store.addTerms(new BigdataValue[] { rdfType, rdfProperty, unknownURI });

            // point tests for unknown values (there are no known values).
            assertNull(vocab.get(RDF.TYPE));
//            try {
//                
//                vocab.get(RDF.TYPE);
//                
//                fail("Expecting: " + IllegalArgumentException.class);
//                
//            } catch (IllegalArgumentException ex) {
//                
//                log.info("Ignoring expected exception: " + ex);
//                
//            }

            assertNull(vocab.get(RDF.PROPERTY));

//            try {
//                
//                vocab.get(RDF.PROPERTY);
//                
//                fail("Expecting: " + IllegalArgumentException.class);
//                
//            } catch (IllegalArgumentException ex) {
//                
//                log.info("Ignoring expected exception: " + ex);
//                
//            }

            assertNull(vocab.get(unknownURI));
            
//            try {
//             
//                vocab.get(unknownURI);
//                
//                fail("Expecting: " + IllegalArgumentException.class);
//                
//            } catch (IllegalArgumentException ex) {
//                
//                log.info("Ignoring expected exception: " + ex);
//                
//            }

            if (store.isStable()) {

                store = reopenStore(store);

                final Vocabulary vocab2 = store.getVocabulary();

                assertSameVocabulary(vocab, vocab2);
                
            }

        } finally {
            
            store.__tearDownUnitTest();
            
        }

    }
    
    public void test_RdfsVocabulary() {
        
        final Properties properties = getProperties();
        
        // override the default.
        properties.setProperty(Options.VOCABULARY_CLASS, RDFSVocabulary.class
                .getName());

        properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class
                .getName());
        
        AbstractTripleStore store = getStore(properties);
        
        try {

            final Vocabulary vocab = store.getVocabulary();

            assertTrue(vocab instanceof RDFSVocabulary);

            // verify (de-)serialization.
            doRoundTripTest(vocab);

            // lookup/add some values.
            final BigdataValueFactory f = store.getValueFactory();

            final BigdataURI rdfType = f.asValue(RDF.TYPE);
            final BigdataURI rdfProperty = f.asValue(RDF.PROPERTY);
            final BigdataURI unknownURI = f.createURI("http://www.bigdata.com/unknown");
            
            // resolve term ids.
            store.addTerms(new BigdataValue[] { rdfType, rdfProperty, unknownURI });

            // point tests for known values.
            
            assertEquals(rdfType.getIV(), vocab.get(RDF.TYPE));
            
            assertEquals(rdfProperty.getIV(), vocab.get(RDF.PROPERTY));

            // point test for an unknown value.
            assertNull(vocab.get(unknownURI));
            
//            try {
//             
//                vocab.get(unknownURI);
//                
//                fail("Expecting: " + IllegalArgumentException.class);
//                
//            } catch (IllegalArgumentException ex) {
//                
//                log.info("Ignoring expected exception: " + ex);
//                
//            }

            if (store.isStable()) {

                store = reopenStore(store);

                final Vocabulary vocab2 = store.getVocabulary();

                assertSameVocabulary(vocab, vocab2);
                
            }
            
        } finally {
            
            store.__tearDownUnitTest();
            
        }
        
    }

    /**
     * Test (de-)serialization of a {@link Vocabulary}.
     */
    static void doRoundTripTest(final Vocabulary expected) {

        final byte[] data = SerializerUtil.serialize(expected);
        
        final Vocabulary actual = (Vocabulary) SerializerUtil.deserialize(data);

        assertSameVocabulary(expected, actual);
        
    }
    
    static protected void assertSameVocabulary(final Vocabulary expected,
            final Vocabulary actual) {

        com.bigdata.rdf.vocab.TestVocabulary.assertSameVocabulary(expected,actual);
        
    }
    
}

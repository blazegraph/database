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

package com.bigdata.rdf.axioms;

import java.util.Iterator;
import java.util.Properties;

import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.io.SerializerUtil;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;
import com.bigdata.rdf.store.AbstractTripleStore.Options;

/**
 * Test suite for the {@link Axioms}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAxioms extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestAxioms() {
        super();
       
    }

    /**
     * @param name
     */
    public TestAxioms(String name) {
        super(name);
       
    }

    public void test_NoAxioms() {

        Properties properties = getProperties();
        
        // override the default axiom model.
        properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        AbstractTripleStore store = getStore(properties);
        
        try {

//            // store is empty.
//            assertEquals(0, store.getStatementCount());
            
            final BigdataValueFactory f = store.getValueFactory();

            final BigdataURI rdfType = f.asValue(RDF.TYPE);
            final BigdataURI rdfProperty = f.asValue(RDF.PROPERTY);
            final BigdataURI unknownURI = f.createURI("http://www.bigdata.com/unknown");
            
            // resolve term ids.
            store.addTerms(new BigdataValue[] { rdfType, rdfProperty, unknownURI });
            
//            final NoAxioms axioms = new NoAxioms(store);
//            
//            axioms.init();

            final NoAxioms axioms = (NoAxioms)store.getAxioms();

            final int naxioms = axioms.size();

            // the model does not define any axioms.
            assertEquals(0,naxioms);
            
            // store contains the axioms.
            assertEquals(naxioms, store.getStatementCount());

            // point test for an axiom NOT defined by this model.
            assertFalse(axioms.isAxiom(rdfType.getIV(), rdfType.getIV(),
                    rdfProperty.getIV()));

            // point test for an axiom NOT defined by this model.
            assertFalse(axioms.isAxiom(rdfType.getIV(), rdfType.getIV(),
                    unknownURI.getIV()));

            {
                
                // verify (de-)serialization.
                final Axioms axioms2 = doRoundTripTest(axioms);
                
                // point test for an axiom NOT defined by this model.
                assertFalse(axioms2.isAxiom(rdfType.getIV(), rdfType.getIV(),
                        rdfProperty.getIV()));

                // point test for an axiom NOT defined by this model.
                assertFalse(axioms2.isAxiom(rdfType.getIV(), rdfType.getIV(),
                        unknownURI.getIV()));
                
            }
            
            if (store.isStable()) {

                store = reopenStore(store);

                final NoAxioms axioms2 = (NoAxioms)store.getAxioms();

                assertSameAxioms(axioms, axioms2);
                
                // point test for an axiom NOT defined by this model.
                assertFalse(axioms2.isAxiom(rdfType.getIV(), rdfType.getIV(),
                        rdfProperty.getIV()));

                // point test for an axiom NOT defined by this model.
                assertFalse(axioms2.isAxiom(rdfType.getIV(), rdfType.getIV(),
                        unknownURI.getIV()));

            }

        } finally {
            
            store.__tearDownUnitTest();
            
        }

    }
    
    public void test_RdfsAxioms() {
        
        Properties properties = getProperties();
        
        // override the default axiom model.
        properties.setProperty(Options.AXIOMS_CLASS, RdfsAxioms.class.getName());
        
        AbstractTripleStore store = getStore(properties);
        
        try {

//            // store is empty.
//            assertEquals(0, store.getStatementCount());
            
            final BigdataValueFactory f = store.getValueFactory();

            final BigdataURI rdfType = f.asValue(RDF.TYPE);
            final BigdataURI rdfProperty = f.asValue(RDF.PROPERTY);
            final BigdataURI unknownURI = f.createURI("http://www.bigdata.com/unknown");
            
            // resolve term ids.
            store.addTerms(new BigdataValue[] { rdfType, rdfProperty, unknownURI });
            
            final RdfsAxioms axioms = (RdfsAxioms)store.getAxioms();

            final int naxioms = axioms.size();
            
            // store contains the axioms.
            assertEquals(naxioms, store.getStatementCount());

            // point test for an axiom.
            assertTrue(axioms.isAxiom(rdfType.getIV(), rdfType.getIV(),
                    rdfProperty.getIV()));

            // point test for NOT an axiom.
            assertFalse(axioms.isAxiom(rdfType.getIV(), rdfType.getIV(),
                    unknownURI.getIV()));

            // verify (de-)serialization.
            {
                
                final Axioms axioms2 = doRoundTripTest(axioms);

                // point test for an axiom.
                assertTrue(axioms2.isAxiom(rdfType.getIV(), rdfType.getIV(),
                        rdfProperty.getIV()));

                // point test for NOT an axiom.
                assertFalse(axioms2.isAxiom(rdfType.getIV(), rdfType.getIV(),
                        unknownURI.getIV()));

            }
            
            if (store.isStable()) {

                store = reopenStore(store);

                final RdfsAxioms axioms2 = (RdfsAxioms)store.getAxioms();

                assertSameAxioms(axioms, axioms2);
                
                // point test for an axiom.
                assertTrue(axioms2.isAxiom(rdfType.getIV(), rdfType.getIV(),
                        rdfProperty.getIV()));

                // point test for NOT an axiom.
                assertFalse(axioms2.isAxiom(rdfType.getIV(), rdfType.getIV(),
                        unknownURI.getIV()));

            }
            
        } finally {
            
            store.__tearDownUnitTest();
            
        }
        
    }

    public void test_OwlAxioms() {
      
        Properties properties = getProperties();
        
        // override the default axiom model.
        properties.setProperty(com.bigdata.rdf.store.AbstractTripleStore.Options.AXIOMS_CLASS, OwlAxioms.class.getName());
        
        AbstractTripleStore store = getStore(properties);
        
        try {

//            // store is empty.
//            assertEquals(0, store.getStatementCount());
            
            final BigdataValueFactory f = store.getValueFactory();

            final BigdataURI rdfType = f.asValue(RDF.TYPE);
            final BigdataURI rdfProperty = f.asValue(RDF.PROPERTY);
            final BigdataURI owlEquivalentClass = f.asValue(OWL.EQUIVALENTCLASS);
            final BigdataURI unknownURI = f.createURI("http://www.bigdata.com/unknown");
            
            // resolve term ids.
            store.addTerms(new BigdataValue[] { rdfType, rdfProperty,
                    owlEquivalentClass, unknownURI });

            final OwlAxioms axioms = (OwlAxioms)store.getAxioms();
            
            final int naxioms = axioms.size();
            
            // store contains the axioms.
            assertEquals(naxioms, store.getStatementCount());
            
            // point test for an RDFS axiom.
            assertTrue(axioms.isAxiom(rdfType.getIV(), rdfType.getIV(),
                    rdfProperty.getIV()));

            // point test for an OWL axiom.
            assertTrue(axioms.isAxiom(owlEquivalentClass.getIV(), rdfType.getIV(),
                    rdfProperty.getIV()));

            // point test for NOT an axiom.
            assertFalse(axioms.isAxiom(rdfType.getIV(), rdfType.getIV(),
                    unknownURI.getIV()));

            {
            
                // verify (de-)serialization.
                final Axioms axioms2 = doRoundTripTest(axioms);

                // point test for an RDFS axiom.
                assertTrue(axioms2.isAxiom(rdfType.getIV(), rdfType.getIV(),
                        rdfProperty.getIV()));

                // point test for an OWL axiom.
                assertTrue(axioms2.isAxiom(owlEquivalentClass.getIV(), rdfType.getIV(),
                        rdfProperty.getIV()));

                // point test for NOT an axiom.
                assertFalse(axioms2.isAxiom(rdfType.getIV(), rdfType.getIV(),
                        unknownURI.getIV()));

            }

            if (store.isStable()) {

                store = reopenStore(store);

                final OwlAxioms axioms2 = (OwlAxioms)store.getAxioms();

                assertSameAxioms(axioms, axioms2);
                
                // point test for an RDFS axiom.
                assertTrue(axioms2.isAxiom(rdfType.getIV(), rdfType.getIV(),
                        rdfProperty.getIV()));

                // point test for an OWL axiom.
                assertTrue(axioms2.isAxiom(owlEquivalentClass.getIV(), rdfType.getIV(),
                        rdfProperty.getIV()));

                // point test for NOT an axiom.
                assertFalse(axioms2.isAxiom(rdfType.getIV(), rdfType.getIV(),
                        unknownURI.getIV()));

            }
            
        } finally {
            
            store.__tearDownUnitTest();
            
        }

    }
    
    /**
     * Test (de-)serialization of an axioms model.
     * 
     * @return The de-serialized axioms.
     */
    protected Axioms doRoundTripTest(final Axioms expected) {

        final byte[] data = SerializerUtil.serialize(expected);
        
        final Axioms actual = (Axioms) SerializerUtil.deserialize(data);

        assertSameAxioms(expected, actual);
        
        return actual;
        
    }
    
    protected void assertSameAxioms(final Axioms expected, final Axioms actual) {
        
        assertEquals("size", expected.size(), actual.size());
        
        final Iterator<SPO> itre = expected.axioms();
        
        final Iterator<SPO> itra = actual.axioms();
        
        while (itre.hasNext()) {

            assertTrue(itra.hasNext());

            final SPO spoe = itre.next();

            final SPO spoa = itra.next();

            if(log.isInfoEnabled()) {
                
                log.info(spoe.toString());
                
            }
            
            assertEquals(spoe, spoa);
            
        }
        
        assertFalse(itra.hasNext());
        
    }
    
}

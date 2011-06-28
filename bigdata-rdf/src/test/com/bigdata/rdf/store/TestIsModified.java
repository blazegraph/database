/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Feb 4, 2010
 */

package com.bigdata.rdf.store;

import java.util.Properties;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * Test suite for {@link ISPO#isModified()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIsModified extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestIsModified() {
    }

    /**
     * @param name
     */
    public TestIsModified(String name) {
        super(name);
    }

    /**
     * Unit test for {@link ISPO#isModified()}. The test explores the correct
     * reporting of statement modification as statements are asserted and
     * retracted on the database using the low-level {@link SPORelation} API.
     */
    public void test_reportMutation() {

        final Properties properties = super.getProperties();

        // override the default vocabulary.
        properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
                NoVocabulary.class.getName());

        // override the default axiom model.
        properties.setProperty(
                com.bigdata.rdf.store.AbstractTripleStore.Options.AXIOMS_CLASS,
                NoAxioms.class.getName());

        final AbstractTripleStore store = getStore(properties);

        try {

            final BigdataValueFactory valueFactory = store.getValueFactory();

            final BigdataURI x = valueFactory.createURI("http://www.foo.org/x");
            final BigdataURI y = valueFactory.createURI("http://www.foo.org/y");
            final BigdataURI z = valueFactory.createURI("http://www.foo.org/z");

            final BigdataURI A = valueFactory.createURI("http://www.foo.org/A");
            final BigdataURI B = valueFactory.createURI("http://www.foo.org/B");
            final BigdataURI C = valueFactory.createURI("http://www.foo.org/C");

            final BigdataURI rdfType = valueFactory.createURI(RDF.TYPE.stringValue());

            final BigdataURI rdfsSubClassOf = valueFactory.createURI(RDFS.SUBCLASSOF.stringValue());

            // resolve term identifiers.
            store.addTerms(new BigdataValue[] { x, y, z, A, B, C, rdfType,
                    rdfsSubClassOf });

            // Add a bunch of statements.
            {

                final BigdataStatement s1 = valueFactory.createStatement(x,
                        rdfType, C, null/* c */, StatementEnum.Explicit);

                final BigdataStatement s2 = valueFactory.createStatement(y,
                        rdfType, B, null/* c */, StatementEnum.Explicit);

                final BigdataStatement s3 = valueFactory.createStatement(z,
                        rdfType, A, null/* c */, StatementEnum.Explicit);

                final BigdataStatement s4 = valueFactory.createStatement(B,
                        rdfsSubClassOf, A, null/* c */, StatementEnum.Explicit);

                final BigdataStatement s5 = valueFactory.createStatement(C,
                        rdfsSubClassOf, B, null/* c */, StatementEnum.Explicit);

                assertFalse(s1.isModified());
                assertFalse(s2.isModified());
                assertFalse(s3.isModified());
                assertFalse(s4.isModified());
                assertFalse(s5.isModified());

                store.getSPORelation().insert(
                        new ISPO[] { s1, s2, s3, s4, s5 }, 5, null/* filter */);

                assertTrue(s1.isModified());
                assertTrue(s2.isModified());
                assertTrue(s3.isModified());
                assertTrue(s4.isModified());
                assertTrue(s5.isModified());

            }

            // Delete two statements, only one of which exists.
            {

                // statement exists.
                final BigdataStatement s1 = valueFactory.createStatement(x,
                        rdfType, C, null/* c */, StatementEnum.Explicit);

                // statement does not exist.
                final BigdataStatement s2 = valueFactory.createStatement(x,
                        x, x, null/* c */, StatementEnum.Explicit);

                assertFalse(store.hasStatement(s2.s(), s2.p(), s2.o(), s2.c()));
                
                assertFalse(s1.isModified());
                assertFalse(s2.isModified());

                assertEquals(1, store.getSPORelation().delete(
                        new ISPO[] { s1, s2 }, 2));

                assertTrue(s1.isModified());
                assertFalse(s2.isModified());

            }

        } finally {

            store.__tearDownUnitTest();

        }

    }

}

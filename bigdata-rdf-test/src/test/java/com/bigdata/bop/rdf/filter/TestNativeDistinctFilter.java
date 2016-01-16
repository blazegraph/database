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
 * Created on Aug 19, 2010
 */

package com.bigdata.bop.rdf.filter;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase2;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.vocab.decls.FOAFVocabularyDecl;

/**
 * Unit tests for {@link NativeDistinctFilter}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNativeDistinctFilter extends TestCase2 {

    /**
     * 
     */
    public TestNativeDistinctFilter() {
    }

    /**
     * @param name
     */
    public TestNativeDistinctFilter(String name) {
        super(name);
    }

    /**
     * Setup for a problem used by many of the join test suites.
     */
    static public class JoinSetup {

        protected final String spoNamespace;

        protected final IV<?, ?> knows, brad, john, fred, mary, paul, leon, luke;
        
        private Journal jnl;
        
        public JoinSetup(final String kbNamespace) {

            if (kbNamespace == null)
                throw new IllegalArgumentException();
            
            final Properties properties = new Properties();

            properties.setProperty(Journal.Options.BUFFER_MODE,
                    BufferMode.Transient.toString());
            
            jnl = new Journal(properties);

            // create the kb.
            final AbstractTripleStore kb = new LocalTripleStore(jnl,
                    kbNamespace, ITx.UNISOLATED, properties);

            kb.create();

            this.spoNamespace = kb.getSPORelation().getNamespace();

            // Setup the vocabulary.
            {
                final BigdataValueFactory vf = kb.getValueFactory();
                final String uriString = "http://bigdata.com/";
                final BigdataURI _knows = vf.asValue(FOAFVocabularyDecl.knows);
                final BigdataURI _brad = vf.createURI(uriString+"brad");
                final BigdataURI _john = vf.createURI(uriString+"john");
                final BigdataURI _fred = vf.createURI(uriString+"fred");
                final BigdataURI _mary = vf.createURI(uriString+"mary");
                final BigdataURI _paul = vf.createURI(uriString+"paul");
                final BigdataURI _leon = vf.createURI(uriString+"leon");
                final BigdataURI _luke = vf.createURI(uriString+"luke");

                final BigdataValue[] a = new BigdataValue[] {
                      _knows,//
                      _brad,
                      _john,
                      _fred,
                      _mary,
                      _paul,
                      _leon,
                      _luke
                };

                kb.getLexiconRelation()
                        .addTerms(a, a.length, false/* readOnly */);

                knows = _knows.getIV();
                brad = _brad.getIV();
                john = _john.getIV();
                fred = _fred.getIV();
                mary = _mary.getIV();
                paul = _paul.getIV();
                leon = _leon.getIV();
                luke = _luke.getIV();

            }

//            // data to insert (in key order for convenience).
//            final SPO[] a = {//
//                    new SPO(paul, knows, mary, StatementEnum.Explicit),// [0]
//                    new SPO(paul, knows, brad, StatementEnum.Explicit),// [1]
//                    
//                    new SPO(john, knows, mary, StatementEnum.Explicit),// [2]
//                    new SPO(john, knows, brad, StatementEnum.Explicit),// [3]
//                    
//                    new SPO(mary, knows, brad, StatementEnum.Explicit),// [4]
//                    
//                    new SPO(brad, knows, fred, StatementEnum.Explicit),// [5]
//                    new SPO(brad, knows, leon, StatementEnum.Explicit),// [6]
//            };
//
//            // insert data (the records are not pre-sorted).
//            kb.addStatements(a, a.length);
//
//            // Do commit since not scale-out.
//            jnl.commit();

        }

        protected void destroy() {

            if (jnl != null) {
                jnl.destroy();
                jnl = null;
            }

        }
        
    }

    /**
     * Unit test for {@link NativeDistinctFilter#getFilterKeyOrder(SPOKeyOrder)}
     */
    public void test_filterKeyOrder_quads() {

        assertEquals(new int[] {0,1,2},
                NativeDistinctFilter.getFilterKeyOrder(SPOKeyOrder.SPOC));

        assertEquals(new int[] {0,1,3},
                NativeDistinctFilter.getFilterKeyOrder(SPOKeyOrder.POCS));

        assertEquals(new int[] {0,2,3},
                NativeDistinctFilter.getFilterKeyOrder(SPOKeyOrder.OCSP));

        assertEquals(new int[] {1,2,3},
                NativeDistinctFilter.getFilterKeyOrder(SPOKeyOrder.CSPO));

        assertEquals(new int[] {0,2,3},
                NativeDistinctFilter.getFilterKeyOrder(SPOKeyOrder.PCSO));

        assertEquals(new int[] {0,1,2},
                NativeDistinctFilter.getFilterKeyOrder(SPOKeyOrder.SOPC));

    }
    
    /**
     * Unit test for {@link NativeDistinctFilter#getFilterKeyOrder(SPOKeyOrder)}
     */
    public void test_filterKeyOrder_triples() {

        assertEquals(new int[] { 0, 1, 2 },
                NativeDistinctFilter.getFilterKeyOrder(SPOKeyOrder.SPO));

        assertEquals(new int[] { 1, 2, 0 },
                NativeDistinctFilter.getFilterKeyOrder(SPOKeyOrder.POS));

        assertEquals(new int[] { 2, 0, 1 },
                NativeDistinctFilter.getFilterKeyOrder(SPOKeyOrder.OSP));

    }
    
    public void test_htreeDistinctSPOFilter() {

        final JoinSetup setup = new JoinSetup(getName());
        
        try {

            /*
             * The distinct SPOs.
             */
            final List<SPO> expected = new LinkedList<SPO>(Arrays.asList(new SPO[] {//
                    
                new SPO(setup.paul, setup.knows, setup.mary, StatementEnum.Explicit),// [0]
                new SPO(setup.paul, setup.knows, setup.brad, StatementEnum.Explicit),// [1]

                new SPO(setup.john, setup.knows, setup.mary, StatementEnum.Explicit),// [2]
                new SPO(setup.john, setup.knows, setup.brad, StatementEnum.Explicit),// [3]

                new SPO(setup.mary, setup.knows, setup.brad, StatementEnum.Explicit),// [4]

                new SPO(setup.brad, setup.knows, setup.fred, StatementEnum.Explicit),// [5]
                new SPO(setup.brad, setup.knows, setup.leon, StatementEnum.Explicit),// [6]
                
        }));

            // Add everything that we expect.
            final List<SPO> given = new LinkedList<SPO>(expected);
            
            // Add in some duplicates too.
            given.addAll(Arrays.asList(new SPO[] {//
                    new SPO(setup.john, setup.knows, setup.brad, StatementEnum.Explicit),// [3]
                    new SPO(setup.mary, setup.knows, setup.brad, StatementEnum.Explicit),// [4]
                    new SPO(setup.brad, setup.knows, setup.fred, StatementEnum.Explicit),// [5]
            }));

            // Iterator should visit the distinct IVs.
            @SuppressWarnings("unchecked")
            final Iterator<SPO> actual = NativeDistinctFilter.newInstance(
                    SPOKeyOrder.CSPO)
                    .filter(given.iterator(), null/* context */);

            assertSameIteratorAnyOrder(expected.toArray(new SPO[0]), actual);

        } finally {

            setup.destroy();

        }

    }

}

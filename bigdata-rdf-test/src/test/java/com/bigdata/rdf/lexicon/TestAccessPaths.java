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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;

import org.openrdf.model.vocabulary.RDF;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;
import com.bigdata.rdf.vocab.NoVocabulary;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.striterator.IKeyOrder;

/**
 * Test suite for
 * {@link LexiconRelation#newAccessPath(IIndexManager, IPredicate, IKeyOrder)}.
 * <p>
 * Note: If you query with {@link IV} or {@link BigdataValue} already cached (on
 * one another or in the termsCache) then the cached value will be returned.
 * <p>
 * Note: Blank nodes will not unify with themselves unless you are using told
 * blank node semantics.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAccessPaths extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestAccessPaths() {
        super();
       
    }

    /**
     * @param name
     */
    public TestAccessPaths(String name) {
        super(name);
       
    }

	/**
	 * 
	 */
    public void test_TERMS_accessPaths() {
    
        final Properties properties = getProperties();
        
        // test w/o predefined vocab.
        properties.setProperty(Options.VOCABULARY_CLASS, NoVocabulary.class
                .getName());

        // test w/o the full text index.
        properties.setProperty(Options.TEXT_INDEX, "false");

        final AbstractTripleStore store = getStore(properties);
        
        try {

            final Collection<BigdataValue> terms = new HashSet<BigdataValue>();

            // lookup/add some values.
            final BigdataValueFactory f = store.getValueFactory();

            final BigdataValue rdfType;
            final BigdataValue largeLiteral;
            terms.add(rdfType=f.asValue(RDF.TYPE));
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
            
            terms.add(largeLiteral=f.createLiteral(TestAddTerms.getVeryLargeLiteral()));

            if (store.getLexiconRelation().isStoreBlankNodes()) {
                /*
                 * Note: Blank nodes will not round trip through the lexicon
                 * unless the "told bnodes" is enabled.
                 */
                terms.add(f.createBNode());
                terms.add(f.createBNode("a"));
            }

            final int size = terms.size();

            final BigdataValue[] a = terms.toArray(new BigdataValue[size]);

            // resolve term ids.
            store.getLexiconRelation().addTerms(a, size, false/* readOnly */);

            // populate map w/ the assigned term identifiers.
            final Collection<IV> ids = new ArrayList<IV>();

            for (BigdataValue t : a) {

                ids.add(t.getIV());

            }

            // Test id2terms for reverse lookup.

            doAccessPathTest(rdfType, LexiconKeyOrder.ID2TERM, store);
            
            doAccessPathTest(largeLiteral, LexiconKeyOrder.BLOBS, store);
			
        } finally {

            store.__tearDownUnitTest();

        }

    }

	/**
	 * Test the access path.
	 * 
	 * @param expected
	 *            The {@link BigdataValue} with its {@link IV}.
	 * @param expectedKeyOrder
	 *            The keyorder to be used.
	 * @param store
	 *            The store.
	 */
	private void doAccessPathTest(final BigdataValue expected,
			final IKeyOrder<BigdataValue> expectedKeyOrder,
			final AbstractTripleStore store) {

		assertNotNull(expected.getIV());

		final LexiconRelation r = store.getLexiconRelation();

		@SuppressWarnings("unchecked")
        final IVariable<BigdataValue> termvar = Var.var("termvar");
		
		final IPredicate<BigdataValue> predicate = LexPredicate
				.reverseInstance(r.getNamespace(), ITx.UNISOLATED, termvar,
						new Constant<IV>(expected.getIV()));

		final IKeyOrder<BigdataValue> keyOrder = r.getKeyOrder(predicate);

		assertEquals(expectedKeyOrder, keyOrder);

		final IAccessPath<BigdataValue> ap = r.newAccessPath(
				store.getIndexManager(), predicate, keyOrder);

		assertEquals(1, ap.rangeCount(false/* exact */));
		assertEquals(1, ap.rangeCount(true/* exact */));

		final Iterator<BigdataValue> itr = ap.iterator();
		assertTrue(itr.hasNext());
		final BigdataValue actual = itr.next();
		assertFalse(itr.hasNext());

		assertEquals(expected, actual);

		assertEquals(expected.getIV(), actual.getIV());

	}
    
}

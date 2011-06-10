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
import java.util.Properties;

import org.openrdf.model.vocabulary.RDF;

import com.bigdata.bop.IPredicate;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.rdf.vocab.NoVocabulary;
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

            fail("write tests for access paths");
            
        } finally {

            store.__tearDownUnitTest();

        }

    }

}

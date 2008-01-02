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
 * Created on Dec 19, 2007
 */

package com.bigdata.rdf.store;

import java.io.File;
import java.util.Properties;
import java.util.UUID;

import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;
import org.openrdf.vocabulary.XmlSchema;

import com.bigdata.rdf.model.OptimizedValueFactory._BNode;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.store.AbstractTripleStore.Options;

/**
 * Test of adding terms with the full text index enabled and of lookup of terms
 * by tokens which appear within those terms.
 * 
 * @todo Note: This is not a proxied test suite since we have to explicitly
 *       create an instance of the store in which the full text index is
 *       enabled. If we ever make the full text index an "all the time" feature
 *       then this test could be moved into {@link TestTripleStoreBasics} and
 *       reconfigured as a proxied test suite.  The methods {@link #getStore()}
 *       and {@link #reopenStore(AbstractTripleStore)} would go away in this
 *       eventuality.
 * 
 * @todo test restart safety of the full text index.
 * 
 * @todo test search against the full text index. abstract the search api so
 *       that it queries the terms index directly when a data typed literal or a
 *       URI is used (typed query).
 * 
 * @todo test both the term at a time and the batch term insert APIs.
 * 
 * @todo test all term types (uris, bnodes, and literals). only literals are
 *       being indexed right now, but there could be a use case for tokenizing
 *       URIs. There is never going to be any reason to tokenize BNodes.
 * 
 * @todo test language code support
 * 
 * @todo test data type support (probably do not index data typed terms).
 * 
 * @todo test XML literal indexing (strip out CDATA and index the tokens found
 *       therein).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestFullTextIndex extends AbstractTestCase {

    /**
     * 
     */
    public TestFullTextIndex() {
    }

    /**
     * @param name
     */
    public TestFullTextIndex(String name) {
        super(name);
    }

    public Properties getProperties() {

        Properties properties = new Properties(super.getProperties());
        
        // enable the full text index.
        properties.setProperty(Options.TEXT_INDEX,"true");
        
        return properties;

    }
    
    protected AbstractTripleStore getStore() {
     
        return new LocalTripleStore( getProperties() );
        
    }

    /**
     * Re-open the same backing store.
     * 
     * @param store
     *            the existing store.
     * 
     * @return A new store.
     * 
     * @exception Throwable
     *                if the existing store is closed, or if the store can not
     *                be re-opened, e.g., from failure to obtain a file lock,
     *                etc.
     */
    protected AbstractTripleStore reopenStore(AbstractTripleStore store) {
        
        // close the store.
        store.close();
        
        if (!((LocalTripleStore) store).store.isStable()) {
            
            throw new UnsupportedOperationException("The backing store is not stable");
            
        }
        
        // Note: clone to avoid modifying!!!
        Properties properties = (Properties)getProperties().clone();
        
        // Turn this off now since we want to re-open the same store.
        properties.setProperty(Options.CREATE_TEMP_FILE,"false");
        
        // The backing file that we need to re-open.
        File file = ((LocalTripleStore) store).store.getFile();
        
        assertNotNull(file);
        
        // Set the file property explictly.
        properties.setProperty(Options.FILE,file.toString());
        
        return new LocalTripleStore( properties );
        
    }

    /**
     * Test helper verifies that the term is not in the lexicon, adds the term
     * to the lexicon, verifies that the term can be looked up by its assigned
     * term identifier, verifies that the term is now in the lexicon, and
     * verifies that adding the term again returns the same term identifier.
     * 
     * @param term
     *            The term.
     */
    protected void doAddTermTest(AbstractTripleStore store, _Value term) {

        assertEquals(NULL, store.getTermId(term));

        final long id = store.addTerm(term);

        assertNotSame(NULL, id);

        assertEquals(id, store.getTermId(term));

        assertEquals(term, store.getTerm(id));

        assertEquals(id, store.addTerm(term));

    }

    public void test_fullTextIndex01() {

        AbstractTripleStore store = getStore();

        try {

            doAddTermTest(store, new _Literal("abc"));
            doAddTermTest(store, new _Literal("abc",
                    new _URI(XmlSchema.DECIMAL)));
            doAddTermTest(store, new _Literal("abc", "en"));
            doAddTermTest(store, new _Literal("good day", "en"));
            doAddTermTest(store, new _Literal("gutten tag", "de"));
            doAddTermTest(store, new _Literal("tag team", "en"));
            doAddTermTest(store, new _Literal("the first day", "en")); // 'the' is a stopword.

            doAddTermTest(store, new _URI("http://www.bigdata.com"));
            doAddTermTest(store, new _URI(RDF.TYPE));
            doAddTermTest(store, new _URI(RDFS.SUBCLASSOF));
            doAddTermTest(store, new _URI(XmlSchema.DECIMAL));

            doAddTermTest(store, new _BNode(UUID.randomUUID().toString()));
            doAddTermTest(store, new _BNode("a12"));

            store.commit();

            dumpTerms(store);

            /*
             * re-open the store before search to verify that the data were made
             * restart safe.
             */
            store = reopenStore(store);
            
            store.textSearch("", "abc"); // finds plain literals (@todo or anytype?)
            store.textSearch("en", "abc");
            store.textSearch("en", "GOOD DAY");
            store.textSearch("de", "gutten tag");
            store.textSearch("de", "tag");
            store.textSearch("en", "tag");
            store.textSearch("de", "team");
            store.textSearch("en", "the"); // 'the' is a stopword.
            
        } finally {

            store.closeAndDelete();

        }

    }
    
}

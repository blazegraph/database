/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Jun 30, 2011
 */

package com.bigdata.rdf.lexicon;

import junit.framework.TestCase2;

import org.openrdf.model.Value;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.btree.BytesUtil.UnsignedByteArrayComparator;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.internal.XSD;

/**
 * Test suite for {@link LexiconKeyBuilder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Test decode of the [code] byte for each type of Value.
 * 
 *          TODO Test ordering assumptions across the values (signed code byte
 *          order) and then Unicode sort order within a given Value type with
 *          wrinkles for datatype literals and language code literals. Tests
 *          which examine the sort code ordering would have to specify a
 *          specific {@link IKeyBuilder} configuration.
 */
public class TestLexiconKeyBuilder extends TestCase2 {

    /**
     * 
     */
    public TestLexiconKeyBuilder() {
    }

    /**
     * @param name
     */
    public TestLexiconKeyBuilder(String name) {
        super(name);
    }

    /**
     * Tests encode of a key and the decode of its "code" byte.
     * 
     * @see ITermIndexCodes
     */
    public void test_encodeDecodeCodeByte() {
        
        final LexiconKeyBuilder keyBuilder = new LexiconKeyBuilder(new KeyBuilder());

        assertEquals(ITermIndexCodes.TERM_CODE_URI, keyBuilder
                .value2Key(RDF.TYPE)[0]);
        
        assertEquals(ITermIndexCodes.TERM_CODE_BND, keyBuilder
                .value2Key(new BNodeImpl("foo"))[0]);
        
        assertEquals(ITermIndexCodes.TERM_CODE_LIT, keyBuilder
                .value2Key(new LiteralImpl("abc"))[0]);
        
        assertEquals(ITermIndexCodes.TERM_CODE_LCL, keyBuilder
                .value2Key(new LiteralImpl("abc","en"))[0]);        
        
        assertEquals(ITermIndexCodes.TERM_CODE_DTL, keyBuilder
                .value2Key(new LiteralImpl("abc",XSD.BOOLEAN))[0]);        
        
    }

    /**
     * Tests the gross ordering over the different kinds of {@link Value}s but
     * deliberately does not pay attention to the sort key ordering for string
     * data.
     * 
     * @see ITermIndexCodes
     */
    public void test_keyOrder() {

        final LexiconKeyBuilder keyBuilder = new LexiconKeyBuilder(
                new KeyBuilder());

        final byte[] uri = keyBuilder.value2Key(RDF.TYPE);

        final byte[] bnd = keyBuilder.value2Key(new BNodeImpl("foo"));

        final byte[] lit = keyBuilder.value2Key(new LiteralImpl("abc"));

        final byte[] lcl = keyBuilder.value2Key(new LiteralImpl("abc", "en"));

        final byte[] dtl = keyBuilder.value2Key(new LiteralImpl("abc",
                XSD.BOOLEAN));

        // URIs before plain literals.
        assertTrue(UnsignedByteArrayComparator.INSTANCE.compare(uri, lit) < 0);

        // plain literals before language code literals.
        assertTrue(UnsignedByteArrayComparator.INSTANCE.compare(lit, lcl) < 0);

        // language code literals before datatype literals.
        assertTrue(UnsignedByteArrayComparator.INSTANCE.compare(lcl, dtl) < 0);

        // datatype literals before blank nodes.
        assertTrue(UnsignedByteArrayComparator.INSTANCE.compare(dtl, bnd) < 0);

    }
    
}

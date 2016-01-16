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
 * Created on Jun 29, 2011
 */

package com.bigdata.rdf.internal;

import java.util.Random;

import junit.framework.TestCase2;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.internal.impl.AbstractIV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.lexicon.BlobsIndexHelper;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.util.Bytes;

/**
 * Test suite for {@link TermId}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTermIV extends TestCase2 {

    public TestTermIV() {
        
    }
    public TestTermIV(String name) {
        super(name);
    }
    
    /**
     * Unit test for {@link TermId#NullIV}.
     */
    public void test_TermIV_isNullIV() {
        
        final TermId<?> iv = TermId.NullIV;
        
        assertTrue(iv.isNullIV());
        
        assertEquals(VTE.URI, iv.getVTE());

        assertEquals(0L, iv.getTermId());

        final TermId<?> fromStringIV = TermId.fromString(iv.toString());
        
        // TermIds have same data but do not compare as equals()
        assertNotSame(iv, fromStringIV);
        assertEquals(iv.flags(), fromStringIV.flags());
        assertEquals(iv.getTermId(), fromStringIV.getTermId());
        
        assertNull(IVUtility.decode(TermId.NullIV.encode(new KeyBuilder())
                .getKey()));

    }

    public void test_TermIV_isExtensionIV() {

        final TermId<BigdataURI> iv = new TermId<BigdataURI>(VTE.URI,
                12L);

        assertEquals(VTE.URI, iv.getVTE());

        assertFalse(iv.isInline());

        assertFalse(iv.isExtension());

        assertEquals(DTE.XSDBoolean, iv.getDTE());

        assertEquals(12, iv.getTermId());

    }

    @SuppressWarnings("unchecked")
    private void doTermIVTest(final VTE vte, final long termId) {

        final IKeyBuilder keyBuilder = new KeyBuilder(1 + Bytes.SIZEOF_LONG);

        @SuppressWarnings("rawtypes")
        final TermId<?> iv = new TermId(vte, termId);

        assertEquals(AbstractIV.toFlags(vte, false/* inline */,
                false/* extension */, DTE.XSDBoolean), iv.flags());

        assertEquals(vte, iv.getVTE());

        assertEquals(termId, iv.getTermId());

        assertEquals(iv, TermId.fromString(iv.toString()));

        final byte[] key = iv.encode(keyBuilder.reset()).getKey();
        
        @SuppressWarnings("rawtypes")
        final TermId iv2 = (TermId) IVUtility.decode(key);

        assertEquals(vte, iv2.getVTE());

        assertEquals(termId, iv2.getTermId());

        assertEquals(iv, TermId.fromString(iv2.toString()));

        assertEquals(iv, iv2);

        final BigdataValue value = BigdataValueFactoryImpl.getInstance(
                getName()).createLiteral("foo");
        
        iv2.setValue(value);
        
        assertEquals(iv,TermId.fromString(iv2.toString()));
        
        
    }
    
    public void test_TermId_URI() {

        doTermIVTest(VTE.URI, 12L);

    }

    public void test_TermId_Literal() {
        
        doTermIVTest(VTE.LITERAL, 12L);
        
    }

    public void test_TermId_BNode() {
        
        doTermIVTest(VTE.BNODE, 12L);

    }

    public void test_TermId_URI_Counter_ONE() {

        doTermIVTest(VTE.URI,1L);

    }

    public void test_TermId_URI_Counter_MINUS_ONE() {
        
        if (!IVUtility.PACK_TIDS)
            doTermIVTest(VTE.URI, -1L);
        
    }

    public void test_TermId_URI_Counter_MIN_VALUE() {

        if (!IVUtility.PACK_TIDS)
            doTermIVTest(VTE.URI, Long.MIN_VALUE);

    }

    /*
     * Note: This is hitting odd fence posts having to do with equality and
     * mock IVs.
     */
//    public void test_TermId_URI_Counter_ZERO() {
//
//        doTermIVTest(VTE.URI, 0);
//
//    }

    public void test_TermId_URI_Counter_MAX_VALUE() {

        doTermIVTest(VTE.URI, Long.MAX_VALUE);

    }

    /**
     * Unit tests for {@link TermId}
     */
    public void test_TermId() {

        final Random r = new Random();

        for (VTE vte : VTE.values()) {

            // 64 bit random term identifier.
            final long termId = IVUtility.PACK_TIDS ? Math.abs(r.nextLong())
                    : r.nextLong();

            final TermId<?> v = new TermId<BigdataValue>(vte, termId);

            assertFalse(v.isInline());

            assertEquals(termId, v.getTermId());

            try {
                v.getInlineValue();
                fail("Expecting " + UnsupportedOperationException.class);
            } catch (UnsupportedOperationException ex) {
                // ignored.
            }

            // Verify we can encode/decode the TermId.
            {

                final byte[] key = v.encode(new KeyBuilder()).getKey();

                final IV<?, ?> iv2 = (IV<?, ?>) IVUtility.decode(key);

                assertEquals(v, iv2);

                assertEquals(0, v.compareTo(iv2));

                assertEquals(0, iv2.compareTo(v));

                assertEquals(v.isURI(), iv2.isURI());
                assertEquals(v.isLiteral(), iv2.isLiteral());
                assertEquals(v.isBNode(), iv2.isBNode());
                assertEquals(v.isStatement(), iv2.isStatement());

            }

            assertEquals("flags=" + v.flags(), vte, v.getVTE());

            // assertEquals(dte, v.getInternalDataTypeEnum());

            switch (vte) {
            case URI:
                assertTrue(v.isURI());
                assertFalse(v.isBNode());
                assertFalse(v.isLiteral());
                assertFalse(v.isStatement());
                break;
            case BNODE:
                assertFalse(v.isURI());
                assertTrue(v.isBNode());
                assertFalse(v.isLiteral());
                assertFalse(v.isStatement());
                break;
            case LITERAL:
                assertFalse(v.isURI());
                assertFalse(v.isBNode());
                assertTrue(v.isLiteral());
                assertFalse(v.isStatement());
                break;
            case STATEMENT:
                assertFalse(v.isURI());
                assertFalse(v.isBNode());
                assertFalse(v.isLiteral());
                assertTrue(v.isStatement());
                break;
            default:
                fail("vte=" + vte);
            }

        }
        
    }

    /**
     * Unit test for {@link TermId#mockIV(VTE)}.
     */
    public void test_TermId_mockIV() {

        for(VTE vte : VTE.values()) {
            
            final TermId<?> v = TermId.mockIV(vte);
            
            assertFalse(v.isInline());

            assertTrue(v.isNullIV());

            assertEquals(vte, v.getVTE());

            assertEquals(0L, v.getTermId());

            // assertEquals(termId, v.getTermId());

            try {
                v.getInlineValue();
                fail("Expecting " + UnsupportedOperationException.class);
            } catch (UnsupportedOperationException ex) {
                // ignored.
            }

            // Verify we can encode/decode the TermId.
            {
             
                final byte[] key = v.encode(new KeyBuilder()).getKey();

                final IV<?, ?> iv2 = (IV<?, ?>) IVUtility.decode(key);

                // Note: decodes as [null].
                assertNull(iv2);
                
            }

            switch (vte) {
            case URI:
                assertTrue(v.isURI());
                assertFalse(v.isBNode());
                assertFalse(v.isLiteral());
                assertFalse(v.isStatement());
                break;
            case BNODE:
                assertFalse(v.isURI());
                assertTrue(v.isBNode());
                assertFalse(v.isLiteral());
                assertFalse(v.isStatement());
                break;
            case LITERAL:
                assertFalse(v.isURI());
                assertFalse(v.isBNode());
                assertTrue(v.isLiteral());
                assertFalse(v.isStatement());
                break;
            case STATEMENT:
                assertFalse(v.isURI());
                assertFalse(v.isBNode());
                assertFalse(v.isLiteral());
                assertTrue(v.isStatement());
                break;
            default:
                fail("vte=" + vte);
            }

            // Verify that toString and fromString work,
            {
            
                final String s = v.toString();

                final TermId<?> tmp = TermId.fromString(s);

                // re-encode to the same string.
                assertEquals(s, tmp.toString());
                
                // TermIds have same data but do not compare as equals()
                assertNotSame(v, tmp);
                assertEquals(v.flags(), tmp.flags());
                assertEquals(v.getTermId(), tmp.getTermId());
                                
            }
            
        }
        
    }
    
    public void test_NullIV_decode_as_null_reference() {
        
        final BlobsIndexHelper h = new BlobsIndexHelper();
        
        final IV<?,?> iv = TermId.NullIV;
        
        final IKeyBuilder keyBuilder = h.newKeyBuilder();

        final byte[] key = IVUtility.encode(keyBuilder, iv).getKey();
        
        assertNull(IVUtility.decode(key));
        
    }
    
}

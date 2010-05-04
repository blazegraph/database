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
 * Created on Apr 19, 2010
 */

package com.bigdata.rdf.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Random;
import java.util.UUID;

import junit.framework.TestCase2;

import org.openrdf.model.Value;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Unit tests for encoding and decoding compound keys (such as are used by the
 * statement indices) in which some of the key components are inline values
 * having variable component lengths while others are term identifiers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestEncodeDecodeKeys.java 2756 2010-05-03 22:26:18Z thompsonbry
 *          $
 * 
 * @todo Code SIDs using a term identifier, UUID, or secure hash function? The
 *       latter two choices are inline options. The former is not.
 * 
 * @todo Unit tests for correct rejection of {@link BigInteger} with large
 *       magnitudes (very long byte[]s). The purpose of this is to avoid
 *       problems in the indices related to keys growing too long for a
 *       reasonable B+Tree leaf. (The maximum length for the encoding is 32kb
 *       per key. At m=256 that is only 8MB, which is not all that bad so maybe
 *       we do not have to do anything here.)
 * 
 *       FIXME Finish these unit tests for all data types [xsd:decimal], and
 *       unsigned byte, short, int and long. For the unsigned values, I need to
 *       figure out whether you pass in a data type with more bits (e.g., long
 *       for an unsigned int) or if you manage the bits as if they were
 *       unsigned. Consider adding unsigned support and xsd:decimal support
 *       afterwards since they are both a PITA.
 * 
 *       FIXME Unit tests for inline of blank nodes (this is based on a UUID, so
 *       there is not much to that).
 * 
 *       FIXME Refactor to pull the inline bit and dataTypeId bit out of the
 *       dataTypeCode and then test extensible projection of types derived by
 *       restriction onto the intrinsic data types.
 */
public class TestEncodeDecodeKeys extends TestCase2 {

    public TestEncodeDecodeKeys() {
        super();
    }
    
    public TestEncodeDecodeKeys(String name) {
        super(name);
    }

    /**
     * Unit test for {@link InternalValueTypeEnum} verifies that the
     * correspondence between the enumerated types and the internal values is
     * correct (self-consistent).
     */
    public void test_VTE_selfConsistent() {
       
        for(InternalValueTypeEnum e : InternalValueTypeEnum.values()) {

            assertTrue("expected: " + e + " (v=" + e.v + "), actual="
                    + InternalValueTypeEnum.valueOf(e.v),
                    e == InternalValueTypeEnum.valueOf(e.v));

        }
        
    }
    
    /**
     * Unit test for {@link InternalValueTypeEnum} verifies that all legal byte
     * values decode to an internal value type enum (basically, this checks that
     * we mask the two lower bits).
     */
    public void test_VTE_decodeNoErrors() {

        for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
            
            assertNotNull(InternalValueTypeEnum.valueOf((byte) i));
            
        }
        
    }

    /**
     * Unit test for {@link InternalDataTypeEnum} verifies that the
     * correspondence between the enumerated types and the internal values is
     * correct.
     */
    public void test_DTE_selfConsistent() {

        for(InternalDataTypeEnum e : InternalDataTypeEnum.values()) {

            assertTrue("expected: " + e + " (v=" + e.v + "), actual="
                    + InternalDataTypeEnum.valueOf(e.v),
                    e == InternalDataTypeEnum.valueOf(e.v));

            assertEquals(e.v, e.v());

        }

    }

//    /**
//     * Unit tests for {@link AbstractInternalValue} focused on encoding and
//     * decoding the flags byte representing the combination of the
//     * {@link InternalValueTypeEnum} and the {@link InternalDataTypeEnum}. This
//     * is done using a private concrete implementation of
//     * {@link AbstractInternalValue} since we need to be able to represent ANY
//     * kind of RDF Value in order to test all possible
//     * {@link InternalValueTypeEnum} and {@link InternalDataTypeEnum}.
//     * 
//     * @see AbstractInternalValue
//     * 
//     * @todo Drop this as the cases are covered by {@link #test_TermId()}?
//     */
//    public void test_AbstractInternalValue() {
//
//        class ValueImpl<V extends BigdataValue/* URI,BNode,Literal,SID */>
//                extends AbstractInternalValue<V, Void> {
//
//            private static final long serialVersionUID = 1L;
//
//            public ValueImpl(final InternalValueTypeEnum vte,
//                    final InternalDataTypeEnum dte) {
//
//                super(vte, dte);
//
//            }
//
//            public String toString() {
//                final String datatype = getInternalDataTypeEnum().getDatatype();
//                return "TermId(" + getInternalValueTypeEnum().getCharCode()
//                        + ")" + (datatype == null ? "" : datatype);
//            }
//
//            final public V asValue(BigdataValueFactory f)
//                    throws UnsupportedOperationException {
//                throw new UnsupportedOperationException();
//            }
//
//            final public Void getInlineValue() {
//                throw new UnsupportedOperationException();
//            }
//
//            final public long getTermId() {
//                throw new UnsupportedOperationException();
//            }
//
//            final public boolean isTermId() {
//                return getInternalDataTypeEnum() == InternalDataTypeEnum.TermId;
//            }
//
//            final public boolean isInline() {
//                return getInternalDataTypeEnum() != InternalDataTypeEnum.TermId;
//            }
//
//            final public boolean isNull() {
//                throw new UnsupportedOperationException();
//            }
//
//        } // test class.
//
////        final Random r = new Random();
//        
//        for(InternalValueTypeEnum vte : InternalValueTypeEnum.values()) {
////        InternalValueTypeEnum vte = InternalValueTypeEnum.BNODE;
////        {
//
//            for(InternalDataTypeEnum dte : InternalDataTypeEnum.values()) {
//
////                // 64 bit random term identifier.
////                final long termId = r.nextLong();
//
//                final ValueImpl<?> v = new ValueImpl<BigdataValue>(vte, dte);
//
////                assertTrue(v.isTermId());
//
////                assertFalse(v.isInline());
//                
////                if (termId == 0L) {
////                    assertTrue(v.toString(), v.isNull());
////                } else {
////                    assertFalse(v.toString(), v.isNull());
////                }
//                
////                assertEquals(termId, v.getTermId());
//
//                try {
//                    v.getInlineValue();
//                    fail("Expecting "+UnsupportedOperationException.class);
//                } catch(UnsupportedOperationException ex) {
//                    // ignored.
//                }
//                
//                assertEquals("flags="+v.flags(), vte, v.getInternalValueTypeEnum());
//
//                assertEquals(dte, v.getInternalDataTypeEnum());
//
//                switch(vte) {
//                case URI:
//                    assertTrue(v.isURI());
//                    assertFalse(v.isBNode());
//                    assertFalse(v.isLiteral());
//                    assertFalse(v.isStatement());
//                    break;
//                case BNODE:
//                    assertFalse(v.isURI());
//                    assertTrue(v.isBNode());
//                    assertFalse(v.isLiteral());
//                    assertFalse(v.isStatement());
//                    break;
//                case LITERAL:
//                    assertFalse(v.isURI());
//                    assertFalse(v.isBNode());
//                    assertTrue(v.isLiteral());
//                    assertFalse(v.isStatement());
//                    break;
//                case STATEMENT:
//                    assertFalse(v.isURI());
//                    assertFalse(v.isBNode());
//                    assertFalse(v.isLiteral());
//                    assertTrue(v.isStatement());
//                    break;
//                default:
//                    fail("vte=" + vte);
//                }
//
//            }
//            
//        }
//        
//    }

    /**
     * Unit tests for {@link TermId}
     * 
     * @todo test asValue(BigdataFactory)
     */
    public void test_TermId() {

        final Random r = new Random();
        
        for(InternalValueTypeEnum vte : InternalValueTypeEnum.values()) {
//        InternalValueTypeEnum vte = InternalValueTypeEnum.BNODE;
//        {

            for(InternalDataTypeEnum dte : InternalDataTypeEnum.values()) {

                // 64 bit random term identifier.
                final long termId = r.nextLong();

                final TermId<?> v = new TermId<BigdataValue>(vte, dte, termId);

                assertTrue(v.isTermId());

                assertFalse(v.isInline());
                
                if (termId == 0L) {
                    assertTrue(v.toString(), v.isNull());
                } else {
                    assertFalse(v.toString(), v.isNull());
                }
                
                assertEquals(termId, v.getTermId());

                try {
                    v.getInlineValue();
                    fail("Expecting "+UnsupportedOperationException.class);
                } catch(UnsupportedOperationException ex) {
                    // ignored.
                }
                
                assertEquals("flags="+v.flags(), vte, v.getInternalValueTypeEnum());

                assertEquals(dte, v.getInternalDataTypeEnum());

                switch(vte) {
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
        
    }

    /**
     * Encode an RDF value into a key for one of the statement indices.
     * 
     * @param keyBuilder
     *            The key builder.
     * @param v
     *            The RDF value.
     * 
     * @return The key builder.
     * 
     * @todo This should be extensible for registered datatypes, enumerations,
     *       etc.
     * 
     * @todo move to SPOKeyOrder.
     */
    private void encodeValue(final IKeyBuilder keyBuilder,
            final InternalValue<?, ?> v) {

        keyBuilder.append(v.flags());

        /*
         * Append the natural value type representation.
         * 
         * Note: We have to handle the unsigned byte, short, int and long values
         * specially to get the correct total key order.
         * 
         * FIXME Add support to encode and decode unsigned short, int, and long
         * and also UUID to IKeyBuilder and KeyBuilder to support the unsigned
         * xsd data types and UUIDs.
         */
        final InternalDataTypeEnum dte = v.getInternalDataTypeEnum();
        
        if (dte == InternalDataTypeEnum.TermId) {

            /*
             * Handle a term identifier.
             */

            keyBuilder.append(v.getTermId());
            
            return;
            
        }
        
        final AbstractDatatypeLiteralInternalValue<?, ?> t = (AbstractDatatypeLiteralInternalValue<?, ?>) v;
        
        switch (dte) {
//        case TermId:
//            keyBuilder.append(v.getTermId());
//            break;
        case XSDBoolean:
            keyBuilder.append((byte) (t.booleanValue() ? 1 : 0));
            break;
        case XSDByte:
            keyBuilder.append(t.byteValue());
            break;
        case XSDShort:
            keyBuilder.append(t.shortValue());
            break;
        case XSDInt:
            keyBuilder.append(t.intValue());
            break;
        case XSDFloat:
            keyBuilder.append(t.floatValue());
            break;
        case XSDLong:
            keyBuilder.append(t.longValue());
            break;
        case XSDDouble:
            keyBuilder.append(t.doubleValue());
            break;
        case XSDInteger:
            keyBuilder.append(t.integerValue());
            break;
        case XSDDecimal:
            keyBuilder.append(t.decimalValue());
            break;
        case UUID:
            keyBuilder.append((UUID)t.getInlineValue());
            break;
//        case XSDUnsignedByte:
//            keyBuilder.appendUnsigned(t.byteValue());
//            break;
//        case XSDUnsignedShort:
//            keyBuilder.appendUnsigned(t.shortValue());
//            break;
//        case XSDUnsignedInt:
//            keyBuilder.appendUnsigned(t.intValue());
//            break;
//        case XSDUnsignedLong:
//            keyBuilder.appendUnsigned(t.longValue());
//            break;
        default:
            throw new AssertionError(v.toString());
        }

    }

    /**
     * Decode a key from one of the statement indices. The components of the key
     * are returned in the order in which they appear in the key. The caller
     * must reorder those components using their knowledge of which index is
     * being decoded in order to reconstruct the corresponding RDF statement.
     * The returned array will always have 4 components. However, the last key
     * component will be <code>null</code> if there are only three components in
     * the <i>key</i>.
     * 
     * @param key
     *            The key.
     * 
     * @return An ordered array of the {@link InternalValue}s for that key.
     * 
     * @todo This could reuse the caller's array.
     */
    public InternalValue<?, ?>[] decodeStatementKey(final byte[] key) {
        
        final InternalValue<?,?>[] a = new InternalValue[4];

        // The byte offset into the key.
        int offset = 0;
        
        for (int i = 0; i < 4; i++) {

            final byte flags = KeyBuilder.decodeByte(key[offset]);
            offset++;

            // The value type (URI, Literal, BNode, SID)
            final InternalValueTypeEnum vte = AbstractInternalValue
                    .getInternalValueTypeEnum(flags);

            // The data type
            final InternalDataTypeEnum dte = AbstractInternalValue
                    .getInternalDataTypeEnum(flags);

            if (dte == InternalDataTypeEnum.TermId) {

                /*
                 * Handle a term identifier (versus an inline value).
                 */

                // decode the term identifier.
                final long termId = KeyBuilder.decodeLong(key, offset);
                offset += Bytes.SIZEOF_LONG;

                // @todo type specific factory is required!
                a[i] = InternalValueTypeFactory.INSTANCE.newTermId(vte, dte,
                        termId);

                continue;
                
            }

            /*
             * Handle an inline value.
             * 
             * @todo construct the InternalValue objects using factory?
             */
            final InternalValue v;
            switch (dte) {
            case XSDBoolean: {
                final byte x = KeyBuilder.decodeByte(key[offset++]);
                if (x == 0) {
                    v = XSDBooleanInternalValue.FALSE;
                } else {
                    v = XSDBooleanInternalValue.TRUE;
                }
                break;
            }
            case XSDByte: {
                final byte x = KeyBuilder.decodeByte(key[offset++]);
                v = new XSDByteInternalValue<BigdataLiteral>(x);
                break;
            }
            case XSDShort: {
                final short x = KeyBuilder.decodeShort(key, offset);
                offset += Bytes.SIZEOF_SHORT;
                v = new XSDShortInternalValue<BigdataLiteral>(x);
                break;
            }
            case XSDInt: {
                final int x = KeyBuilder.decodeInt(key, offset);
                offset += Bytes.SIZEOF_INT;
                v = new XSDIntInternalValue<BigdataLiteral>(x);
                break;
            }
            case XSDLong: {
                final long x = KeyBuilder.decodeLong(key, offset);
                offset += Bytes.SIZEOF_LONG;
                v = new XSDLongInternalValue<BigdataLiteral>(x);
                break;
            }
            case XSDFloat: {
                final float x = KeyBuilder.decodeFloat(key, offset);
                offset += Bytes.SIZEOF_FLOAT;
                v = new XSDFloatInternalValue<BigdataLiteral>(x);
                break;
            }
            case XSDDouble: {
                final double x = KeyBuilder.decodeDouble(key, offset);
                offset += Bytes.SIZEOF_DOUBLE;
                v = new XSDDoubleInternalValue<BigdataLiteral>(x);
                break;
            }
            case XSDInteger: {
                final byte[] b = KeyBuilder.decodeBigInteger2(offset, key);
                offset += 2 + b.length;
                final BigInteger x = new BigInteger(b);
                v = new XSDIntegerInternalValue<BigdataLiteral>(x);
                break;
            }
//            case XSDDecimal:
//                keyBuilder.append(t.decimalValue());
//                break;
            case UUID: {
                final UUID x = KeyBuilder.decodeUUID(key, offset);
                offset += Bytes.SIZEOF_UUID;
                v = new UUIDInternalValue<BigdataLiteral>(x);
                break;
            }
//            case XSDUnsignedByte:
//                keyBuilder.appendUnsigned(t.byteValue());
//                break;
//            case XSDUnsignedShort:
//                keyBuilder.appendUnsigned(t.shortValue());
//                break;
//            case XSDUnsignedInt:
//                keyBuilder.appendUnsigned(t.intValue());
//                break;
//            case XSDUnsignedLong:
//                keyBuilder.appendUnsigned(t.longValue());
//                break;
            default:
                // FIXME handle all of the inline value types.
                throw new UnsupportedOperationException("vte=" + vte + ", dte="
                        + dte);
            }
            
            a[i] = v;

            if (i == 2 && offset == key.length) {
                // We have three components and the key is exhausted.
                break;
            }

        }
        
        return a; 
        
    }

    /**
     * Factory for {@link InternalValue} objects.
     * 
     * @todo Only one instance? Should this be linked to the
     *       {@link BigdataInlineValueConfig}?
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public static class InternalValueTypeFactory {

        public static InternalValueTypeFactory INSTANCE = new InternalValueTypeFactory();
        
        private InternalValueTypeFactory() {
            
        }
        
        public InternalValue<?, ?> newTermId(final InternalValueTypeEnum vte,
                final InternalDataTypeEnum dte, final long termId) {

            if (vte == null)
                throw new IllegalArgumentException();
            
            if (dte == null)
                throw new IllegalArgumentException();

            /*
             * FIXME If the DTE is independent of whether or not we are inlining
             * the value the we need to take one bit for (termId | inline). On
             * the other hand, if we only preserve the DTE when we are inlining
             * the value, then we are throwing away the datatype information for
             * term identifiers.
             */
            assert dte == InternalDataTypeEnum.TermId;

            return new TermId(vte, dte, termId);
            
        }
        
    }

    /**
     * Encode a key for an RDF Statement index.
     * 
     * @param keyBuilder
     * @param s
     * @param p
     * @param o
     * @param c
     *            The context position (used iff the key order has 4 components
     *            in the key (quads)).
     * @return
     * 
     * @todo move to SPOKeyOrder
     */
    private byte[] encodeStatement(final IKeyBuilder keyBuilder,
            final InternalValue<?, ?> s, final InternalValue<?, ?> p,
            final InternalValue<?, ?> o, final InternalValue<?, ?> c) {

        keyBuilder.reset();

        encodeValue(keyBuilder, s);

        encodeValue(keyBuilder, p);

        encodeValue(keyBuilder, o);

        if (c != null) {

            encodeValue(keyBuilder, c);

        }

        return keyBuilder.getKey();

    }

    /**
     * Encodes an array of {@link InternalValue}s and then decodes them and
     * verifies that the decoded values are equal-to the original values.
     * 
     * @param e
     *            The array of the expected values.
     */
    protected void doEncodeDecodeTest(final InternalValue<?, ?>[] e) {

        /*
         * Encode.
         */
        final byte[] key;
        {
            final IKeyBuilder keyBuilder = new KeyBuilder();

            for (int i = 0; i < e.length; i++) {

                encodeValue(keyBuilder, e[i]);

            }

            key = keyBuilder.getKey();
        }

        /*
         * Decode
         */
        {
            final InternalValue<?, ?>[] a = decodeStatementKey(key);

            for (int i = 0; i < e.length; i++) {

                if (!e[i].equals(a[i])) {
                 
                    fail("index=" + Integer.toString(i) + " : expected=" + e[i]
                            + ", actual=" + a[i]);
                    
                }

            }

        }

    }

    /**
     * Unit test for encoding and decoding a statement formed from
     * {@link TermId}s.
     */
    public void test_SPO_encodeDecode_allTermIds() {

        final InternalValue<?, ?>[] e = {//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 1L),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 2L),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 3L),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 4L) //
        };

        doEncodeDecodeTest(e);
        
    }

    /**
     * Unit test where the RDF Object position is an xsd:boolean.
     */
    public void test_SPO_encodeDecode_XSDBoolean() {

        final InternalValue<?, ?>[] e = {//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 1L),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 2L),//
                new XSDBooleanInternalValue<BigdataLiteral>(true),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 4L) //
        };

        doEncodeDecodeTest(e);

        e[2] = new XSDBooleanInternalValue<BigdataLiteral>(false);

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test where the RDF Object position is an xsd:byte.
     */
    public void test_SPO_encodeDecode_XSDByte() {

        final InternalValue<?, ?>[] e = {//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 1L),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 2L),//
                new XSDByteInternalValue<BigdataLiteral>((byte)1),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 4L) //
        };

        doEncodeDecodeTest(e);

        e[2] = new XSDByteInternalValue<BigdataLiteral>((byte) -1);

        doEncodeDecodeTest(e);

        e[2] = new XSDByteInternalValue<BigdataLiteral>((byte) 0);

        doEncodeDecodeTest(e);

        e[2] = new XSDByteInternalValue<BigdataLiteral>(Byte.MAX_VALUE);

        doEncodeDecodeTest(e);

        e[2] = new XSDByteInternalValue<BigdataLiteral>(Byte.MIN_VALUE);

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test where the RDF Object position is an xsd:short.
     */
    public void test_SPO_encodeDecode_XSDShort() {

        final InternalValue<?, ?>[] e = {//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 1L),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 2L),//
                new XSDShortInternalValue<BigdataLiteral>((short)1),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 4L) //
        };

        doEncodeDecodeTest(e);

        e[2] = new XSDShortInternalValue<BigdataLiteral>((short) -1);

        doEncodeDecodeTest(e);

        e[2] = new XSDShortInternalValue<BigdataLiteral>((short) 0);

        doEncodeDecodeTest(e);

        e[2] = new XSDShortInternalValue<BigdataLiteral>(Short.MAX_VALUE);

        doEncodeDecodeTest(e);

        e[2] = new XSDShortInternalValue<BigdataLiteral>(Short.MIN_VALUE);

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test where the RDF Object position is an xsd:int.
     */
    public void test_SPO_encodeDecode_XSDInt() {

        final InternalValue<?, ?>[] e = {//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 1L),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 2L),//
                new XSDIntInternalValue<BigdataLiteral>(1),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 4L) //
        };

        doEncodeDecodeTest(e);

        e[2] = new XSDIntInternalValue<BigdataLiteral>(-1);

        doEncodeDecodeTest(e);

        e[2] = new XSDIntInternalValue<BigdataLiteral>(0);

        doEncodeDecodeTest(e);

        e[2] = new XSDIntInternalValue<BigdataLiteral>(Integer.MAX_VALUE);

        doEncodeDecodeTest(e);

        e[2] = new XSDIntInternalValue<BigdataLiteral>(Integer.MIN_VALUE);

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test where the RDF Object position is an xsd:long.
     */
    public void test_SPO_encodeDecode_XSDLong() {

        final InternalValue<?, ?>[] e = {//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 1L),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 2L),//
                new XSDLongInternalValue<BigdataLiteral>(1L),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 4L) //
        };

        doEncodeDecodeTest(e);

        e[2] = new XSDLongInternalValue<BigdataLiteral>(-1L);

        doEncodeDecodeTest(e);

        e[2] = new XSDLongInternalValue<BigdataLiteral>(0L);

        doEncodeDecodeTest(e);

        e[2] = new XSDLongInternalValue<BigdataLiteral>(Long.MAX_VALUE);

        doEncodeDecodeTest(e);

        e[2] = new XSDLongInternalValue<BigdataLiteral>(Long.MIN_VALUE);

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test where the RDF Object position is an xsd:float.
     */
    public void test_SPO_encodeDecode_XSDFloat() {

        final InternalValue<?, ?>[] e = {//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 1L),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 2L),//
                new XSDFloatInternalValue<BigdataLiteral>(1f),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 4L) //
        };

        doEncodeDecodeTest(e);

        e[2] = new XSDFloatInternalValue<BigdataLiteral>(-1f);

        doEncodeDecodeTest(e);

        e[2] = new XSDFloatInternalValue<BigdataLiteral>(+0f);

        doEncodeDecodeTest(e);

        e[2] = new XSDFloatInternalValue<BigdataLiteral>(-0f);

        doEncodeDecodeTest(e);

        e[2] = new XSDFloatInternalValue<BigdataLiteral>(Float.MAX_VALUE);

        doEncodeDecodeTest(e);

        e[2] = new XSDFloatInternalValue<BigdataLiteral>(Float.MIN_VALUE);

        doEncodeDecodeTest(e);

        e[2] = new XSDFloatInternalValue<BigdataLiteral>(Float.MIN_NORMAL);

        doEncodeDecodeTest(e);

        e[2] = new XSDFloatInternalValue<BigdataLiteral>(Float.POSITIVE_INFINITY);

        doEncodeDecodeTest(e);

        e[2] = new XSDFloatInternalValue<BigdataLiteral>(Float.NEGATIVE_INFINITY);

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test where the RDF Object position is an xsd:double.
     */
    public void test_SPO_encodeDecode_XSDDouble() {

        final InternalValue<?, ?>[] e = {//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 1L),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 2L),//
                new XSDDoubleInternalValue<BigdataLiteral>(1d),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 4L) //
        };

        doEncodeDecodeTest(e);

        e[2] = new XSDDoubleInternalValue<BigdataLiteral>(-1d);

        doEncodeDecodeTest(e);

        e[2] = new XSDDoubleInternalValue<BigdataLiteral>(-0d);

        doEncodeDecodeTest(e);

        e[2] = new XSDDoubleInternalValue<BigdataLiteral>(+0d);

        doEncodeDecodeTest(e);

        e[2] = new XSDDoubleInternalValue<BigdataLiteral>(Double.MAX_VALUE);

        doEncodeDecodeTest(e);

        e[2] = new XSDDoubleInternalValue<BigdataLiteral>(Double.MIN_VALUE);

        doEncodeDecodeTest(e);

        e[2] = new XSDDoubleInternalValue<BigdataLiteral>(Double.MIN_NORMAL);

        doEncodeDecodeTest(e);

        e[2] = new XSDDoubleInternalValue<BigdataLiteral>(Double.POSITIVE_INFINITY);

        doEncodeDecodeTest(e);

        e[2] = new XSDDoubleInternalValue<BigdataLiteral>(Double.NEGATIVE_INFINITY);

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test where the RDF Object position is an xsd:float whose value is
     * {@link Float#NaN}.
     * 
     * @todo This unit test fails for NaN. I am not convinced that this is a
     *       problem since I do not know what it would mean (in terms of the
     *       natural order of the key) to allow a value which is not a number
     *       into a key. At a miminum we need to clarify the behavior for NaN
     *       for the lexicon and the {@link IKeyBuilder}.
     *       <p>
     *       According to the XML Schema Datatypes Recommendation: NaN equals
     *       itself but is ·incomparable· with (neither greater than nor less
     *       than) any other value in the ·value space·.
     */
    public void test_SPO_encodeDecode_XSDFloat_NaN() {

        final InternalValue<?, ?>[] e = {//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 1L),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 2L),//
                new XSDFloatInternalValue<BigdataLiteral>(Float.NaN),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 4L) //
        };
    
        e[2] = new XSDFloatInternalValue<BigdataLiteral>(Float.NaN);

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test where the RDF Object position is an xsd:double whose value is
     * {@link Double#NaN}.
     * 
     * @todo This unit test fails for NaN. I am not convinced that this is a
     *       problem since I do not know what it would mean (in terms of the
     *       natural order of the key) to allow a value which is not a number
     *       into a key. At a miminum we need to clarify the behavior for NaN
     *       for the lexicon and the {@link IKeyBuilder}.
     *       <p>
     *       According to the XML Schema Datatypes Recommendation: NaN equals
     *       itself but is ·incomparable· with (neither greater than nor less
     *       than) any other value in the ·value space·.
     */
    public void test_SPO_encodeDecode_XSDDouble_NaN() {

        final InternalValue<?, ?>[] e = {//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 1L),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 2L),//
                new XSDDoubleInternalValue<BigdataLiteral>(Double.NaN),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 4L) //
        };
    
        e[2] = new XSDDoubleInternalValue<BigdataLiteral>(Double.NaN);

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test where the RDF Object position is a {@link UUID}.
     */
    public void test_SPO_encodeDecode_UUID() {

        final InternalValue<?, ?>[] e = {//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 1L),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 2L),//
                new UUIDInternalValue<BigdataLiteral>(UUID.randomUUID()),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 4L) //
        };

        doEncodeDecodeTest(e);

        for (int i = 0; i < 1000; i++) {

            e[2] = new UUIDInternalValue<BigdataLiteral>(UUID.randomUUID());

            doEncodeDecodeTest(e);

        }

    }

    /**
     * Unit test where the RDF Object position is an xsd:integer.
     */
    public void test_SPO_encodeDecode_XSDInteger() {

        final InternalValue<?, ?>[] e = {//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 1L),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 2L),//
                new XSDIntegerInternalValue<BigdataLiteral>(BigInteger
                        .valueOf(3L)),//
                new TermId<BigdataURI>(InternalValueTypeEnum.URI,
                        InternalDataTypeEnum.TermId, 4L) //
        };

        doEncodeDecodeTest(e);
        
    }

//    /**
//     * Unit test verifies the ability to decode a key with an inline variable
//     * length coding of a datatype literal using <code>xsd:integer</code>, aka
//     * {@link BigInteger}.
//     */
//    public void test_SPO_encodeDecode_BigInteger() {
//
//        final Long s = 1L;
//        final Long p = 2L;
//        final BigInteger o = BigInteger.valueOf(3L);
//        final Long c = 3L;
//
//        final byte[] key = new KeyBuilder().append(s).append(p).append(o)
//                .append(c).getKey();
//
//        int off = 0;
//        
//        final Long e_s = KeyBuilder.decodeLong(key, off);
//        off += Bytes.SIZEOF_LONG;
//        
//        final Long e_p = KeyBuilder.decodeLong(key, off);
//        off += Bytes.SIZEOF_LONG;
//        
//        final BigInteger e_o;
//        {
//            final byte[] b = KeyBuilder.decodeBigInteger2(off, key);
//            off += 2 + b.length;
//            e_o = new BigInteger(b);
//        }
//
//        final Long e_c = KeyBuilder.decodeLong(key, off);
//        off += Bytes.SIZEOF_LONG;
//
//        assertEquals("s", s, e_s);
//        assertEquals("p", p, e_p);
//        assertEquals("o", o, e_o);
//        assertEquals("c", c, e_c);
//        
//    }

    /**
     * Abstract base class for inline RDF values (literals, blank nodes, and
     * statement identifiers can be inlined).
     * <p>
     * {@inheritDoc}
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id: TestEncodeDecodeKeys.java 2753 2010-05-01 16:36:59Z
     *          thompsonbry $
     */
    static abstract public class AbstractInlineInternalValue<V extends BigdataValue, T>
            extends AbstractInternalValue<V, T> {

        /**
         * 
         */
        private static final long serialVersionUID = -2847844163772097836L;

        protected AbstractInlineInternalValue(final InternalValueTypeEnum vte,
                final InternalDataTypeEnum dte) {

            super(vte, dte);

        }
        
        /**
         * Returns the String-value of a Value object. This returns either a
         * Literal's label, a URI's URI or a BNode's ID.
         * 
         * @see Value#stringValue()
         */
        abstract public String stringValue();
        
        public String toString() {
            
            return super.toString() + "[" + stringValue() + "]";
            
        }
        
    }

    /**
     * Class for inline RDF blank nodes. Blank nodes MUST be based on UUIDs in
     * order to be lined.
     * <p>
     * {@inheritDoc}
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id: TestEncodeDecodeKeys.java 2753 2010-05-01 16:36:59Z
     *          thompsonbry $
     * 
     * @see AbstractTripleStore.Options
     */
    static public class AbstractBNodeInternalValue<V extends BigdataBNode> extends
            AbstractInlineInternalValue<V, UUID> {

        /**
         * 
         */
        private static final long serialVersionUID = -4560216387427028030L;
        
        private final UUID id;
        
        public AbstractBNodeInternalValue(final UUID id) {

            super(InternalValueTypeEnum.BNODE, InternalDataTypeEnum.UUID);

            if (id == null)
                throw new IllegalArgumentException();

            this.id = id;

        }

        @Override
        public String stringValue() {
            return id.toString();
        }

        final public UUID getInlineValue() {
            return id;
        }

        final public long getTermId() {
            throw new UnsupportedOperationException();
        }

        final public boolean isInline() {
            return true;
        }

        final public boolean isNull() {
            return false;
        }

        final public boolean isTermId() {
            return false;
        }

        public V asValue(BigdataValueFactory f)
                throws UnsupportedOperationException {
            // TODO asValue()
            throw new UnsupportedOperationException();
        }

        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o instanceof AbstractBNodeInternalValue) {
                return this.id.equals(((AbstractBNodeInternalValue) o).id);
            }
            return false;
        }

        public int hashCode() {
            return id.hashCode();
        }
        
    }
    
    /**
     * Abstract base class for inline RDF literals.
     * <p>
     * {@inheritDoc}
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id: TestEncodeDecodeKeys.java 2753 2010-05-01 16:36:59Z
     *          thompsonbry $
     */
    static abstract public class AbstractLiteralInternalValue<V extends BigdataLiteral, T>
            extends AbstractInlineInternalValue<V, T> {

        /**
         * 
         */
        private static final long serialVersionUID = -2684528247542410336L;

        protected AbstractLiteralInternalValue(final InternalDataTypeEnum dte) {

            super(InternalValueTypeEnum.LITERAL, dte);

        }

    }

    /**
     * Abstract base class for RDF datatype literals adds primitive data type
     * value access methods.
     * <p>
     * {@inheritDoc}
     * 
     * @todo What are the SPARQL semantics for casting among these datatypes?
     *       They should probably be reflected here since that is the real use
     *       case. I believe that those casts also require failing a solution if
     *       the cast is not legal, in which case these methods might not be all
     *       that useful.
     *       <p>
     *       Also see BigdataLiteralImpl and XMLDatatypeUtil. It handles the
     *       conversions by reparsing, but there is no reason to do that here
     *       since we have the canonical point in the value space.
     * 
     * @see http://www.w3.org/TR/rdf-sparql-query/#FunctionMapping, The casting
     *      rules for SPARQL
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id: TestEncodeDecodeKeys.java 2753 2010-05-01 16:36:59Z
     *          thompsonbry $
     */
    static abstract public class AbstractDatatypeLiteralInternalValue<V extends BigdataLiteral, T>
            extends AbstractLiteralInternalValue<V, T> {

        /**
         * 
         */
        private static final long serialVersionUID = 5962615541158537189L;

        protected AbstractDatatypeLiteralInternalValue(final InternalDataTypeEnum dte) {

            super(dte);

        }

        final public boolean isInline() {
            return true;
        }

        final public boolean isTermId() {
            return false;
        }

        final public boolean isNull() {
            return false;
        }

        final public long getTermId() {
            throw new UnsupportedOperationException();
        }

        /** Return the <code>boolean</code> value of <i>this</i> value. */
        abstract public boolean booleanValue();

        /**
         * Return the <code>byte</code> value of <i>this</i> value.
         * <p>
         * Note: Java lacks unsigned data types. For safety, operations on
         * unsigned XSD data types should be conducted after a widening
         * conversion. For example, operations on <code>xsd:unsignedByte</code>
         * should be performed using {@link #shortValue()}.
         */
        abstract public byte byteValue();

        /**
         * Return the <code>short</code> value of <i>this</i> value.
         * <p>
         * Note: Java lacks unsigned data types. For safety, operations on
         * unsigned XSD data types should be conducted after a widening
         * conversion. For example, operations on <code>xsd:unsignedShort</code>
         * should be performed using {@link #intValue()}.
         */
        abstract public short shortValue();

        /**
         * Return the <code>int</code> value of <i>this</i> value.
         * <p>
         * Note: Java lacks unsigned data types. For safety, operations on
         * unsigned XSD data types should be conducted after a widening
         * conversion. For example, operations on <code>xsd:unsignedInt</code>
         * should be performed using {@link #longValue()}.
         */
        abstract public int intValue();

        /**
         * Return the <code>long</code> value of <i>this</i> value.
         * <p>
         * Note: Java lacks unsigned data types. For safety, operations on
         * unsigned XSD data types should be conducted after a widening
         * conversion. For example, operations on <code>xsd:unsignedLong</code>
         * should be performed using {@link #integerValue()}.
         */
        abstract public long longValue();

        /** Return the <code>float</code> value of <i>this</i> value. */
        abstract public float floatValue();

        /** Return the <code>double</code> value of <i>this</i> value. */
        abstract public double doubleValue();

        /** Return the {@link BigInteger} value of <i>this</i> value. */
        abstract public BigInteger integerValue();

        /** Return the {@link BigDecimal} value of <i>this</i> value. */
        abstract public BigDecimal decimalValue();

    }

    /** Implementation for inline <code>xsd:boolean</code>. */
    static public class XSDBooleanInternalValue<V extends BigdataLiteral> extends
            AbstractDatatypeLiteralInternalValue<V, Boolean> {

        static public transient final XSDBooleanInternalValue<BigdataLiteral> TRUE = new XSDBooleanInternalValue<BigdataLiteral>(
                true);

        static public transient final XSDBooleanInternalValue<BigdataLiteral> FALSE = new XSDBooleanInternalValue<BigdataLiteral>(
                false);
        
        private final boolean value;

        public XSDBooleanInternalValue(final boolean value) {
            
            super(InternalDataTypeEnum.XSDBoolean);
            
            this.value = value;
            
        }

        final public Boolean getInlineValue() {

            return value ? Boolean.TRUE : Boolean.FALSE;

        }

        @SuppressWarnings("unchecked")
        public V asValue(final BigdataValueFactory f) {
            return (V) f.createLiteral(value);
        }

        @Override
        final public long longValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean booleanValue() {
            return value;
        }

        @Override
        public byte byteValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public double doubleValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public float floatValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int intValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public short shortValue() {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public String stringValue() {
            return Boolean.toString(value);
        }

        @Override
        public BigDecimal decimalValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BigInteger integerValue() {
            throw new UnsupportedOperationException();
        }

        public boolean equals(final Object o) {
            if(this==o) return true;
            if(o instanceof XSDBooleanInternalValue) {
                return this.value == ((XSDBooleanInternalValue) o).value;
            }
            return false;
        }
        
        /**
         * Return the hash code of the byte value.
         * 
         * @see Boolean#hashCode()
         */
        public int hashCode() {
            return value ? Boolean.TRUE.hashCode() : Boolean.FALSE.hashCode();
        }

    }

    /** Implementation for inline <code>xsd:byte</code>. */
    static public class XSDByteInternalValue<V extends BigdataLiteral> extends
            AbstractDatatypeLiteralInternalValue<V, Byte> {

        private final byte value;

        public XSDByteInternalValue(final byte value) {
            
            super(InternalDataTypeEnum.XSDByte);
            
            this.value = value;
            
        }

        final public Byte getInlineValue() {
            
            return value;
            
        }

        @SuppressWarnings("unchecked")
        public V asValue(final BigdataValueFactory f) {
            return (V) f.createLiteral(value);
        }

        @Override
        final public long longValue() {
            return (long) value;
        }

        @Override
        public boolean booleanValue() {
            return value == 0 ? false : true;
        }

        @Override
        public byte byteValue() {
            return value;
        }

        @Override
        public double doubleValue() {
            return (double) value;
        }

        @Override
        public float floatValue() {
            return (float) value;
        }

        @Override
        public int intValue() {
            return (int)value;
        }

        @Override
        public short shortValue() {
            return (short) value;
        }
        
        @Override
        public String stringValue() {
            return Byte.toString(value);
        }

        @Override
        public BigDecimal decimalValue() {
            return BigDecimal.valueOf(value);
        }

        @Override
        public BigInteger integerValue() {
            return BigInteger.valueOf(value);
        }

        public boolean equals(final Object o) {
            if(this==o) return true;
            if(o instanceof XSDByteInternalValue) {
                return this.value == ((XSDByteInternalValue) o).value;
            }
            return false;
        }
        
        /**
         * Return the hash code of the byte value.
         * 
         * @see Byte#hashCode()
         */
        public int hashCode() {
            return (int) value;
        }

    }

    /** Implementation for inline <code>xsd:short</code>. */
    static public class XSDShortInternalValue<V extends BigdataLiteral> extends
            AbstractDatatypeLiteralInternalValue<V, Short> {

        private final short value;

        public XSDShortInternalValue(final short value) {
            
            super(InternalDataTypeEnum.XSDShort);
            
            this.value = value;
            
        }

        final public Short getInlineValue() {
            
            return value;
            
        }

        @SuppressWarnings("unchecked")
        public V asValue(final BigdataValueFactory f) {
            return (V) f.createLiteral(value);
        }

        @Override
        final public long longValue() {
            return (long) value;
        }

        @Override
        public boolean booleanValue() {
            return value == 0 ? false : true;
        }

        @Override
        public byte byteValue() {
            return (byte) value;
        }

        @Override
        public double doubleValue() {
            return (double) value;
        }

        @Override
        public float floatValue() {
            return (float) value;
        }

        @Override
        public int intValue() {
            return (int)value;
        }

        @Override
        public short shortValue() {
            return value;
        }
        
        @Override
        public String stringValue() {
            return Short.toString(value);
        }

        @Override
        public BigDecimal decimalValue() {
            return BigDecimal.valueOf(value);
        }

        @Override
        public BigInteger integerValue() {
            return BigInteger.valueOf(value);
        }

        public boolean equals(final Object o) {
            if(this==o) return true;
            if(o instanceof XSDShortInternalValue) {
                return this.value == ((XSDShortInternalValue) o).value;
            }
            return false;
        }
        
        /**
         * Return the hash code of the short value.
         * 
         * @see Short#hashCode()
         */
        public int hashCode() {
            return (int) value;
        }

    }

    /** Implementation for inline <code>xsd:int</code>. */
    static public class XSDIntInternalValue<V extends BigdataLiteral> extends
            AbstractDatatypeLiteralInternalValue<V, Integer> {

        private final int value;

        public XSDIntInternalValue(final int value) {
            
            super(InternalDataTypeEnum.XSDInt);
            
            this.value = value;
            
        }

        final public Integer getInlineValue() {
            
            return value;
            
        }

        @SuppressWarnings("unchecked")
        public V asValue(final BigdataValueFactory f) {
            return (V) f.createLiteral(value);
        }

        @Override
        final public long longValue() {
            return (long) value;
        }

        @Override
        public boolean booleanValue() {
            return value == 0 ? false : true;
        }

        @Override
        public byte byteValue() {
            return (byte) value;
        }

        @Override
        public double doubleValue() {
            return (double) value;
        }

        @Override
        public float floatValue() {
            return (float) value;
        }

        @Override
        public int intValue() {
            return value;
        }

        @Override
        public short shortValue() {
            return (short) value;
        }
        
        @Override
        public String stringValue() {
            return Integer.toString(value);
        }

        @Override
        public BigDecimal decimalValue() {
            return BigDecimal.valueOf(value);
        }

        @Override
        public BigInteger integerValue() {
            return BigInteger.valueOf(value);
        }

        public boolean equals(final Object o) {
            if(this==o) return true;
            if(o instanceof XSDIntInternalValue) {
                return this.value == ((XSDIntInternalValue) o).value;
            }
            return false;
        }
        
        /**
         * Return the hash code of the int value.
         * 
         * @see Integer#hashCode()
         */
        public int hashCode() {
            return value;
        }

    }

    /** Implementation for inline <code>xsd:long</code>. */
    static public class XSDLongInternalValue<V extends BigdataLiteral> extends
            AbstractDatatypeLiteralInternalValue<V, Long> {

        /**
         * 
         */
        private static final long serialVersionUID = -6972910330385311194L;
        
        private final long value;

        public XSDLongInternalValue(final long value) {
            
            super(InternalDataTypeEnum.XSDLong);
            
            this.value = value;
            
        }

        final public Long getInlineValue() {
            return value;
        }

        @SuppressWarnings("unchecked")
        public V asValue(final BigdataValueFactory f) {
            return (V) f.createLiteral(value);
        }

        @Override
        final public long longValue() {
            return value;
        }

        @Override
        public boolean booleanValue() {
            return value == 0 ? false : true;
        }

        @Override
        public byte byteValue() {
            return (byte) value;
        }

        @Override
        public double doubleValue() {
            return (double) value;
        }

        @Override
        public float floatValue() {
            return (float) value;
        }

        @Override
        public int intValue() {
            return (int) value;
        }

        @Override
        public short shortValue() {
            return (short) value;
        }
        
        @Override
        public String stringValue() {
            return Long.toString(value);
        }

        @Override
        public BigDecimal decimalValue() {
            return BigDecimal.valueOf(value);
        }

        @Override
        public BigInteger integerValue() {
            return BigInteger.valueOf(value);
        }

        public boolean equals(final Object o) {
            if(this==o) return true;
            if(o instanceof XSDLongInternalValue) {
                return this.value == ((XSDLongInternalValue) o).value;
            }
            return false;
        }
        
        /**
         * Return the hash code of the long value.
         */
        public int hashCode() {
            return (int) (value ^ (value >>> 32));
        }

    }

    /** Implementation for inline <code>xsd:float</code>. */
    static public class XSDFloatInternalValue<V extends BigdataLiteral> extends
            AbstractDatatypeLiteralInternalValue<V, Float> {

        /**
         * 
         */
        private static final long serialVersionUID = 2274203835967555711L;

        private final float value;

        public XSDFloatInternalValue(final float value) {
            
            super(InternalDataTypeEnum.XSDFloat);
            
            this.value = value;
            
        }

        final public Float getInlineValue() {
            return value;
        }

        @SuppressWarnings("unchecked")
        public V asValue(final BigdataValueFactory f) {
            return (V) f.createLiteral(value);
        }

        @Override
        final public float floatValue() {
            return value;
        }

        @Override
        public boolean booleanValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte byteValue() {
            return (byte) value;
        }

        @Override
        public double doubleValue() {
            return (double) value;
        }

        @Override
        public int intValue() {
            return (int) value;
        }

        @Override
        public long longValue() {
            return (long) value;
        }

        @Override
        public short shortValue() {
            return (short) value;
        }

        @Override
        public BigDecimal decimalValue() {
            return BigDecimal.valueOf(value);
        }

        @Override
        public BigInteger integerValue() {
            return BigInteger.valueOf((long) value);
        }

        @Override
        public String stringValue() {
            return Float.toString(value);
        }
        
        public boolean equals(final Object o) {
            if(this==o) return true;
            if(o instanceof XSDFloatInternalValue) {
                return this.value == ((XSDFloatInternalValue) o).value;
            }
            return false;
        }
        
        /**
         * Return the hash code of the float value.
         * 
         * @see Float#hashCode()
         */
        public int hashCode() {

            return Float.floatToIntBits(value);
            
        }

    }

    /** Implementation for inline <code>xsd:double</code>. */
    static public class XSDDoubleInternalValue<V extends BigdataLiteral> extends
            AbstractDatatypeLiteralInternalValue<V, Double> {

        private final double value;

        public XSDDoubleInternalValue(final double value) {
            
            super(InternalDataTypeEnum.XSDDouble);
            
            this.value = value;
            
        }

        final public Double getInlineValue() {
            return value;
        }

        @SuppressWarnings("unchecked")
        public V asValue(final BigdataValueFactory f) {
            return (V) f.createLiteral(value);
        }

        @Override
        final public float floatValue() {
            return (float) value;
        }

        @Override
        public boolean booleanValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte byteValue() {
            return (byte) value;
        }

        @Override
        public double doubleValue() {
            return value;
        }

        @Override
        public int intValue() {
            return (int) value;
        }

        @Override
        public long longValue() {
            return (long) value;
        }

        @Override
        public short shortValue() {
            return (short) value;
        }

        @Override
        public BigDecimal decimalValue() {
            return BigDecimal.valueOf(value);
        }

        @Override
        public BigInteger integerValue() {
            return BigInteger.valueOf((long) value);
        }

        @Override
        public String stringValue() {
            return Double.toString(value);
        }
        
        public boolean equals(final Object o) {
            if(this==o) return true;
            if(o instanceof XSDDoubleInternalValue) {
                return this.value == ((XSDDoubleInternalValue) o).value;
            }
            return false;
        }
        
        /**
         * Return the hash code of the double value.
         * 
         * @see Double#hashCode()
         */
        public int hashCode() {

            final long t = Double.doubleToLongBits(value);

            return (int) (t ^ (t >>> 32));

        }

    }

    /** Implementation for inline <code>xsd:integer</code>. */
    static public class XSDIntegerInternalValue<V extends BigdataLiteral> extends
            AbstractDatatypeLiteralInternalValue<V, BigInteger> {
        
        private final BigInteger value;

        public XSDIntegerInternalValue(final BigInteger value) {
            
            super(InternalDataTypeEnum.XSDInteger);

            if (value == null)
                throw new IllegalArgumentException();
            
            this.value = value;
            
        }

        final public BigInteger getInlineValue() {

            return value;
            
        }

        @SuppressWarnings("unchecked")
        public V asValue(final BigdataValueFactory f) {
            // @todo factory should cache the XSD URIs.
            return (V) f.createLiteral(value.toString(),//
                    f.createURI(InternalDataTypeEnum.XSDInteger.getDatatype()));
        }

        @Override
        final public long longValue() {
            return value.longValue();
        }

        @Override
        public boolean booleanValue() {
            return value.equals(BigInteger.ZERO) ? false : true;
        }

        @Override
        public byte byteValue() {
            return value.byteValue();
        }

        @Override
        public double doubleValue() {
            return value.doubleValue();
        }

        @Override
        public float floatValue() {
            return value.floatValue();
        }

        @Override
        public int intValue() {
            return value.intValue();
        }

        @Override
        public short shortValue() {
            return value.shortValue();
        }
        
        @Override
        public String stringValue() {
            return value.toString();
        }

        @Override
        public BigDecimal decimalValue() {
            return new BigDecimal(value);
        }

        @Override
        public BigInteger integerValue() {
            return value;
        }

        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (o instanceof XSDIntegerInternalValue) {
                return this.value.equals(((XSDIntegerInternalValue) o).value);
            }
            return false;
        }

        /**
         * Return the hash code of the {@link BigInteger}.
         */
        public int hashCode() {
            return value.hashCode();
        }

    }

    /**
     * Implementation for inline {@link UUID}s (there is no corresponding XML
     * Schema Datatype).
     */
    static public class UUIDInternalValue<V extends BigdataLiteral> extends
            AbstractDatatypeLiteralInternalValue<V, UUID> {
        
        private final UUID value;

        public UUIDInternalValue(final UUID value) {
            
            super(InternalDataTypeEnum.UUID);

            if (value == null)
                throw new IllegalArgumentException();
            
            this.value = value;
            
        }

        final public UUID getInlineValue() {
            return value;
        }

        @SuppressWarnings("unchecked")
        public V asValue(final BigdataValueFactory f) {
            return (V) f.createLiteral(value.toString(), //
                    f.createURI(InternalDataTypeEnum.UUID.getDatatype()));
        }

        @Override
        final public long longValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean booleanValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte byteValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public double doubleValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public float floatValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int intValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public short shortValue() {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public String stringValue() {
            return value.toString();
        }

        @Override
        public BigDecimal decimalValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BigInteger integerValue() {
            throw new UnsupportedOperationException();
        }

        public boolean equals(final Object o) {
            if(this==o) return true;
            if(o instanceof UUIDInternalValue) {
                return this.value.equals(((UUIDInternalValue) o).value);
            }
            return false;
        }
        
        /**
         * Return the hash code of the {@link UUID}.
         */
        public int hashCode() {
            return value.hashCode();
        }

    }

}

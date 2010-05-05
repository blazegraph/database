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

import java.math.BigInteger;
import java.util.Random;
import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;

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
     */
    private void encodeValue(final IKeyBuilder keyBuilder,
            final InternalValue<?, ?> v) {

        keyBuilder.append(v.flags());

        /*
         * Append the natural value type representation.
         * 
         * Note: We have to handle the unsigned byte, short, int and long values
         * specially to get the correct total key order.
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
             * FIXME Construct the InternalValue objects using factory since we
             * will have to scope how the RDF Value is represented to the
             * lexicon relation with which it is associated.
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
            case UUID: {
                final UUID x = KeyBuilder.decodeUUID(key, offset);
                offset += Bytes.SIZEOF_UUID;
                v = new UUIDInternalValue<BigdataLiteral>(x);
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
     * A factory for {@link InternalValue} objects.
     * <p>
     * Note: The behavior of the factory is informed by a
     * {@link LexiconConfiguration}. The factory will produce different
     * representations
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id: TestEncodeDecodeKeys.java 2760 2010-05-04 16:48:47Z
     *          thompsonbry $
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

}

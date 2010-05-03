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
 * @version $Id$
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
         * FIXME Add support to encode and decode unsigned short, int and long
         * to IKeyBuilder and KeyBuilder to support the unsigned xsd data types.
         */
        final InternalDataTypeEnum dte = v.getInternalDataTypeEnum();
        
        if (dte == InternalDataTypeEnum.TermId) {

            keyBuilder.append(v.getTermId());
            
            return;
            
        }
        
        final AbstractDatatypeLiteralInternalValue<?, ?> t = (AbstractDatatypeLiteralInternalValue<?, ?>) v;
        
        switch (dte) {
//        case TermId:
//            keyBuilder.append(v.getTermId());
//            break;
        case XSDBoolean:
            keyBuilder.append(t.booleanValue());
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
    public InternalValue<?, ?>[] decodeStatement(final byte[] key) {
        
        final InternalValue<?,?>[] a = new InternalValue[4];

        // The byte offset into the key.
        int offset = 0;
        
        for (int i = 0; i < 4; i++) {

            final byte flags = KeyBuilder.decodeByte(key[offset]);
            offset++;

            // The value type (URI, Literal, BNode, SID)
            final InternalValueTypeEnum vte = AbstractInternalValue.getInternalValueTypeEnum(flags);
            
            // The data type
            final InternalDataTypeEnum dte = AbstractInternalValue.getInternalDataTypeEnum(flags);

            if (dte == InternalDataTypeEnum.TermId) {

                // decode the term identifier.
                final long termId = KeyBuilder.decodeLong(key, offset);
                offset += Bytes.SIZEOF_LONG;
                
                // @todo type specific factory is required!
                a[i] = InternalValueTypeFactory.INSTANCE.newTermId(vte, dte,
                        termId);

            } else {
                throw new UnsupportedOperationException("vte=" + vte + ", dte="
                        + dte);
            }

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
     * Unit test for encoding and decoding a statement formed from
     * {@link TermId}s.
     * 
     * FIXME Finish the encode/decode statement key logic.
     */
    public void test_SPO_encodeDecode_001() {

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

        final byte[] key = encodeStatement(new KeyBuilder(), e[0], e[1], e[2],
                e[3]);

        final InternalValue<?, ?>[] a = decodeStatement(key);

        for (int i = 0; i < e.length; i++) {

            assertEquals("index=" + Integer.toString(i), e[i], a[i]);

        }
        
    }

    
    /**
     * Unit test verifies the ability to decode a key with an inline variable
     * length coding of a datatype literal using <code>xsd:integer</code>, aka
     * {@link BigInteger}.
     */
    public void test_SPO_encodeDecode_BigInteger() {

        final Long s = 1L;
        final Long p = 2L;
        final BigInteger o = BigInteger.valueOf(3L);
        final Long c = 3L;

        final byte[] key = new KeyBuilder().append(s).append(p).append(o)
                .append(c).getKey();

        int off = 0;
        
        final Long e_s = KeyBuilder.decodeLong(key, off);
        off += Bytes.SIZEOF_LONG;
        
        final Long e_p = KeyBuilder.decodeLong(key, off);
        off += Bytes.SIZEOF_LONG;
        
        final BigInteger e_o;
        {
            final byte[] b = KeyBuilder.decodeBigInteger2(off, key);
            off += 2 + b.length;
            e_o = new BigInteger(b);
        }

        final Long e_c = KeyBuilder.decodeLong(key, off);
        off += Bytes.SIZEOF_LONG;

        assertEquals("s", s, e_s);
        assertEquals("p", p, e_p);
        assertEquals("o", o, e_o);
        assertEquals("c", c, e_c);
        
    }

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

    /** Implementation for inline <code>xsd:long</code>. */
    static public class LongInternalValue<V extends BigdataLiteral> extends
            AbstractDatatypeLiteralInternalValue<V, Long> {

        /**
         * 
         */
        private static final long serialVersionUID = -6972910330385311194L;
        
        private final long value;

        public LongInternalValue(final long value) {
            
            super(InternalDataTypeEnum.XSDLong);
            
            this.value = value;
            
        }

        final public Long getInlineValue() {
            return value;
        }

        /*
         * FIXME Reconcile with BigdataValueImpl and BigdataValue. The role of
         * the valueFactory reference on BigdataValueImpl was to detect when an
         * instance was created by another value factory. The choice of whether
         * or not to inline the value is determined by the lexicon
         * configuration, and that choice is probably captured by a
         * BigdataValueFactory configuration object.
         */
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
            return value == 0 ? Boolean.FALSE : true; // @todo SPARQL semantics?
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
            if(o instanceof LongInternalValue) {
                return this.value == ((LongInternalValue) o).value;
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
    static public class FloatInternalValue<V extends BigdataLiteral> extends
            AbstractDatatypeLiteralInternalValue<V, Float> {

        /**
         * 
         */
        private static final long serialVersionUID = 2274203835967555711L;

        private final float value;

        public FloatInternalValue(final float value) {
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
            if(o instanceof FloatInternalValue) {
                return this.value == ((FloatInternalValue) o).value;
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

    /**
     * Configuration determines which RDF Values are inlined into the statement
     * indices rather than being assigned term identifiers by the lexicon.
     */
    public interface BigdataInlineValueConfig {

        /**
         * <code>true</code> iff the fixed size numerics (<code>xsd:int</code>,
         * <code>xsd:short</code>, <code>xsd:float</code>, etc) and
         * <code>xsd:boolean</code> should be inlined.
         */
        public boolean isNumericInline();

        /**
         * <code>true</code> iff xsd:integer should be inlined.
         */
        public boolean isXSDIntegerInline();

        /**
         * <code>true</code> iff <code>xsd:decimal</code> should be inlined.
         * 
         * @todo This option is not yet supported.
         */
        public boolean isXSDDecimalInline();

        /**
         * <code>true</code> iff blank node identifiers should be inlined. This
         * is only possible when the blank node identifiers are internally
         * generated {@link UUID}s since otherwise they can be arbitrary Unicode
         * strings which, like text-based Literals, can not be inlined.
         * <p>
         * This option is NOT compatible with
         * {@link AbstractTripleStore.Options#STORE_BLANK_NODES}.
         * 
         * @todo Separate option to inlined SIDs?
         */
        public boolean isBlankNodeInline();

        /**
         * <code>true</code> if UUID values (other than blank nodes) should be
         * inlined.
         */
        public boolean isUUIDInline();

        /**
         * @todo Option to enable storing of long literals (over a configured
         *       threshold) as blob references. The TERM2ID index would have a
         *       hash function (MD5, SHA-1, SHA-2, etc) of the value and assign
         *       a termId. The ID2TERM index would map the termId to a blob
         *       reference. The blob data would be stored in the journal and
         *       migrate into index segments during overflow processing for
         *       scale-out.
         */
        public boolean isLongLiteralAsBlob();
        
    }

}

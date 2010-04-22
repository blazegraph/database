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

import java.io.Serializable;
import java.math.BigInteger;
import java.util.UUID;

import junit.framework.TestCase2;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.lexicon.ITermIdCodes;
import com.bigdata.rdf.lexicon.TermIdEncoder;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.TestSPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.ibm.icu.math.BigDecimal;

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
     * Unit test verifies the ability to decode a key with an inline variable
     * length coding of a datatype literal (xsd:integer, aka BigInteger).
     */
    public void test001() {

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
     * Map declares the code associated with each data type which can be
     * inlined. Inline values MUST be decodable and MUST have a natural order.
     * Whether or not the datatype capable of being inlined is actually inlined
     * is a configuration option for the lexicon. If a data type is not inlined,
     * then its code will be {@link #TermId}, which indicates that the "inline"
     * value is a term identifier which must be resolved against the ID2TERM
     * index to materialize the value.
     * <p>
     * The {@link InternalValueTypeCodes} as 4 distinctions (URI, Literal,
     * BlankNode, and SID) and is coded in the high 2 bits of a byte while the
     * {@link InternalDatatypeEnum} has 64 possible distinctions, which are
     * coded in the lower 6 bits of the same byte. Only a subset of those 64
     * possible distinctions for inline data types have been declared. The
     * remaining code values are reserved for future use.
     * <p>
     * Note: Unicode values CAN NOT be inlined because (a) Unicode sort keys are
     * not decodable; and (b) the collation rules for Unicode depend on the
     * lexicon configuration, which specifies parameters such as Locale,
     * Strength, etc.
     * <p>
     * Blanks nodes (their IDs are UUIDs) and datatypes with larger values
     * (UUIDs) or varying length values (xsd:integer, xsd:decimanl) can be
     * inlined. Whether it makes sense to do so is a question which trades off
     * redundancy in the statement indices for faster materialization of the
     * data type values and a smaller lexicon. UUIDs for purposes other than
     * blank nodes can also be inlined, however they will have a different
     * prefix to indicate that they are Literals rather than blank nodes.
     * <p>
     * Note: Datatypes which are multidimensional CAN be inlined. However, they
     * can not be placed into a total order which has meaningful semantics by a
     * single index. For example, geo:point describes a 2-dimensional location
     * and lacks any meaningful locality when inlined into the index. The only
     * reason to inline such data types is to avoid indirection through the
     * lexicon to materialize their values. Efficiently resolving points in a
     * region requires the use of a spatial index.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * 
     * @todo extensibility?
     * 
     * @todo geo:point?
     * 
     * @todo It is possible to inline ASCII values (URIs and Literals) if the
     *       lexicon is so configured. Those would be a varying length type.
     * 
     * @todo Above some configured size, long literals must be allocated in the
     *       lexicon, the inline value will be a termId, the key in the TERM2ID
     *       index will be an MD5 signature (to avoid problems with very long
     *       keys) and the blob reference will be stored in the ID2TERM tuple
     *       value. The blob itself is a raw record in the journal or index
     *       segment.
     * 
     * @todo support xsd:decimal coding in KeyBuilder by discarding the
     *       precision and only retaining the scale information. We need to
     *       record both the signum and the scale, and the scale needs to be
     *       adjusted to normalize for either zero or one digits before the
     *       decimal. This is nearly the same coding as {@link BigInteger}
     *       except that scale is not quite the same as runLength (it is
     *       runLength base 10, not base 256) so there might be fence posts
     *       there too.
     * 
     * @see http://www.w3.org/TR/xmlschema-2/
     * 
     * @todo replace [byte] with [int] to avoid problems with bit math on a
     *       signed byte?
     * 
     * @todo support the "unsigned" versions of xsd:byte, xsd:short, xsd:int,
     *       and xsd:long? We have enough bits to do this.
     * 
     * @todo Add support for extensible inline enumerations. Use one code value
     *       to indicate that this is an inline enumeration. Then another 2-4
     *       bytes to indicate which enumeration (registered somewhere) and then
     *       the code for the enumerated value. E.g., state codes, product
     *       codes, etc.
     */
    static public enum InternalDatatypeEnum {
        /**
         * The "inline" value is a term identifier which must be resolved
         * against the ID2TERM index in the lexicon in order to materialize the
         * corresponding RDF Value.
         */
        TermId((byte) 0x00, Bytes.SIZEOF_LONG, Long.class, null/* N/A */), //
        /**
         * The "inline" value is a boolean (xsd:boolean). Only the distinct
         * points in the xsd:boolean value space are represented. xsd:boolean
         * has multiple lexical forms which map onto "true" and "false". Those
         * distinctions are not preserved.
         */
        XSDBoolean((byte) 0x01, Bytes.SIZEOF_BYTE, Boolean.class,
                BigdataValueFactoryImpl.xsd + "boolean"), //
        /** The "inline" value is a signed byte (xsd:byte). */
        XSDByte((byte) 0x02, Bytes.SIZEOF_BYTE, Byte.class,
                BigdataValueFactoryImpl.xsd + "byte"), //
        /** The "inline" value is a signed short (xsd:short). */
        XSDShort((byte) 0x03, Bytes.SIZEOF_SHORT, Short.class,
                BigdataValueFactoryImpl.xsd + "short"), //
        /** The "inline" value is a signed 4 byte integer (xsd:int). */
        XSDInt((byte) 0x04, Bytes.SIZEOF_INT, Integer.class,
                BigdataValueFactoryImpl.xsd + "int"), //
        /**
         * The "inline" value is a single precision floating point number
         * (xsd:float).
         */
        XSDFloat((byte) 0x05, Bytes.SIZEOF_FLOAT, Float.class,
                BigdataValueFactoryImpl.xsd + "float"), //
        /** The "inline" value is a signed 8 byte integer (xsd:long). */
        XSDLong((byte) 0x06, Bytes.SIZEOF_LONG, Long.class,
                BigdataValueFactoryImpl.xsd + "long"), //
        /**
         * The "inline" value is a double precision floating point number
         * (xsd:double).
         */
        XSDDouble((byte) 0x07, Bytes.SIZEOF_DOUBLE, Double.class,
                BigdataValueFactoryImpl.xsd + "double"), //
        /**
         * The "inline" value is an xsd:integer, which is equivalent to
         * {@link BigInteger}.
         */
        XSDInteger((byte) 0x08, 0/* variable length */, BigInteger.class,
                BigdataValueFactoryImpl.xsd + "integer"), //
        /**
         * The "inline" value is an xsd:decimal. This is mostly equivalent to
         * {@link BigDecimal}, but unlike that Java class, xsd:decimal DOES NOT
         * preserve the precision of the value. (This fact is convenient for
         * indices since {@link BigDecimal} has, among other things, many
         * distinct representations of ZERO with different precision, etc. If we
         * had to represent the precision, we could not use xsd:decimal in an
         * index!)
         */
        XSDDecimal((byte) 0x09, 0/* variable length */, BigDecimal.class,
                BigdataValueFactoryImpl.xsd + "decimal"), //
        /**
         * The "inline" value is a {@link UUID}.
         * 
         * @todo URI for UUID data types?
         */
        UUID((byte) 0x0a, Bytes.SIZEOF_UUID, UUID.class, null/* N/A */), //
//      /**
//      * @todo reserved for xsd:gregorianCalendar, but I do not know //
//      *       yet whether its representation is fixed or varying in length.
//      */
//     XSDGregorianCalendar((byte) 0x10, -1,XMLGregorianCalendar.class), //
        ;
        
        private InternalDatatypeEnum(final byte b, final int len,
                final Class<?> cls, final String datatype) {
            this.v = b;
            this.len = len;
            this.cls = cls;
            this.datatype = datatype;
        }
        
        static final public InternalDatatypeEnum valueOf(final byte b) {
            /*
             * Note: This switch MUST correspond to the declarations above (you
             * can not made the cases of the switch from [v] since it is not
             * considered a to be constant by the compiler).
             */
            switch((MASK_BITS & b)) {
            case 0x00: return TermId;
            case 0x01: return XSDBoolean;
            case 0x02: return XSDByte;
            case 0x03: return XSDShort;
            case 0x04: return XSDInt;
            case 0x05: return XSDFloat;
            case 0x06: return XSDLong;
            case 0x07: return XSDDouble;
            case 0x08: return XSDInteger;
            case 0x09: return XSDDecimal;
            case 0x0a: return UUID;
//            case 0x0b: return XSDGregorianCalendar;
            default:
                throw new IllegalArgumentException(Byte.toString(b));
            }
        }
        
        /**
         * The code for the data type.
         */
        private final byte v;

        /**
         * The length of the inline value -or- ZERO (0) if the value has a
         * variable length (xsd:integer, xsd:decimal).
         */
        private final int len;

        /**
         * The class of the Java object used to represent instances of the coded
         * data type.
         */
        private final Class<?> cls;
        
        /**
         * The URI for the data type.
         */
        private final String datatype;

        /**
         * An <code>int</code> value whose lower 6 bits indicate the internal
         * data type (including {@link #TermId}).
         */
        final public int value() {
            return v;
        }
        
        /**
         * The length of the data type value when represented as a component in
         * an unsigned byte[] key -or- ZERO iff the key component has a variable
         * length for that data type.
         */
        final public int len() {
            return len;
        }

        /**
         * The class of the Java object used to represent instances of the coded
         * data type.
         */
        final public Class<?> getCls() {
            return cls;
        }

        /**
         * The string value of the corresponding datatype URI.
         */
        final public String getDatatype() {
            return datatype;
        }
        
        /**
         * The bit mask that is bit-wise ANDed with the flags byte in order to
         * reveal the code for the data type (the low SIX (6) bits of the mask
         * are set).
         */
        static final byte MASK_BITS = (byte) (0xbf & 0xff);

        /**
         * Return <code>true</code> iff the low bits of the byte value indicate
         * that the inline value is a term identifier and hence must be resolved
         * against the ID2TERM index to materialize the data type value.
         * 
         * @param b
         *            The byte coding the RDF Value type and its data type.
         * 
         * @return <code>true</code> iff the inline value is a term identifier
         *         (otherwise it will be some inlined data type value).
         */
        static final public boolean isTermId(final byte b) {
            return (MASK_BITS & b) == TermId.v;
        }
        
    }

    /**
     * Class with static methods for interpreting and setting the bit flags used
     * to identify the type of an RDF Value (URI, Literal, Blank Node, SID,
     * etc).
     * 
     * @todo This replaces {@link ITermIdCodes}.
     * 
     * @todo update {@link TermIdEncoder}. This encodes term identifiers for
     *       scale-out but moving some bits around. It will be simpler now that
     *       the term identifier is all bits in the long integer with an
     *       additional byte prefix to differentiate URI vs Literal vs BNode vs
     *       SID and to indicate the inline value type (termId vs everything
     *       else).
     * 
     * @todo Rework as an enum like {@link InternalDatatypeEnum}?
     * 
     * @todo add method to combine the value type codes and the data type codes
     *       into a single byte and write unit tests for that method (reworking
     *       the unit tests in {@link TestSPO}).
     */
    public static class InternalValueTypeCodes {

        /**
         * The bit mask that is bit-wise ANDed with a term identifier in order
         * to reveal the term code. The low high bits of the lower byte in the
         * mask are set. The high bytes are ignored (they are there just to make
         * it easier to deal with unsigned byte values in Java).
         * 
         * @see #TERMID_CODE_URI
         * @see #TERMID_CODE_BNODE
         * @see #TERMID_CODE_LITERAL
         * @see #TERMID_CODE_STATEMENT
         */
        static final public int TERMID_CODE_MASK = 0xC0;

//        /**
//         * The #of bits in the {@link #TERMID_CODE_MASK} (
//         * {@value #TERMID_CODE_MASK_BITS}).
//         */
//        static final public long TERMID_CODE_MASK_BITS = 2L;

        /**
         * The bit value used to indicate that a term identifier stands for a
         * {@link URI}.
         */
        static final public int TERMID_CODE_URI = 0x00 << 6;

        /**
         * The bit value used to indicate that a term identifier stands for a
         * {@link BNode}.
         */
        static final public int TERMID_CODE_BNODE = 0x01 << 6;

        /**
         * The bit value used to indicate that a term identifier stands for a
         * {@link Literal}.
         */
        static final public int TERMID_CODE_LITERAL = 0x02 << 6;

        /**
         * The bit value used to indicate that a term identifier stands for a
         * statement (when support for statement identifiers is enabled).
         */
        static final public int TERMID_CODE_STATEMENT = 0x03 << 6;

        /**
         * Return true iff the flags identify an RDF {@link Literal}.
         * <p>
         * Note: Some entailments require the ability to filter based on whether
         * or not a term is a literal. For example, literals may not be entailed
         * into the subject position. This method makes it possible to determine
         * whether or not a term is a literal without materializing the term,
         * thereby allowing the entailments to be computed purely within the
         * term identifier space.
         */
        static public boolean isLiteral(final long flags) {

            return (flags & TERMID_CODE_MASK) == TERMID_CODE_LITERAL;

        }

        /**
         * Return true iff the flags identify an RDF {@link BNode}.
         */
        static public boolean isBNode(final long flags) {

            return (flags & TERMID_CODE_MASK) == TERMID_CODE_BNODE;

        }

        /**
         * Return true iff the flags identify an RDF {@link URI}.
         */
        static public boolean isURI(final byte flags) {

            /*
             * Note: The additional != NULL test is necessary for a URI term
             * identifier so that it will not report 'true' for 0L since the two
             * bits used to mark a URI are both zero and the bits to indicate a
             * TermId are also zero.
             */

            return (flags & TERMID_CODE_MASK) == TERMID_CODE_URI
                    && flags != IRawTripleStore.NULL;

        }

        /**
         * Return true iff the flags identify a statement (this feature is
         * enabled with {@link Options#STATEMENT_IDENTIFIERS}).
         */
        static public boolean isStatement(final byte flags) {

            return (flags & TERMID_CODE_MASK) == TERMID_CODE_STATEMENT;

        }

        /**
         * Return a byte representing the combination of the specified value
         * type and data type.
         * 
         * @param vt
         *            The value type (URI, Literal, BNode, or SID).
         * @param dt
         *            The data type.
         * 
         * @todo How do we handle SIDs? (They are a special case of BNode.)
         */
        static public byte fuse(final Value vte,
                final InternalDatatypeEnum dte) {

            return (byte) ((((int) vte.v << 6) | dte.v) & 0xff);

        }

    }

    /**
     * Interface for the internal representation of an RDF Value (the
     * representation which is encoded within the statement indices).
     */
    public interface InternalValue<V extends BigdataValue, T> extends
            Serializable {

        /** <code>true</code> iff the RDF value is represented inline. */
        boolean isInline();

        /**
         * <code>true</code> iff the RDF value is represented by a term
         * identifier.
         */
        boolean isTermId();

        /**
         * <code>true</code> iff the RDF value is a term identifier whose value
         * is <code>0L</code>.
         */
        boolean isNull();

        /**
         * Return the term identifier.
         * 
         * @return The term identifier.
         * @throws UnsupportedOperationException
         *             unless the RDF value is represented by a term identifier.
         */
        long getTermId() throws UnsupportedOperationException;

        /**
         * Return the {@link InternalDatatypeEnum} for the {@link InternalValue}
         * . This will be {@link InternalDatatypeEnum#TermId} iff the internal
         * "value" is a term identifier. Otherwise it will be the type safe enum
         * corresponding to the specific data type which can be decoded from
         * this {@link InternalValue} using {@link #getInline()}.
         */
        InternalDatatypeEnum getInternalDatatype();

        /**
         * Return the Java {@link Object} corresponding to the inline value.
         * 
         * @return The {@link Object}.
         * @throws UnsupportedOperationException
         *             unless the RDF value is inline.
         */
        T getInline() throws UnsupportedOperationException;

        /**
         * Inflate an inline RDF value to a {@link BigdataValue}. This method
         * DOES NOT guarantee a singleton pattern for the inflated value and the
         * value factory. However, implementations are encouraged to cache the
         * inflated {@link BigdataValue} on a transient field.
         * 
         * @param f
         *            The value factory.
         * @return The corresponding {@link BigdataValue}.
         * @throws UnsupportedOperationException
         *             unless the RDF value is inline.
         */
        V asValue(BigdataValueFactory f) throws UnsupportedOperationException;
        
        /**
         * Return <code>true</code> iff this is an RDF Literal. Note that some
         * kinds of RDF Literals MAY be represented inline.
         */
        boolean isLiteral();

        /** Return <code>true</code> iff this is an RDF BlankNode. */
        boolean isBNode();

        /**
         * Return <code>true</code> iff this is an RDF {@link URI}.
         */
        boolean isURI();

        /**
         * Return <code>true</code> iff this is a statement identifier (this
         * feature is enabled with {@link Options#STATEMENT_IDENTIFIERS}).
         */
        boolean isStatement();

    }

    /**
     * Abstract base class for the inline representation of an RDF Value (the
     * representation which is encoded in to the keys of the statement indices).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * @param <V>
     *            The generic type for the RDF {@link Value} implementation.
     * @param <T>
     *            The generic type for the inline value.
     */
    public abstract class AbstractInternalValue<V extends BigdataValue, T>
            implements InternalValue<V, T> {

        /**
         * Bit flags indicating the kind of RDF Value and the interpretation of
         * the inline value (either as a term identifier or as some data type
         * value).
         * 
         * @see InternalValueTypeCodes
         * @see InternalDatatypeEnum
         */
        private final byte flags;

        private AbstractInternalValue(final byte flags) {
            this.flags = flags;
        }
        
        final public boolean isBNode() {
            return InternalValueTypeCodes.isBNode(flags);
        }

        final public boolean isLiteral() {
            return InternalValueTypeCodes.isLiteral(flags);
        }

        final public boolean isStatement() {
            return InternalValueTypeCodes.isStatement(flags);
        }

        final public boolean isURI() {
            return InternalValueTypeCodes.isURI(flags);
        }
        
        final public InternalDatatypeEnum getInternalDatatype() {
            return InternalDatatypeEnum.valueOf(flags);
        }

    }

    /**
     * Implementation for any kind of RDF Value when the values is not being
     * inlined. Instances of this class can represent URIs, Blank Nodes (if they
     * are not being inlined), Literals (including datatype literals if they are
     * not being inlined) or SIDs (statement identifiers).
     */
    public class TermId<V extends BigdataValue/* URI,BNode,Literal,SID */>
            extends AbstractInternalValue<V, Void> {

        /**
         * 
         */
        private static final long serialVersionUID = 4309045651680610931L;
        
        /** The term identifier. */
        private final long termId;

        public TermId(final byte flags, final long termId) {
            super(flags);
            this.termId = termId;
        }

        final public V asValue(BigdataValueFactory f)
                throws UnsupportedOperationException {
            throw new UnsupportedOperationException();
        }

        final public Void getInline() {
            throw new UnsupportedOperationException();
        }

        final public long getTermId() {
            return termId;
        }

        final public boolean isInline() {
            return false;
        }

        final public boolean isNull() {
            return termId != IRawTripleStore.NULL;
        }

        final public boolean isTermId() {
            return true;
        }

    }

    /**
     * Abstract base class for RDF datatype literals.
     * {@inheritDoc}
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    abstract public class AbstractDatatypeInternalValue<V extends BigdataLiteral, T>
            extends AbstractInternalValue<V, T> {

        public AbstractDatatypeInternalValue(final InternalDatatypeEnum datatype) {
            super((byte) (InternalValueTypeCodes.TERMID_CODE_LITERAL | datatype
                    .value()));
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

        /*
         * Primitive data type value access methods.
         * 
         * @todo What are the SPARQL semantics for casting among these
         * datatypes? They should probably be reflected here since that is the
         * real use case. I believe that those casts also require failing a
         * solution if the cast is not legal, in which case these methods might
         * not be all that useful.
         * 
         * Also see BigdataLiteralImpl and XMLDatatypeUtil. It handles the
         * conversions by reparsing, but there is no reason to do that here
         * since we have the canonical point in the value space.
         * 
         * @see http://www.w3.org/TR/rdf-sparql-query/#FunctionMapping, The
         * casting rules for SPARQL
         */

//        /** Cast to xsd:boolean. */
//        abstract public boolean booleanValue();
//
//        /** Cast to xsd:byte (aka signed byte). */
//        abstract public byte byteValue();
//
//        /** Cast to xsd:short. */
//        abstract public short shortValue();
//
//        /** Cast to xsd:int. */
//        abstract public int intValue();
//
//        /** Cast to xsd:float. */
//        abstract public float floatValue();
//
//        /** Cast to xsd:long. */
//        abstract public long longValue();
//
//        /** Cast to xsd:double. */
//        abstract public double doubleValue();
//
//        /** Cast to xsd:string. */
//        abstract public String stringValue();
        
    }

    /** Implementation for inline <code>xsd:long</code>. */
    public class LongInternalValue<V extends BigdataLiteral> extends
            AbstractDatatypeInternalValue<V, Long> {

        /**
         * 
         */
        private static final long serialVersionUID = -6972910330385311194L;
        
        private final long value;

        public LongInternalValue(final long value) {
            super(InternalDatatypeEnum.XSDLong);
            this.value = value;
        }

        final public Long getInline() {
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

//        @Override @todo override iff part of the abstract parent class.
        final public long longValue() {
            return value;
        }

//        @Override
//        public boolean booleanValue() {
//            return value == 0 ? Boolean.FALSE : true; // @todo SPARQL semantics?
//        }
//
//        @Override
//        public byte byteValue() {
//            return (byte) value;
//        }
//
//        @Override
//        public double doubleValue() {
//            return (double) value;
//        }
//
//        @Override
//        public float floatValue() {
//            return (float) value;
//        }
//
//        @Override
//        public int intValue() {
//            return (int) value;
//        }
//
//        @Override
//        public short shortValue() {
//            return (short) value;
//        }
//        
//        @Override
//        public String stringValue() {
//            return Long.toString(value);
//        }

    }

    /** Implementation for inline <code>xsd:float</code>. */
    public class FloatInternalValue<V extends BigdataLiteral> extends
            AbstractDatatypeInternalValue<V, Float> {

        /**
         * 
         */
        private static final long serialVersionUID = 2274203835967555711L;
        
        private final float value;

        public FloatInternalValue(final float value) {
            super(InternalDatatypeEnum.XSDFloat);
            this.value = value;
        }

        final public Float getInline() {
            return value;
        }

        @SuppressWarnings("unchecked")
        public V asValue(final BigdataValueFactory f) {
            return (V) f.createLiteral(value);
        }

//        @Override @todo override iff part of the abstract parent class.
        final public float floatValue() {
            return value;
        }

//        @Override
//        public boolean booleanValue() {
//            return value == 0 ? Boolean.FALSE : true; // @todo SPARQL semantics?
//        }
//
//        @Override
//        public byte byteValue() {
//            return (byte) value;
//        }
//
//        @Override
//        public double doubleValue() {
//            return (double) value;
//        }
//
//        @Override
//        public float floatValue() {
//            return (float) value;
//        }
//
//        @Override
//        public int intValue() {
//            return (int) value;
//        }
//
//        @Override
//        public short shortValue() {
//            return (short) value;
//        }
//        
//        @Override
//        public String stringValue() {
//            return Long.toString(value);
//        }

    }

    /**
     * Configuration determines which RDF Values are inlined into the statement
     * indices rather than being assigned term identifiers by the lexicon.
     */
    public interface BigdataInlineValueConfig {
        
        /**
         * <code>true</code> iff the fixed size numerics and xsd:boolean shoul
         * be inlined.
         */
        public boolean isNumericInline();

        /**
         * <code>true</code> iff xsd:integer and xsd:decimal should be inlined.
         */
        public boolean isBigNumericInline();

        /**
         * <code>true</code> iff blank node identifiers should be inlined.
         * However, the ID associated with a blank node will be the ID from the
         * RDF interchange syntax if
         * {@link AbstractTripleStore.Options#STORE_BLANK_NODES} AND you have
         * configured the RDF parser to preserve blank nodes.
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

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
 * Created on May 3, 2010
 */

package com.bigdata.rdf.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.UUID;

import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;

/**
 * Map declares the code associated with each data type which can be inlined.
 * Inline values MUST be decodable and MUST have a natural order. Whether or not
 * the datatype capable of being inlined is actually inlined is a configuration
 * option for the lexicon. If a data type is not inlined, then its code will be
 * {@link #TermId}, which indicates that the "inline" value is a term identifier
 * which must be resolved against the ID2TERM index to materialize the value.
 * <p>
 * The {@link InternalValueTypeEnum} as 4 distinctions (URI, Literal, BlankNode,
 * and SID) and is coded in the high 2 bits of a byte while the
 * {@link InternalDataTypeEnum} has 64 possible distinctions, which are coded in
 * the lower 6 bits of the same byte. Only a subset of those 64 possible
 * distinctions for inline data types have been declared. The remaining code
 * values are reserved for future use.
 * <p>
 * Note: Unicode values CAN NOT be inlined because (a) Unicode sort keys are not
 * decodable; and (b) the collation rules for Unicode depend on the lexicon
 * configuration, which specifies parameters such as Locale, Strength, etc.
 * <p>
 * Blanks nodes (their IDs are UUIDs) and datatypes with larger values (UUIDs)
 * or varying length values (xsd:integer, xsd:decimanl) can be inlined. Whether
 * it makes sense to do so is a question which trades off redundancy in the
 * statement indices for faster materialization of the data type values and a
 * smaller lexicon. UUIDs for purposes other than blank nodes can also be
 * inlined, however they will have a different prefix to indicate that they are
 * Literals rather than blank nodes.
 * <p>
 * Note: While multidimensional datatypes (such as points or rectangles) could
 * be inlined (in the sense that their values could be converted to unsigned
 * byte[] keys and the keys could be decoded), they can not be placed into a
 * total order which has meaningful semantics by a single index. For example,
 * geo:point describes a 2-dimensional location and lacks any meaningful
 * locality when inlined into the index. The only reason to inline such data
 * types is to avoid indirection through the lexicon to materialize their
 * values. Efficiently resolving points in a region requires the use of a
 * spatial index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestEncodeDecodeKeys.java 2753 2010-05-01 16:36:59Z thompsonbry
 *          $
 * 
 * @todo It is possible to inline ASCII values (URIs and Literals) if the
 *       lexicon is so configured. Those would be a varying length type.
 *       Alternatively, we could add a Comparator to the B+Tree so we could
 *       directly compare the (decoded) RDF Values in each statement. However,
 *       either approach would be challenged by long literals and the Comparator
 *       would require a DDL or something tricky like byte code to C code
 *       generation to be compatible with a GPU.
 * 
 * @todo Above some configured size, long literals must be allocated in the
 *       lexicon, the inline value will be a termId, the key in the TERM2ID
 *       index will be an MD5 signature (to avoid problems with very long keys)
 *       and the blob reference will be stored in the ID2TERM tuple value. The
 *       blob itself is a raw record in the journal or index segment.
 * 
 * @todo support xsd:decimal coding in KeyBuilder by discarding the precision
 *       and only retaining the scale information. We need to record both the
 *       signum and the scale, and the scale needs to be adjusted to normalize
 *       for either zero or one digits before the decimal. This is nearly the
 *       same coding as {@link BigInteger} except that scale is not quite the
 *       same as runLength (it is runLength base 10, not base 256) so there
 *       might be fence posts there too.
 * 
 * @see http://www.w3.org/TR/xmlschema-2/
 * 
 * @todo Add support for extensible inline enumerations. Use one code value to
 *       indicate that this is an inline enumeration. Then another 2-4 bytes to
 *       indicate which enumeration (registered somewhere) and then the code for
 *       the enumerated value. E.g., state codes, product codes, etc.
 * 
 * @todo Consider supporting derived types by using the appropriate type from
 *       this enumeration for the code and then recognizing the specific
 *       datatype from an inlined termId for the datatype URI. That would take
 *       one bit from our 6 for "derived", leaving us with 32 base distinctions.
 *       The statements would wind up clustered by the distinction unless we did
 *       something to prevent that.
 *       <p>
 *       If we also always encode the datatype bits, then we have to reserve one
 *       bit for the (termId | inline) distinction which leaves us with only 4
 *       bits for the inline datatype bits, which is just 16 distinctions. That
 *       is enough for the distinctions that we have right now, but it is close
 *       (we have 14 distinctions right now if you exclude TermId). <code>
 *       [termId|inline] : 1
 *       [datatype] : 5 (32 distinctions).
 *       </code> where one datatype code is "extension". If the DTE code is
 *       "extension", then the next 8 bytes are the termId of the datatype URI,
 *       followed by one byte giving the run length of the code points, and
 *       finally [runLength] bytes encoding the code point for a value in the
 *       value space for that specific datatype URI. <code>
 *       [datatypeTermId] : 8 bytes
 *       [runLength] : 1 byte
 *       [codePoint] : [runLength] bytes.
 *       </code>
 *       <p>
 *       While this is not nearly as compact as a two character state code, it
 *       does allow application extensible inlining of critical value types.
 *       <p>
 *       If those code points are assigned in the desired sort order for the
 *       enumerated type, then we can also optimize range queries in the
 *       application specific data types by first projecting the query onto the
 *       code points for the value space of that datatype.
 *       <p>
 *       Another perspective is that the kind of value spaces which correspond
 *       to enumerations are going to be rapidly cached if they are heavily used
 *       so the only benefit might be range queries inside of the value space
 *       and those could also be enabled by enumeration of the points in the
 *       value space which lie in the desired range.
 *       <p>
 *       Given that, yet another proposal would be to reserve one bit to
 *       indicate that the termId of the data type follows the flags byte:<code>
 *       [termId|inline] : 1
 *       [datatypeId] : 1 (when true, the actual datatype URO is different and is inline) 
 *       [datatypeCode] : 4 (16 distinctions).
 *       </code>
 *       <p>
 *       When the datatypeId bit is set, the next field is the termId of the
 *       datatype URI, which is then followed by the representation of the point
 *       in the value space for that datatype URI using the representation of
 *       the specified datatypeCode. For example, if you have a datatype which
 *       specializes <code>xsd:long</code> to create <code>bd:millis</code>, the
 *       datatypeId would partition the index key space based on the actual
 *       datatypeURI while the datatypeCode would specify the manner in which
 *       that value space was captured within the inline representation. (This
 *       relationship would be available from the derivation of the datatype.)
 *       <p>
 *       This last proposal relies on there being a very limited number of
 *       interesting codings for things, basically correspond to the signed and
 *       unsigned primitive data types, xsd:integer, xsd:decimal, xsd:boolean,
 *       and maybe one or two others (since we are nearly out of bits with only
 *       16 distinctions!). [We could include a pattern for N ASCII or UTF-32
 *       characters and capture state codes that way.]
 */
public enum InternalDataTypeEnum {

    /**
     * The "inline" value is a term identifier which must be resolved against
     * the ID2TERM index in the lexicon in order to materialize the
     * corresponding RDF Value.
     * 
     * @todo URI for internal term identifiers? (probably not).
     */
    TermId((byte) 0, Bytes.SIZEOF_LONG, Long.class, null/* N/A */,
            DTEFlags.NOFLAGS), //

    /**
     * The "inline" value is a boolean (xsd:boolean). Only the distinct points
     * in the xsd:boolean value space are represented. xsd:boolean has multiple
     * lexical forms which map onto "true" and "false". Those distinctions are
     * not preserved.
     */
    XSDBoolean((byte) 1, Bytes.SIZEOF_BYTE, Boolean.class,
            BigdataValueFactoryImpl.xsd + "boolean", DTEFlags.NOFLAGS), //

    /** The "inline" value is a signed byte (xsd:byte). */
    XSDByte((byte) 2, Bytes.SIZEOF_BYTE, Byte.class,
            BigdataValueFactoryImpl.xsd + "byte", DTEFlags.NUMERIC), //

    /** The "inline" value is a signed short (xsd:short). */
    XSDShort((byte) 3, Bytes.SIZEOF_SHORT, Short.class,
            BigdataValueFactoryImpl.xsd + "short", DTEFlags.NUMERIC), //

    /** The "inline" value is a signed 4 byte integer (xsd:int). */
    XSDInt((byte) 4, Bytes.SIZEOF_INT, Integer.class,
            BigdataValueFactoryImpl.xsd + "int", DTEFlags.NUMERIC), //

    /**
     * The "inline" value is a single precision floating point number
     * (xsd:float).
     */
    XSDFloat((byte) 5, Bytes.SIZEOF_FLOAT, Float.class,
            BigdataValueFactoryImpl.xsd + "float", DTEFlags.NUMERIC), //

    /** The "inline" value is a signed 8 byte integer (xsd:long). */
    XSDLong((byte) 6, Bytes.SIZEOF_LONG, Long.class,
            BigdataValueFactoryImpl.xsd + "long", DTEFlags.NUMERIC), //

    /**
     * The "inline" value is a double precision floating point number
     * (xsd:double).
     */
    XSDDouble((byte) 7, Bytes.SIZEOF_DOUBLE, Double.class,
            BigdataValueFactoryImpl.xsd + "double", DTEFlags.NUMERIC), //

    /**
     * The "inline" value is an xsd:integer, which is equivalent to
     * {@link BigInteger}.
     */
    XSDInteger((byte) 8, 0/* variable length */, BigInteger.class,
            BigdataValueFactoryImpl.xsd + "integer", DTEFlags.NUMERIC), //

    /**
     * The "inline" value is an xsd:decimal. This is mostly equivalent to
     * {@link BigDecimal}, but unlike that Java class, xsd:decimal DOES NOT
     * preserve the precision of the value. (This fact is convenient for indices
     * since {@link BigDecimal} has, among other things, many distinct
     * representations of ZERO with different precision, etc. If we had to
     * represent the precision, we could not use xsd:decimal in an index!)
     */
    XSDDecimal((byte) 9, 0/* variable length */, BigDecimal.class,
            BigdataValueFactoryImpl.xsd + "decimal", DTEFlags.NUMERIC), //

    /**
     * The "inline" value is a {@link UUID}.
     * 
     * @see http://lists.xml.org/archives/xml-dev/201003/msg00027.html
     * 
     * @todo URI for UUID data types?
     */
    UUID((byte) 10, Bytes.SIZEOF_UUID, UUID.class, null/* N/A */,
            DTEFlags.NOFLAGS), //

    /** The "inline" value is an unsigned byte (xsd:unsignedByte). */
    XSDUnsignedByte((byte) 11, Bytes.SIZEOF_BYTE, Byte.class,
            BigdataValueFactoryImpl.xsd + "unsignedByte",
            DTEFlags.UNSIGNED_NUMERIC), //

    /** The "inline" value is a unsigned short (xsd:unsignedShort). */
    XSDUnsignedShort((byte) 12, Bytes.SIZEOF_SHORT, Short.class,
            BigdataValueFactoryImpl.xsd + "unsignedShort",
            DTEFlags.UNSIGNED_NUMERIC), //

    /** The "inline" value is an unsigned 4 byte integer (xsd:unsignedInt). */
    XSDUnsignedInt((byte) 13, Bytes.SIZEOF_INT, Integer.class,
            BigdataValueFactoryImpl.xsd + "unsignedInt",
            DTEFlags.UNSIGNED_NUMERIC), //

    /** The "inline" value is an unsigned 8 byte integer (xsd:unsignedLong). */
    XSDUnsignedLong((byte) 14, Bytes.SIZEOF_LONG, Long.class,
            BigdataValueFactoryImpl.xsd + "unsignedLong",
            DTEFlags.UNSIGNED_NUMERIC), //

    // /**
    // * @todo reserved for xsd:gregorianCalendar, but I do not know //
    // * yet whether its representation is fixed or varying in length.
    // */
    // XSDGregorianCalendar((byte) ..., -1,XMLGregorianCalendar.class), //
    ;

    /**
     * @param v
     *            The code for the data type.
     * @param len
     *            The length of the inline value -or- ZERO (0) if the value has
     *            a variable length (xsd:integer, xsd:decimal).
     * @param cls
     *            The class of the Java object used to represent instances of
     *            the coded data type.
     * @param datatype
     *            The string value of the well-known URI for the data type.
     * @param flags
     *            Some bit flags. See {@link #NUMERIC},
     *            {@link #UNSIGNED_NUMERIC}, etc.
     */
    private InternalDataTypeEnum(final byte v, final int len,
            final Class<?> cls, final String datatype, final int flags) {
        this.v = v;
        this.len = len;
        this.cls = cls;
        this.datatype = datatype;
        this.flags = flags;
    }

    static final public InternalDataTypeEnum valueOf(final byte b) {
        /*
         * Note: This switch MUST correspond to the declarations above (you can
         * not made the cases of the switch from [v] since it is not considered
         * a to be constant by the compiler).
         */
        switch ((MASK_BITS & b)) {
        case 0:
            return TermId;
        case 1:
            return XSDBoolean;
        case 2:
            return XSDByte;
        case 3:
            return XSDShort;
        case 4:
            return XSDInt;
        case 5:
            return XSDFloat;
        case 6:
            return XSDLong;
        case 7:
            return XSDDouble;
        case 8:
            return XSDInteger;
        case 9:
            return XSDDecimal;
        case 10:
            return UUID;
        case 11:
            return XSDUnsignedByte;
        case 12:
            return XSDUnsignedShort;
        case 13:
            return XSDUnsignedInt;
        case 14:
            return XSDUnsignedLong;
            // case ...: return XSDGregorianCalendar;
        default:
            throw new IllegalArgumentException(Byte.toString(b));
        }
    }

    /**
     * The code for the data type.
     */
    final byte v;

    /**
     * The length of the inline value -or- ZERO (0) if the value has a variable
     * length (xsd:integer, xsd:decimal).
     */
    private final int len;

    /**
     * The class of the Java object used to represent instances of the coded
     * data type.
     * 
     * @todo cls extends {@link AbstractInternalValue}? Use {@link TermId} for a
     *       term identifier? If blank nodes are inlined, then use a special
     *       class for that, otherwise use {@link TermId}? Ditto for SIDs. URIs
     *       are never inlined. Datatype literals MAY be inline, depending on
     *       the datatype. The unsigned datatypes need to have special handling
     *       to avoid problems with math on unsigned values. That special
     *       handling could be provided by appropriate subclasses of
     *       {@link AbstractDatatypeLiteralInternalValue}.
     */
    private final Class<?> cls;

    /**
     * The string value of the well-known URI for the data type.
     */
    private final String datatype;

    /**
     * Some bit flags.
     * 
     * @see #NUMERIC
     * @see #UNSIGNED_NUMERIC
     */
    private final int flags;

    /**
     * An <code>byte</code> value whose whose lower 6 bits code the
     * {@link InternalDataTypeEnum}.
     */
    final public byte v() {
        return v;
    }

    /**
     * The length of the data type value when represented as a component in an
     * unsigned byte[] key -or- ZERO iff the key component has a variable length
     * for that data type.
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
     * reveal the code for the data type (the low SIX (6) bits of the mask are
     * set).
     */
    private static final byte MASK_BITS = (byte) (0xbf & 0xff);

    /**
     * <code>true</code> for any of the numeric data types (xsd:byte,
     * xsd:unsignedByte, xsd:short, xsd:unsignedShort, xsd:int, xsd:unsignedInt,
     * xsd:long, xsd:unsignedLong, xsd:float, xsd:double, xsd:integer, and
     * xsd:decimal).
     */
    public boolean isNumeric() {

        return (flags & DTEFlags.NUMERIC) != 0;
        
    }

    /**
     * <code>true</code> for an unsigned numeric datatype ( xsd:unsignedByte,
     * xsd:unsignedShort, xsd:unsignedInt, xsd:unsignedLong).
     */
    public boolean isUnsignedNumeric() {
        
        return (flags & DTEFlags.UNSIGNED_NUMERIC) != 0;
        
    }

    /**
     * This is <code>!isBigNumeric()</code> and is <code>true</code> for any of
     * the fixed length numeric data types (xsd:byte, xsd:unsignedByte,
     * xsd:short, xsd:unsignedShort, xsd:int, xsd:unsignedInt, xsd:long,
     * xsd:unsignedLong, xsd:float, xsd:double).
     */
    public boolean isShortNumeric() {
        
        return (flags & DTEFlags.NUMERIC) != 0 && len != 0;
        
    }

    /**
     * <code>true</code> for xsd:integer and xsd:decimal.
     */
    public boolean isBigNumeric() {
        
        return (flags & DTEFlags.NUMERIC) != 0 && len == 0;
        
    }

}

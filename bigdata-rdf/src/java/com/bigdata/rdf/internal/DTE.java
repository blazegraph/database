/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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

import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.internal.impl.BlobIV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Data Type Enumeration (DTE) is a class which declares the known intrinsic
 * data types, provides for extensibility to new data types, and provides for
 * data types which either can not be inlined or are not being inlined. The
 * intrinsic data types are those having natural orders which can be encoded
 * into an unsigned byte[] key and decoded without loss. Whether or not a given
 * data type is actually inlined is a configuration option for the lexicon.
 * <p>
 * If a data type is not inlined, then the representation of the value must be
 * materialized. Non-inline values are {@link BlobIV}s and are materialized by
 * looking {@link IV} in the TERMS index. More recently, we are also permitting
 * indirection from the TERMS index. For example, very large objects may be
 * stored in the file system, in S3, etc. In those cases, you must still resolve
 * the {@link IV} against the TERMS index. Now the resulting object may be a
 * {@link BigdataValue}. However, the object materialized from the TERMS index
 * may also provide indirection into the file system, S3, etc.
 * <p>
 * The {@link VTE} has 4 distinctions (URI, Literal, BlankNode, and SID) and is
 * coded in the high 2 bits of a byte while the {@link DTE} has 16 possible
 * distinctions, one of which is reserved against future use and one of which is
 * reserved against extensibility in the set of intrinsic types.
 * <p>
 * Note: Unicode values CAN NOT be inlined because (a) Unicode sort keys are not
 * decodable; and (b) the collation rules for Unicode depend on the lexicon
 * configuration, which specifies parameters such as Locale, Strength, etc.
 * <p>
 * Blanks nodes (their IDs are UUIDs) and data types with larger values (UUIDs)
 * or varying length values (xsd:integer, xsd:decimanl) can be inlined. Whether
 * it makes sense to do so is a question which trades off redundancy in the
 * statement indices for faster materialization of the data type values and a
 * smaller lexicon. UUIDs for purposes other than blank nodes can also be
 * inlined, however they will have a different prefix to indicate that they are
 * Literals rather than blank nodes.
 * <p>
 * Note: While multidimensional data types (such as points or rectangles) could
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
 * @see http://www.w3.org/TR/xmlschema-2/
 */
public enum DTE {

    /**
     * The "inline" value is a boolean (xsd:boolean). Only the distinct points
     * in the xsd:boolean value space are represented. xsd:boolean has multiple
     * lexical forms which map onto "true" and "false". Those distinctions are
     * not preserved.
     */
    XSDBoolean((byte) 0, Bytes.SIZEOF_BYTE, Boolean.class,
            XSD.BOOLEAN, DTEFlags.NOFLAGS), //

    /** The "inline" value is a signed byte (xsd:byte). */
    XSDByte((byte) 1, Bytes.SIZEOF_BYTE, Byte.class,
            XSD.BYTE, DTEFlags.NUMERIC), //

    /** The "inline" value is a signed short (xsd:short). */
    XSDShort((byte) 2, Bytes.SIZEOF_SHORT, Short.class,
            XSD.SHORT, DTEFlags.NUMERIC), //

    /** The "inline" value is a signed 4 byte integer (xsd:int). */
    XSDInt((byte) 3, Bytes.SIZEOF_INT, Integer.class,
            XSD.INT, DTEFlags.NUMERIC), //

    /** The "inline" value is a signed 8 byte integer (xsd:long). */
    XSDLong((byte) 4, Bytes.SIZEOF_LONG, Long.class,
            XSD.LONG, DTEFlags.NUMERIC), //

    /*
     * unsigned byte, short, int, long.
     */

    /** The "inline" value is an unsigned byte (xsd:unsignedByte). */
    XSDUnsignedByte((byte) 5, Bytes.SIZEOF_BYTE, Byte.class,
            XSD.UNSIGNED_BYTE,
            DTEFlags.UNSIGNED_NUMERIC), //

    /** The "inline" value is a unsigned short (xsd:unsignedShort). */
    XSDUnsignedShort((byte) 6, Bytes.SIZEOF_SHORT, Short.class,
            XSD.UNSIGNED_SHORT,
            DTEFlags.UNSIGNED_NUMERIC), //

    /** The "inline" value is an unsigned 4 byte integer (xsd:unsignedInt). */
    XSDUnsignedInt((byte) 7, Bytes.SIZEOF_INT, Integer.class,
            XSD.UNSIGNED_INT,
            DTEFlags.UNSIGNED_NUMERIC), //

    /** The "inline" value is an unsigned 8 byte integer (xsd:unsignedLong). */
    XSDUnsignedLong((byte) 8, Bytes.SIZEOF_LONG, Long.class,
            XSD.UNSIGNED_LONG,
            DTEFlags.UNSIGNED_NUMERIC), //
            
    /*
     * float, double.
     */
            
    /**
     * The "inline" value is a single precision floating point number
     * (xsd:float).
     */
    XSDFloat((byte) 9, Bytes.SIZEOF_FLOAT, Float.class,
            XSD.FLOAT, DTEFlags.NUMERIC), //
            
    /**
     * The "inline" value is a double precision floating point number
     * (xsd:double).
     */
    XSDDouble((byte) 10, Bytes.SIZEOF_DOUBLE, Double.class,
            XSD.DOUBLE, DTEFlags.NUMERIC), //

    /*
     * xsd:integer, xsd:decimal.
     */
            
    /**
     * The "inline" value is an xsd:integer, which is equivalent to
     * {@link BigInteger}.
     */
    XSDInteger((byte) 11, 0/* variable length */, BigInteger.class,
            XSD.INTEGER, DTEFlags.NUMERIC), //

    /**
     * The "inline" value is an xsd:decimal. This is mostly equivalent to
     * {@link BigDecimal}, but unlike that Java class, xsd:decimal DOES NOT
     * preserve the precision of the value. (This fact is convenient for indices
     * since {@link BigDecimal} has, among other things, many distinct
     * representations of ZERO with different precision, etc. If we had to
     * represent the precision, we could not use xsd:decimal in an index!)
     */
    XSDDecimal((byte) 12, 0/* variable length */, BigDecimal.class,
            XSD.DECIMAL, DTEFlags.NUMERIC), //

    /*
     * custom intrinsic data types.
     */
            
    /**
     * The "inline" value is a {@link UUID}.
     * 
     * @see http://lists.xml.org/archives/xml-dev/201003/msg00027.html
     */
    UUID((byte) 13, Bytes.SIZEOF_UUID, UUID.class, XSD.UUID,
            DTEFlags.NOFLAGS), //

    /**
     * The "inline" value is a compressed Unicode string. This is decodable
     * compressed encoding rather than a Unicode sort key. It is suitable for
     * representing "small" Unicode values directly within the statement
     * indices. "Small" is configurable, but should not be overly large. The
     * tradeoff is the growth in the B+Tree leaf size for a statement index
     * versus the overhead required when indirecting through the
     * {@link LexiconRelation} to materialize the RDF {@link Value}. Further,
     * there is a practical upper bound on the size of a key in the B+Tree.
     * Therefore, inlining of Unicode values having between 32 and 64 characters
     * is suggested as a recommended practice. Beyond that, inlining can
     * contribute significantly to the growth in the B+Tree leaf size and have a
     * negative impact on join performance for the statement indices.
     * <p>
     * This {@link DTE} may be used in combination with {@link VTE} as follows:
     * <dl>
     * <dt>{@link VTE#BNODE}</dt>
     * <dd>Represent an inline blank node identifier.</dd>
     * <dt>{@link VTE#LITERAL}</dt>
     * <dd>Represent a plain literal.</dd>
     * <dt>{@link VTE#LITERAL} plus the extension bit</dt>
     * <dd>Represent a data type literal where the extension IV is the data type
     * of the literal.</dd>
     * <dt>{@link VTE#URI} plus the extension bit</dt>
     * <dd>Represent a URI where the extension IV is the namespace of the URI
     * and the inline Unicode component is the local name of the URI. This
     * depends on the openrdf definition of a {@link URI}'s namespace and local
     * name (basically, everything after the last '/' in the URI path or after
     * the '#' if there is a URI anchor).</dd>
     * </dl>
     */
    XSDString((byte) 14, 0/* len */, String.class, XSD.STRING,
            DTEFlags.NOFLAGS), //

	/**
	 * This is a place holder for extension of the intrinsic data types. Its
	 * code corresponds to 0xf, which is to say all four bits are on. When this
	 * code is used, the next byte(s) must be examined to determine the actual
	 * intrinsic data type.
	 * <p>
	 * Note: This is NOT the same as the {@link AbstractIV#isExtension()} bit.
	 * The latter <strong>always</strong> indicates that an {@link IV} follows
	 * the <code>flags</code> byte. In contrast, {@link DTE#Extension} gives you
	 * another byte which you can use to handle additional "intrinsic" types.
	 */
    Extension((byte) 15, 0/* len */, Void.class, null/* datatype */,
            DTEFlags.NOFLAGS);
    
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
     *            The well-known URI for the data type.
     * @param flags
     *            Some bit flags. See {@link #NUMERIC},
     *            {@link #UNSIGNED_NUMERIC}, etc.
     */
    private DTE(final byte v, final int len, final Class<?> cls,
            final URI datatypeURI, final int flags) {
        this.v = v;
        this.len = len;
        this.cls = cls;
        this.datatypeURI = datatypeURI;
        this.flags = flags;
    }

    static final public DTE valueOf(final byte b) {
        /*
         * Note: This switch MUST correspond to the declarations above (you can
         * not made the cases of the switch from [v] since it is not considered
         * a to be constant by the compiler).
         * 
         * Note: This masks off everything but the lower 4 bits.
         */
        switch (b & 0x0f) {
        case 0:
            return XSDBoolean;
        case 1:
            return XSDByte;
        case 2:
            return XSDShort;
        case 3:
            return XSDInt;
        case 4:
            return XSDLong;
        case 5:
            return XSDUnsignedByte;
        case 6:
            return XSDUnsignedShort;
        case 7:
            return XSDUnsignedInt;
        case 8:
            return XSDUnsignedLong;
        case 9:
            return XSDFloat;
        case 10:
            return XSDDouble;
        case 11:
            return XSDInteger;
        case 12:
            return XSDDecimal;
        case 13:
            return UUID;
        case 14:
            return XSDString;
        case 15:
            return Extension;
        default:
            throw new IllegalArgumentException(Byte.toString(b));
        }
    }

    /**
     * Return the {@link DTE} for the datatype {@link URI}.
     * 
     * @param datatype
     *            The datatype {@link URI}.
     *            
     * @return The {@link DTE} for that datatype -or- {@link #Extension} if the
     *         datatype is <code>null</code> (there is no specific datatype for
     *         an extension since extensions by their nature can handle any
     *         datatype) -or- <code>null</code> if the datatype {@link URI} is
     *         none of the datatypes for which native support is provided.
     */
    static final public DTE valueOf(final URI datatype) {
        /*
         * Note: This switch MUST correspond to the declarations above (you can
         * not make the cases of the switch from [v] since it is not considered
         * a to be constant by the compiler).
         * 
         * TODO Optimize using trie, weighted frequency lookup tree, hash map,
         * etc. Also, the match will always be on the local name once we proof
         * the namespace.
         */
        
        if (datatype == null) {
            return null;
        }
        
        if (datatype.equals(XSD.IPV4)) {
            /*
             * Note: This is a bit of a rough spot in the API. There is no
             * datatype associated with [Extension] since it is a place holder
             * for any an extension for any datatype.
             * 
             * Right now I am hijacking Extension for IPv4.
             * 
             * TODO FIXME
             */
            return Extension;
        }
        if (datatype.equals(XSD.BOOLEAN))
            return XSDBoolean;
        if (datatype.equals(XSD.BYTE))
            return XSDByte;
        if (datatype.equals(XSD.SHORT))
            return XSDShort;
        if (datatype.equals(XSD.INT))
            return XSDInt;
        if (datatype.equals(XSD.LONG))
            return XSDLong;
        if (datatype.equals(XSD.UNSIGNED_BYTE))
            return XSDUnsignedByte;
        if (datatype.equals(XSD.UNSIGNED_SHORT))
            return XSDUnsignedShort;
        if (datatype.equals(XSD.UNSIGNED_INT))
            return XSDUnsignedInt;
        if (datatype.equals(XSD.UNSIGNED_LONG))
            return XSDUnsignedLong;
        if (datatype.equals(XSD.FLOAT))
            return XSDFloat;
        if (datatype.equals(XSD.DOUBLE))
            return XSDDouble;
        if (datatype.equals(XSD.INTEGER))
            return XSDInteger;
        if (datatype.equals(XSD.DECIMAL))
            return XSDDecimal;
        if (datatype.equals(XSD.UUID))
            return UUID;
        if (datatype.equals(XSD.STRING))
            return XSDString;
        /*
         * Not a known DTE datatype.
         */
        return null;
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
     */
    private final Class<?> cls;

    /**
     * The well-known URI for the data type.
     */
    private final URI datatypeURI;

    /**
     * Some bit flags.
     * 
     * @see #NUMERIC
     * @see #UNSIGNED_NUMERIC
     */
    private final int flags;

    /**
     * An <code>byte</code> value whose whose lower 6 bits code the
     * {@link DTE}.
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
     * The corresponding datatype {@link URI}.
     */
    final public URI getDatatypeURI() {

        return datatypeURI;
        
    }

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
     * <code>true</code> for an signed numeric datatype ( xsd:byte,
     * xsd:short, xsd:int, xsd:long, xsd:float, xsd:double, xsd:integer, and
     * xsd:decimal).
     */
    public boolean isSignedNumeric() {
        
        return isNumeric() && !isUnsignedNumeric();
        
    }
    
    /**
     * <code>true</code> for an unsigned numeric datatype ( xsd:unsignedByte,
     * xsd:unsignedShort, xsd:unsignedInt, xsd:unsignedLong).
     */
    public boolean isUnsignedNumeric() {
        
        /*
         * Note: The DTEFlags are not independent bits so when we test them
         * we have to test for equality rather than masking off the various
         * bits.
         */
        return flags == DTEFlags.UNSIGNED_NUMERIC;
        
    }

    /**
     * This is <code>!isBigNumeric()</code> and is <code>true</code> for any of
     * the fixed length numeric data types (<code>xsd:byte, xsd:unsignedByte,
     * xsd:short, xsd:unsignedShort, xsd:int, xsd:unsignedInt, xsd:long,
     * xsd:unsignedLong, xsd:float, xsd:double</code>).
     */
    public boolean isFixedNumeric() {
        
        return (flags & DTEFlags.NUMERIC) != 0 && len != 0;
        
    }

    /**
     * <code>true</code> for xsd:integer and xsd:decimal.
     */
    public boolean isBigNumeric() {
        
        return (flags & DTEFlags.NUMERIC) != 0 && len == 0;
        
    }

    /**
     * <code>true</code> for xsd:float, xsd:double, and xsd:decimal.
     */
    public boolean isFloatingPointNumeric() {
        
        return this == XSDFloat || this == XSDDouble || this == XSDDecimal;
        
    }

}

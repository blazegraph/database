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

import org.openrdf.model.Value;

import com.bigdata.rdf.model.BigdataValue;

/**
 * Abstract base class for the inline representation of an RDF Value (the
 * representation which is encoded in to the keys of the statement indices).
 * This class is responsible for combining the {@link VTE} and the {@link DTE}
 * together into the flags byte used as a common prefix for all keys formed from
 * RDF Values regardless of whether they are based on an assigned term
 * identifier or the inlining of the RDF Value.
 * 
 * <h3>Binary record format</h3>
 * 
 * We currently have 14 built-in (or intrinsic) data types (see {@link DTE}).
 * Each of those types has a natural order which we can encode and decode from
 * the B+Tree key. In general, there is a relatively limited set of interesting
 * intrinsic codings, which is important since we will dedicate just 4 bits for
 * to code the natural order of the value space, which is just only 16
 * distinctions. Given that we have 14 intrinsic data types, that leaves room
 * for just two more. One of those bits is reserved against (see
 * {@link DTE#Reserved1}). The other bit is reserved for extensibility in the
 * framework itself as described below (see {@link DTE#Extension}).
 * <p>
 * The header byte contains various bit flags which are laid out as follows:
 * 
 * <pre>
 * [valueType]    : 2 bits
 * [inline]       : 1 bit
 * [extension]   : 1 bit
 * [dataTypeCode] : 4 bits
 * </pre>
 * 
 * <dl>
 * <dt>valueType</dt>
 * <dd>These 2 bits distinguish between URIs, Literals, Blank Nodes, and
 * statement identifiers (SIDs). These bits are up front and therefore partition
 * the key space first by the RDF Value type. See {@link VTE} which governs
 * these bits.</dd>
 * <dt>inline</dt>
 * <dd>This bit indicates whether the value is inline or represented by a term
 * identifier in the key. This bit is set based on how a given triple store or
 * quad store instance is configured. However, because the bit is present in the
 * flags, we know how to decode the key without reference to this configuration
 * metadata.</dd>
 * <dt>extension</dt>
 * <dd>This bit is ignored (and should be zero) unless the RDF Value is a
 * Literal with a data type URI which is being inlined. For data type literals,
 * this bit is set if the actual data type is not one of those which we handle
 * intrinsically but is one of those which has been registered (by the
 * application) as an "extended" data type projected onto one of the intrinsic
 * data types. Thus, this bit partitions the key space into the intrinsic data
 * types and the extended data types.<br/>
 * When <code>true</code>, this bit signals that information about the actual
 * RDF Value data type will follow (see below). When <code>false</code>, the
 * datatype URI is directly recoverable (for a data type Literal) from the
 * <code>dataTypeCode</code>.</dd>
 * <dt>dataTypeCode</dt>
 * <dd>These 4 bits indicate the intrinsic data type for the inline value and
 * are ignored (and should be zero) unless a data type Literal is being inlined.
 * These bits partition the key space. However, since <code>extension</code> bit
 * comes first this will not interleave inline values for intrinsic and extended
 * data types having the same <code>dataTypeCode</code>. <br/>
 * Note: The <code>dataTypeCode</code> <code>0xf</code> ({@link DTE#Extension)}
 * is reserved for extending the set of intrinsic data types. When the code is
 * <code>0xf</code> the next byte must be considered as well to determine the
 * actual intrinsic data type code.</dd>
 * </dl>
 * 
 * <pre>
 * ---------- byte boundary ----------
 * </pre>
 * 
 * If <code>extension</code> was true, then then the next byte(s) encode
 * information about the source data type URI and the key space will be
 * partitioned based on the extended data type URI [the precise format of that
 * data has not yet been decided -- see below].
 * 
 * <pre>
 * ---------- byte boundary ----------
 * </pre>
 * 
 * The unsigned byte[] representation of the value in the value space for one of
 * the intrinsic types. The length of this byte[] may be directly determined
 * from the [dataTypeCode] for most data types. However, for xsd:integer and
 * xsd:decimal, the length is part of the representation.
 * 
 * <pre>
 * ---------- byte boundary and end of the record ----------
 * </pre>
 * 
 * <h3>Extensibility</h3>
 * 
 * There are three core use cases for extensibility:
 * <dl>
 * <dt>projections</dt>
 * <dd>A projection takes an application specific data type and maps it onto one
 * of the intrinsic data types (int, float, double, etc). Projections provide an
 * extensible mechanism which allows an application to benefit from inline
 * representation of RDF Values and allows the query optimizer to chose
 * key-range scans for application defined data types if they can be projected
 * onto intrinsic data types. For example, if you define an application specific
 * data type <code>foo:milliseconds</code> representing milliseconds since the
 * epoch, then the value space of that data type can be projected onto an
 * <code>xsd:long</code>.</dd>
 * <dt>enumerations</dt>
 * <dd>An enumeration is an application specific data type having a specific set
 * of values. Those values are then projected onto an intrinsic data type such
 * as <code>byte</code> (256 distinctions) or <code>short</code> (64k
 * distinctions). Enumerations make it possible to inline application specific
 * data types while benefiting from XSD validation of those RDF Values. When an
 * enumeration is registered, the order in which the members of the enumeration
 * are given may optionally specify the natural order of that enumeration. The
 * natural order is imposed by projecting the first member of the enumeration
 * one ZERO, the second member onto ONE, etc. An enumeration with a natural
 * order will be sorted based on that defined order and query optimizations may
 * perform key-range scans informed by that natural order.<br/>
 * Enumerations may be used in cases where you might otherwise use short
 * character codes. For example, an enumeration could be defined for the two
 * character abbreviations for the 50 US States. That enumeration could be
 * mapped onto a single byte.</dd>
 * <dt>custom indices</dt>
 * <dd>The best example here is spatial data, which requires literals which
 * represent points, rectangles, circles, arcs, clouds, etc to be inserted into
 * special spatial indices. Queries must be aware of spatial data and must be
 * rewritten to run against the appropriate spatial indices.<br/>
 * Another use case would be carrying specialized indices for bioinformatics or
 * genomics data.</dd>
 * </dl>
 * Note: Both projected and enumerated extensible data types MAY map many RDF
 * Values onto the same internal value but each internal value MUST map onto a
 * single RDF Value (materialization must be deterministic). This can be seen as
 * normalization imposed by the database.
 * 
 * @todo Note: There can be more than one URI for the same XSD datatype (there
 *       is more than one accepted namespace - see <a
 *       href="http://www.w3.org/TR/xmlschema-2/#namespaces"> XML Schema
 *       Datatypes namespaces </a>). I propose that we collapse these by default
 *       onto a canonical datatype URI.
 * 
 * @todo For a extensible data type which is being projected onto an intrinsic
 *       data type we would need both (a) a method to project the RDF Value onto
 *       the appropriate intrinsic data type; and (b) a method to materialize an
 *       RDF Value from the inline representation.
 *       <p>
 *       If we put the registrations into their own index, then we could use a
 *       more compact representation (the term identifier of the datatype URI is
 *       8 bytes, but we could do with 2 or 4 bytes). Alternatively, we could
 *       use the LongPacker to pack an unsigned long integer into as few bytes
 *       as possible. This would break the natural ordering across the
 *       dataTypeIds, but I can not see how that would matter since the term
 *       identifiers are essentially arbitrary anyway so their order has little
 *       value.
 * 
 * @todo Can we inline the language code for a literal? I think that the
 *       language code must be ASCII and might be restricted to two characters.
 *       This might use up our {@link DTE#Reserved1} bit.
 * 
 * @todo One consequences of this refactor is that you must use equals() rather
 *       than == to compare internal values, including term identifiers. This
 *       boils down to verifying that the two internal values are the same type
 *       (same VTE, DTE, etc) and have the same value (termId, long, etc). That
 *       can all be done rather quickly, but it is more overhead than testing a
 *       == b.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestEncodeDecodeKeys.java 2753 2010-05-01 16:36:59Z thompsonbry
 *          $
 * @param <V>
 *            The generic type for the RDF {@link Value} implementation.
 * @param <T>
 *            The generic type for the inline value.
 */
public abstract class AbstractInternalValue<V extends BigdataValue, T>
        implements InternalValue<V, T> {

    /**
     * 
     */
    private static final long serialVersionUID = 4710700756635103123L;

    /**
     * Bit flags indicating the kind of RDF Value ({@link VTE}), whether the RDF
     * Value is inline, whether this is an extension datatype, and the natural
     * order and binary representation of the inline value ({@link See #DTE}).
     * 
     * @see VTE
     * @see DTE
     */
    private final byte flags;

    /**
     * The RDF Value type (URI, BNode, Literal or Statement) and the data type
     * are combined and stored in a single byte together with whether the RDF
     * value has been inlined (an <i>inline</i> bit) and whether the RDF Value
     * is an extended data type (the <i>extension</> bit). The <i>vte</i> has 4
     * distinctions and is thus TWO (2) bits wide. The <i>dte</i> allows for up
     * to 16 distinctions and is SIX (6) bits wide. The bits allocated to these
     * sets of distinctions are combined within a single byte as follows:
     * 
     * <pre>
     * [vviedddd]
     * </pre>
     * 
     * where <code>v</code> is a {@link VTE} bit, <code>i</code> is the
     * <i>inline</i> bit, <code>e</code> is the extension bit, and
     * <code>d</code> is a {@link DTE} bit.
     * 
     * @param vte
     *            The RDF Value type (URI, BNode, Literal, or Statement).
     * @param inline
     *            <code>true</code> iff the RDF value will be represented inline
     *            in the key. When <code>false</code>, the term identifier of
     *            the RDF Value will be represented inline instead of its actual
     *            value.
     * @param extension
     *            When <code>true</code>, the actual RDF data type URI differs
     *            from the intrinsic data type (the {@link DTE}) but has been
     *            projected onto the natural order of the intrinsic data type.
     * @param dte
     *            The internal datatype for the RDF value (termId, xsd:int,
     *            xsd:long, xsd:float, xsd:double, etc).
     * 
     * @see VTE
     * @see DTE
     */
    protected AbstractInternalValue(final VTE vte, final boolean inline,
            final boolean extension, final DTE dte) {

        // vte << 6 bits (it is in the high 2 bits).
        // inline << 5 bits
        // extension << 4 bits
        // dte is in the low 4 bits.
        this( (byte) ((//
                (((int) vte.v) << VTE_SHIFT)//
                | ((inline ? 1 : 0) << INLINE_SHIFT)//
                | ((extension ? 1 : 0) << EXTENSION_SHIFT) //
                | (dte.v)//
                ) & 0xff));

    }
    
    /**
     * Constructor used when decoding since you already have the flags.
     * 
     * @param flags
     *            The flags.
     */
    protected AbstractInternalValue(final byte flags) {
        
        this.flags = flags;
        
    }

    final public byte flags() {

        return flags;

    }

    /**
     * The #of bits (SIX) that the {@link VTE} is shifted to
     * the left when encoding it into the {@link #flags}.
     */
    private final static int VTE_SHIFT = 6;

    /**
     * The bit mask that is bit-wise ANDed with the flags in order to reveal the
     * {@link VTE}. The high TWO (2) bits of the low byte in the mask are set.
     */
    private final static int VTE_MASK = 0xC0;

    /**
     * The #of bits (FIVE) that the <i>inline</i> flag is shifted to the left
     * when encoding it into the {@link #flags}.
     */
    private final static int INLINE_SHIFT = 5;

    /**
     * The bit mask that is bit-wise ANDed with the flags in order to reveal
     * the <code>inline</code> bit.
     */
    private final static int INLINE_MASK = 0x20;

    /**
     * The #of bits (FOUR) that the <i>extension</i> flag is shifted to the left
     * when encoding it into the {@link #flags}.
     */
    private final static int EXTENSION_SHIFT = 4;

    /**
     * The bit mask that is bit-wise ANDed with the flags in order to reveal the
     * {@link DTE}. The low FOUR (4) bits in the mask are set.
     */
    private final static int DTE_MASK = 0x0f;

    /**
     * Return <code>true</code> if the flags byte has its <code>inline</code>
     * bit set.
     * 
     * @param flags
     *            The flags byte.
     */
    static public boolean isInline(final byte flags) {

        return (flags & INLINE_MASK) != 0;
        
    }
    
    final public VTE getInternalValueTypeEnum() {

        return VTE
                .valueOf((byte) (((flags & VTE_MASK) >>> VTE_SHIFT) & 0xff));

    }

    final public DTE getInternalDataTypeEnum() {

        return DTE.valueOf((byte) ((flags & DTE_MASK) & 0xff));

    }

    /**
     * Helper method decodes a flags byte as found in a statement index key to
     * an {@link VTE}.
     * 
     * @param flags
     *            The flags byte.
     * 
     * @return The {@link VTE}
     */
    static final public VTE getInternalValueTypeEnum(
            final byte flags) {

        return VTE
                .valueOf((byte) (((flags & VTE_MASK) >>> VTE_SHIFT) & 0xff));

    }

    /**
     * Helper method decodes a flags byte as found in a statement index key to
     * an {@link DTE}.
     * 
     * @param flags
     *            The flags byte.
     * @return The {@link DTE}
     */
    static public DTE getInternalDataTypeEnum(final byte flags) {

        return DTE.valueOf((byte) ((flags & DTE_MASK) & 0xff));

    }

    final public boolean isLiteral() {

        return (flags & VTE_MASK) >>> VTE_SHIFT == VTE.LITERAL.v;

    }

    final public boolean isBNode() {

        return (flags & VTE_MASK) >>> VTE_SHIFT == VTE.BNODE.v;

    }

    final public boolean isURI() {

        return (flags & VTE_MASK) >>> VTE_SHIFT == VTE.URI.v;

    }

    final public boolean isStatement() {

        return (flags & VTE_MASK) >>> VTE_SHIFT == VTE.STATEMENT.v;

    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation based on the <code>inline</code> bit flag. This can
     * be overridden in many derived classes which have compile time knowledge
     * of whether the RDF value is inline or not.
     */
    public boolean isInline() {
        return isInline(flags);
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation based on the <code>inline</code> bit flag. This can
     * be overridden in many derived classes which have compile time knowledge
     * of whether the RDF value is inline or not.
     */
    public boolean isTermId() {
        return !isInline();
    }
    
    final public boolean isNumeric() {
        return isInline() && getInternalDataTypeEnum().isNumeric();
    }

    final public boolean isUnsignedNumeric() {
        return isInline() && getInternalDataTypeEnum().isUnsignedNumeric();
    }

    final public boolean isShortNumeric() {
        return isInline() && getInternalDataTypeEnum().isShortNumeric();
    }

    final public boolean isBigNumeric() {
        return isInline() && getInternalDataTypeEnum().isBigNumeric();
    }

    /**
     * Return a hash code based on the value of the point in the value space.
     */
    abstract public int hashCode();

    /**
     * Return true iff the two values are the same point in the same value
     * space. Points in different value spaces (as identified by different
     * datatype URIs) are NOT equal even if they have the same value in the
     * corresponding primitive data type.
     */
    abstract public boolean equals(Object o);
    
}

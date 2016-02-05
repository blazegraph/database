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
 * Created on May 3, 2010
 */

package com.bigdata.rdf.internal.impl;

import java.io.IOException;
import java.util.UUID;

import org.openrdf.model.Literal;
import org.openrdf.model.Value;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.DTEExtension;
import com.bigdata.rdf.internal.IExtensionIV;
import com.bigdata.rdf.internal.IInlineUnicode;
import com.bigdata.rdf.internal.IPv4Address;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUnicode;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.InlineLiteralIV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.bnode.FullyInlineUnicodeBNodeIV;
import com.bigdata.rdf.internal.impl.bnode.NumericBNodeIV;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.FullyInlineTypedLiteralIV;
import com.bigdata.rdf.internal.impl.literal.LiteralArrayIV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.MockedValueIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedByteIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedIntIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedLongIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedShortIV;
import com.bigdata.rdf.internal.impl.uri.FullyInlineURIIV;
import com.bigdata.rdf.internal.impl.uri.URIExtensionIV;
import com.bigdata.rdf.internal.impl.uri.VocabURIByteIV;
import com.bigdata.rdf.internal.impl.uri.VocabURIShortIV;
import com.bigdata.rdf.lexicon.ITermIndexCodes;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Abstract base class for the inline representation of an RDF Value (the
 * representation which is encoded in to the keys of the statement indices).
 * This class is responsible for combining the {@link VTE} and the {@link DTE}
 * together into the flags byte used as a common prefix for all keys formed from
 * RDF Values regardless of whether they are based on an assigned term
 * identifier or the inlining of the RDF Value.
 * <p>
 * Literals which are projected onto primitive data types with a natural order
 * (int, float, double, long, etc.) are in total order within that segment of
 * the statement index and such segments are partitioned into non-overlapping
 * key ranges. Thus key range scans may be used for where the primitive data
 * type allows it while filtering on the value must be used where the data type
 * does not have a natural ordering. Unicode values do not have a "natural"
 * ordering within the statement indices as they are modeled by a reversible
 * compression rather than a collation sort key. Therefore, when a high level
 * query is constrained such that a variable is known to be of a given data type
 * you can use {@link IV} aware operations. Otherwise, the {@link IV}s must be
 * materialized and operations performed on {@link BigdataValue}s instead.
 * 
 * <h3>Binary record format</h3>
 * 
 * We currently have 14 built-in (or intrinsic) data types (see {@link DTE}).
 * Each of those types has a natural order which we can encode and decode from
 * the B+Tree key. In general, there is a relatively limited set of interesting
 * intrinsic codings, which is important since we will dedicate just 4 bits for
 * to code the natural order of the value space, which is just only 16
 * distinctions. Given that we have 14 intrinsic data types, that leaves room
 * for just two more. One of those bits provides for Unicode data (see
 * {@link DTE#XSDString} without a collation order). The other bit provides
 * extensibility in the framework itself as described below (see
 * {@link DTE#Extension}).
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
 * ---------- byte boundary (IFF DTE == DTE.Extension) ----------
 * </pre>
 * 
 * If the DTE value was <code>DTE.Extension</code> was, then the next byte(s) encode
 * the DTEExtension (extended intrinsic datatype aka primitive datatype).
 * 
 * <pre>
 * ---------- byte boundary ----------
 * </pre>
 * 
 * If <code>extension</code> was true, then the next byte(s) encode
 * information about the source data type URI (its {@link IV}) and the key space
 * will be partitioned based on the extended data type URI.
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
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @param <V>
 *            The generic type for the RDF {@link Value} implementation.
 * @param <T>
 *            The generic type for the inline value.
 */
public abstract class AbstractIV<V extends BigdataValue, T>
        implements IV<V, T> {

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
    protected final byte flags;

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
    protected AbstractIV(final VTE vte, final boolean inline,
            final boolean extension, final DTE dte) {

		this(toFlags(vte, inline, extension, dte));

    }

    /**
     * Return the <code>flags</code> byte that encodes the provided metadata
     * about an {@link IV internal value}.
     * 
     * @param vte
     *            The value type
     * @param inline
     *            <code>true</code> iff the value is inline (no index access is
     *            required to materialize the value).
     * @param extension
     *            <code>true</code> iff the value is an extension data type (a
     *            user specific data type defined whose underlying data type is
     *            specified by the {@link DTE}).
     * @param dte
     *            The basic data type.
     * 
     * @return The flags byte that encodes that information.
     */
    public static byte toFlags(final VTE vte, final boolean inline,
            final boolean extension, final DTE dte) {

        // vte << 6 bits (it is in the high 2 bits).
        // inline << 5 bits
        // extension << 4 bits
        // dte is in the low 4 bits.
        return (byte) ((//
                (((int) vte.v()) << VTE_SHIFT)//
                | ((inline ? 1 : 0) << INLINE_SHIFT)//
                | ((extension ? 1 : 0) << EXTENSION_SHIFT) //
                | (dte.v())//
                ) & 0xff);

    }
    
    /**
     * Constructor used when decoding since you already have the flags.
     * 
     * @param flags
     *            The flags.
     */
    protected AbstractIV(final byte flags) {
        
        this.flags = flags;
        
    }

    @Override
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
     * The bit mask that is bit-wise ANDed with the flags in order to reveal
     * the <code>inline</code> bit.
     */
    private final static int EXTENSION_MASK = 0x10;
    
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
    
    /**
     * Return <code>true</code> if the flags byte has its <code>extension</code>
     * bit set.
     * 
     * @param flags
     *            The flags byte.
     */
    static public boolean isExtension(final byte flags) {

        return (flags & EXTENSION_MASK) != 0;
        
    }

	/**
	 * Return the {@link VTE} encoding in a flags byte.
	 * <p>
	 * Note: {@link VTE#valueOf(byte)} assumes that the VTE bits are in the TWO
	 * (2) LSB bits of the byte. However, the VTE bits are actually stored in
	 * the TWO (2) MSB bits of the <i>flags</i> byte. This method is responsible
	 * for shifting the VTE bits down before invoking {@link VTE#valueOf(byte)}.
	 * 
	 * @param flags
	 *            A flags byte.
	 * 
	 * @return The {@link VTE} encoded in the flags byte.
	 */
	static final public VTE getVTE(final byte flags) {

		return VTE.valueOf((byte) (((flags & VTE_MASK) >>> VTE_SHIFT) & 0xff));

    }

	@Override
    final public VTE getVTE() {

        return getVTE(flags);

    }

    /**
     * Return the {@link DTE} for this {@link IV}.
     */
	@Override
    final public DTE getDTE() {

        return DTE.valueOf((byte) ((flags & DTE_MASK) & 0xff));

    }

    @Override
    public DTEExtension getDTEX() {
        return null;
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
    static public DTE getDTE(final byte flags) {

        return DTE.valueOf((byte) ((flags & DTE_MASK) & 0xff));

    }

    @Override
    final public boolean isLiteral() {

        return (flags & VTE_MASK) >>> VTE_SHIFT == VTE.LITERAL.v();

    }

    @Override
    final public boolean isBNode() {

        return (flags & VTE_MASK) >>> VTE_SHIFT == VTE.BNODE.v();

    }

    @Override
    final public boolean isURI() {

        return (flags & VTE_MASK) >>> VTE_SHIFT == VTE.URI.v();

    }
    
    @Override
    final public boolean isStatement() {

        return (flags & VTE_MASK) >>> VTE_SHIFT == VTE.STATEMENT.v();

    }

    @Override
    final public boolean isResource() {

        return isURI() || isBNode();

    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation based on the <code>inline</code> bit flag. This can
     * be overridden in many derived classes which have compile time knowledge
     * of whether the RDF value is inline or not.
     */
    @Override
    public boolean isInline() {
        return isInline(flags);
    }

	/**
	 * {@inheritDoc}
	 * <p>
	 * This implementation based on the <code>extension</code> bit flag. Since
	 * the extension flag is only used for specific kinds of {@link IV}s, this
	 * method can be overridden in many derived classes which have compile time
	 * knowledge of whether the value is an RDF {@link Literal} or not.
	 */
    @Override
    public boolean isExtension() {
        return isExtension(flags);
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * The default implementation returns <code>false</code>.
     */
    @Override
    public boolean isVocabulary() {
        return false;
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * This implementation returns <code>false</code>. It is overridden by
     * {@link TermId}.
     */
    @Override
    public boolean isNullIV() {
    	return false;
    }
    
    @Override
    public boolean isNumeric() {
        return isInline() && getDTE().isNumeric();
    }

    @Override
    final public boolean isSignedNumeric() {
        return isInline() && getDTE().isSignedNumeric();
    }

    @Override
    final public boolean isUnsignedNumeric() {
        return isInline() && getDTE().isUnsignedNumeric();
    }

    @Override
    final public boolean isFixedNumeric() {
        return isInline() && getDTE().isFixedNumeric();
    }

    @Override
    final public boolean isBigNumeric() {
        return isInline() && getDTE().isBigNumeric();
    }

    @Override
    final public boolean isFloatingPointNumeric() {
        return isInline() && getDTE().isFloatingPointNumeric();
    }

    /**
	 * Return a hash code based on the value of the point in the value space.
	 * <p>
	 * Note: The {@link IV} implementations typically DO NOT return hash codes
	 * that are consistent with {@link BigdataValue#hashCode()}. Therefore you
	 * MUST NOT mix {@link IV}s and {@link BigdataValue}s in the keys of a map
	 * or the values of a set.
	 */
    @Override
    abstract public int hashCode();

    /**
	 * Return true iff the two {@link IV}s are the same point in the same value
	 * space. Points in different value spaces (as identified by different
	 * datatype URIs) are NOT equal even if they have the same value in the
	 * corresponding primitive data type.
	 * <p>
	 * Note: The {@link IV} implementations typically DO NOT compare equals()
	 * with {@link BigdataValue}s. Therefore you MUST NOT mix {@link IV}s and
	 * {@link BigdataValue}s in the keys of a map or the values of a set.
	 */
    @Override
    abstract public boolean equals(Object o);
    
    /**
     * Imposes an ordering of IVs based on their natural sort ordering in the 
     * index as unsigned byte[]s.
     */
    @Override
    @SuppressWarnings("rawtypes")
    final public int compareTo(final IV o) {

        if (this == o)
            return 0;

        if (o == null) {
			/*
			 * TODO This is ordering NULLs last. Why is this code even seeing
			 * null references?  Also, note that SPARQL ORDER BY orders the
			 * nulls first.
			 */
            return 1;
        }
        
        /*
         * First order based on the flags byte. This is the first byte of the
         * key, so it always partitions the key space and hence provides the
         * initial dimension of the total IV ordering.
         * 
         * Note: This comparison will always sort out things such that URIs,
         * Literals, BNodes, and SIDs will never compare as equals. It will also
         * sort out extension types and datatype literals with a natural
         * datatype.
         */
		
        final AbstractIV<?, ?> t = (AbstractIV<?, ?>) o;

		final int ret = (int) flags - (int) t.flags;

        if (ret < 0)
            return -1;
        
        if (ret > 0)
            return 1;

        /*
         * At this point we are comparing two IVs of the same intrinsic type.
         * These can be compared by directly comparing their primitive values.
         * E.g., long to long, int to int, etc. For the IVs which have inline
         * Unicode, the comparison should be of the String values per the API
         * for the appropriate RDF Value type (Literal, URI, Blank node). The
         * String comparison will reflect the code points rather than any
         * collation order, but Unicode values are inlined using a code point
         * ordering preserving compression so comparing the Strings is the right
         * thing to do.
         */
        return _compareTo( t );

    }

    /**
     * Compare two {@link IV}s having the same intrinsic datatype.
     */
    @SuppressWarnings("rawtypes")
    public abstract int _compareTo(IV o);

    @Override
    public IKeyBuilder encode(final IKeyBuilder keyBuilder) {

        // First emit the flags byte.
        keyBuilder.appendSigned(flags);

        if (!isInline()) {
            /*
             * The IV is not 100% inline.
             */
            if (isExtension()) {
                /*
                 * The IV uses the "extension" bit. We have two different use
                 * cases here. One is URIs in which we have factored out the
                 * namespaceIV for the URI. The other is data type literals in
                 * which we have factored out the datatypeIV for the literal
                 * (this is only used when the datatype literal can not be
                 * projected onto one of intrinsic data types).
                 * 
                 * {namespaceIV,localName} URIs
                 * 
                 * {datatypeIV,label} literals.
                 */
                final AbstractNonInlineExtensionIVWithDelegateIV<?, ?> t = (AbstractNonInlineExtensionIVWithDelegateIV<?, ?>) this;
                // Add the extension byte.
                keyBuilder.appendSigned(t.getExtensionByte());
                // Add the extension IV.
                IVUtility.encode(keyBuilder, t.getExtensionIV());
                // Add the delegate IV.
                IVUtility.encode(keyBuilder, t.getDelegate());
                return keyBuilder;
            }
			/*
			 * The RDF Value is represented as a term identifier (i.e., a key
			 * into the TERMS index).
			 * 
			 * Note: This is handled by TermIV#encode() so we will never get to
			 * this point in the code.
			 */
//            keyBuilder.append(getTermId());
//            return keyBuilder;
            throw new AssertionError(getClass().getName() + ":" + toString());
        }
        
        if (isURI()) {
            /*
             * Handle a fully inline URI.
             */
            if(isExtension()) {

                @SuppressWarnings("unchecked")
                final URIExtensionIV<BigdataURI> extension = (URIExtensionIV<BigdataURI>) this;

                // The namespaceIV (a Vocabulary item).
                IVUtility.encode(keyBuilder, extension.getExtensionIV());

                // The inline localName (any inline literal data).
                IVUtility.encode(keyBuilder, extension.getLocalNameIV());

                return keyBuilder;
            }
            switch (getDTE()) {
            case XSDByte: {
				keyBuilder.append(((VocabURIByteIV<?>) this).byteValue());
				break;
            }
            case XSDShort: {
				keyBuilder.append(((VocabURIShortIV<?>) this).getInlineValue());
				break;
            }
            case XSDString: {
                final FullyInlineURIIV<?> iv = (FullyInlineURIIV<?>) this;
                final String uriString = iv.getInlineValue().stringValue();
                final byte[] b = IVUnicode.encode1(uriString);
                keyBuilder.append(b);
                iv.setByteLength(1/* flags */+ b.length);
				break;
            }
            default:
                throw new UnsupportedOperationException("dte="+getDTE());
            }
            return keyBuilder;
        }

        if (isBNode()) {
            /*
             * Handle inline blank nodes.
             */
            switch (getDTE()) {
			case XSDInt:
				keyBuilder.append(((NumericBNodeIV<?>) this).intValue());
				break;
            case UUID:
                keyBuilder.append((UUID) getInlineValue());
                break;
            case XSDString: {
                final FullyInlineUnicodeBNodeIV<?> iv = (FullyInlineUnicodeBNodeIV<?>) this;
                final byte[] b = IVUnicode.encode1(iv.getInlineValue());
                keyBuilder.append(b);
                iv.setByteLength(1/* flags */+ b.length);
                break;
            }
            default:
                throw new UnsupportedOperationException("dte="+getDTE());
            }
            // done.
            return keyBuilder;
            
        } 
        
        // The datatype.
        final DTE dte = getDTE();
        final DTEExtension dtex;
        if (dte == DTE.Extension) {
            /*
             * @see BLZG-1507 (Implement support for DTE extension types for
             * URIs)
             * 
             * @see BLZG-1595 ( DTEExtension for compressed timestamp)
             */
            dtex = getDTEX();
            keyBuilder.append(dtex.v());
        } else
            dtex = null;

        /*
         * We are dealing with some kind of inlined Literal. It may either be a
         * natural datatype [int, long, float, etc.] or an IExtension projected
         * onto a natural datatype.
         * 
         * Note: An optimized xsd:string handled by XSDStringExtension goes
         * through this code path.
         */
        assert getVTE() == VTE.LITERAL;

        if (isExtension()) {

            /*
             * Append the extension type for a datatype literal into the key.
             * 
             * Note: Non-inline extensions were handled above!
             */
            
        	final IExtensionIV extension = (IExtensionIV) this;
        	
            IVUtility.encode(keyBuilder, extension.getExtensionIV());

        }

        /*
         * Append the natural value type representation.
         * 
         * Note: We have to handle the unsigned byte, short, int and long values
         * specially to get the correct total key order.
         * 
         * Note: When we generate the key from an IV with Unicode data we cache
         * the byteLength! This avoids having to compute it again since we know
         * it at the time that we encode the IV.
         */
        final AbstractLiteralIV<?, ?> t = isExtension() //
                ? ((LiteralExtensionIV<?>) this).getDelegate()//
                : (AbstractLiteralIV<?, ?>) this;

        switch (dte) {
        case XSDBoolean:
            keyBuilder.appendSigned((byte) (t.booleanValue() ? 1 : 0));
            break;
        case XSDByte:
            keyBuilder.appendSigned(t.byteValue());
            break;
        case XSDShort:
            keyBuilder.append(t.shortValue());
            break;
        case XSDInt:
            keyBuilder.append(t.intValue());
            break;
        case XSDLong:
            keyBuilder.append(t.longValue());
            break;
        case XSDFloat:
            keyBuilder.append(t.floatValue());
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
            keyBuilder.append((UUID) t.getInlineValue());
            break;
        case XSDUnsignedByte:
            keyBuilder.appendSigned(((XSDUnsignedByteIV<?>) t).rawValue());
            break;
        case XSDUnsignedShort:
            keyBuilder.append(((XSDUnsignedShortIV<?>) t).rawValue());
            break;
        case XSDUnsignedInt:
            keyBuilder.append(((XSDUnsignedIntIV<?>) t).rawValue());
            break;
        case XSDUnsignedLong:
            keyBuilder.append(((XSDUnsignedLongIV<?>) t).rawValue());
            break;
        case XSDString: {
            if (this instanceof FullyInlineTypedLiteralIV<?>) {
                /*
                 * A fully inline Literal
                 */
                final FullyInlineTypedLiteralIV<?> iv = (FullyInlineTypedLiteralIV<?>) this;
                // notice the current key length.
                final int pos0 = keyBuilder.len();
                // append the term code.
                keyBuilder.append((byte) iv.getTermCode());
                // handle language code or datatype URI.
                if (iv.getLanguage() != null) {
                    // language code
                    keyBuilder.append(IVUnicode.encode1(iv.getLanguage()));
                } else if (iv.getDatatype() != null) {
                    // datatype URI
                    keyBuilder.append(IVUnicode.encode1(iv.getDatatype()
                            .stringValue()));
                }
                // the literal's label
                keyBuilder.append(IVUnicode.encode1(iv.getLabel()));
                // figure the final length of the key.
                final int len = keyBuilder.len() - pos0;
                // cache the byteLength on the IV.
                iv.setByteLength(1/* flags */+ len);
                return keyBuilder;
            }
            /*
             * Optimized code path for xsd:string when using in combination with
             * ExternalIV.
             */
            // append the term code (note: plain literal!!!)  
            keyBuilder.append((byte) ITermIndexCodes.TERM_CODE_LIT);
            final byte[] b = IVUnicode.encode1((String) t.getInlineValue());
            keyBuilder.append(b);
            ((IInlineUnicode) t)
                    .setByteLength(1/* flags */+ 1/* termCode */+ b.length);
            return keyBuilder;
        }
        case Extension: {
            switch(dtex) {
            case IPV4: {
                // Append the IPv4Address
                keyBuilder.append(((IPv4Address) t.getInlineValue()).getBytes());
                break;
            }
            case PACKED_LONG: {
                // Third, emit the packed long's byte value
                ((KeyBuilder) keyBuilder).pack(((Long) t.getInlineValue()).longValue());
                break;
            }
            case MOCKED_IV: {
                final IV<?,?> iv = ((MockedValueIV) t).getIV();
                iv.encode(keyBuilder);
                break;
            }
            case ARRAY: {
                final InlineLiteralIV[] ivs = ((LiteralArrayIV) t).getIVs();
                /*
                 * Append the length of the array as a byte. InlineLiteralIV
                 * only supports arrays of length (1...256).
                 */
                // int(1...256) --> byte(0...255)
                final byte len = (byte) (ivs.length-1);
                keyBuilder.append(len);
                /*
                 * Then append the ivs one by one.
                 */
                for (InlineLiteralIV<?,?> iv : ivs) {
                    iv.encode(keyBuilder);
                }
                break;
            }
            default:
                throw new UnsupportedOperationException("DTExtension=" + dtex);
            }
            break;
        }
        default:
            throw new AssertionError(toString());
        }

        return keyBuilder;
        
    }
    
//    public IV getExtensionIV() {
//        return null;
//    }

//    public String bnodeId() {
//
//        throw new UnsupportedOperationException();
//        
//    }
    
    /*
     * RDF Value cache.
     */
    
    /**
     * Value cache (transient, but overridden serialization is used to send this
     * anyway for various purposes).
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/337
     */
	private volatile transient V cache = null;

    @Override
	final public V getValue() {

		final V v = cache;

		if (v == null)
			throw new NotMaterializedException(toString());

		return v;

	}

    @Override
	final public V setValue(final V v) {

		return (this.cache = v);
		
	}

	/**
	 * Return the cached {@link BigdataValue} or -<code>null</code> if it is not
	 * cached.
	 */
	protected final V getValueCache() {
		
		return cache;
		
	}
	
//	final public void dropValue() {
//		
//		this.cache = null;
//		
//	}
	
    @Override
	final public boolean hasValue() {
		
		return cache != null;
		
	}
	
	/*
	 * Serialization.
	 */
	
    /**
     * Override default serialization to send the cached {@link BigdataValue}.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/337
     */
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {

        out.defaultWriteObject();
        
        out.writeObject(getValueCache());

    }

    /**
     * Override default serialization to recover the cached {@link BigdataValue}
     * .
     */
    @SuppressWarnings("unchecked")
    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {

        in.defaultReadObject();

        final V v = (V) in.readObject();

        if (v != null) {
            // set the value cache.
            setValue(v);
        }
        
    }

}

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
     * Bit flags indicating the kind of RDF Value and the interpretation of the
     * inline value (either as a term identifier or as some data type value).
     * 
     * @see InternalValueTypeEnum
     * @see InternalDataTypeEnum
     */
    private final byte flags;

    /**
     * The RDF Value type (URI, BNode, Literal or Statement) and the data type
     * are combined and stored in a single byte. The <i>vte</i> has 4
     * distinctions and is thus TWO (2) bits wide. The <i>dte</i> allows for up
     * to 64 distinctions and is SIX (6) bits wide. The bits allocated to these
     * sets of distinctions are combined within a single byte as follows:
     * <code>[vvdddddd]</code>, where <code>v</code> is a <i>vte</i> bit and
     * <code>d</code> is a <i>dte</i> bit.
     * 
     * @param vte
     *            The RDF Value type (URI, BNode, Literal, or Statement).
     * @param dte
     *            The internal datatype for the RDF value (termId, xsd:int,
     *            xsd:long, xsd:float, xsd:double, etc).
     */
    protected AbstractInternalValue(final InternalValueTypeEnum vte,
            final InternalDataTypeEnum dte) {

        // vte << 6 bits (it is in the high 2 bits).
        // dte is in the low six bits.
        this.flags = (byte) ((((int) vte.v) << VTE_SHIFT | (dte.v)) & 0xff);

    }

    final public byte flags() {

        return flags;

    }

    /**
     * The #of bits (SIX) that the {@link InternalValueTypeEnum} is shifted to
     * the left when encoding it into the {@link #flags}.
     */
    private final static int VTE_SHIFT = 6;

    /**
     * The bit mask that is bit-wise ANDed with the flags in order to reveal the
     * {@link InternalValueTypeEnum}. The high TWO (2) bits of the low byte in
     * the mask are set.
     */
    private final static int VTE_MASK = 0xC0;

    /**
     * The bit mask that is bit-wise ANDed with the flags in order to reveal the
     * {@link InternalDataTypeEnum}. The low SIX (6) bits in the mask are set.
     */
    private final static int DTE_MASK = 0x3f;

    final public InternalValueTypeEnum getInternalValueTypeEnum() {

        return InternalValueTypeEnum
                .valueOf((byte) (((flags & VTE_MASK) >>> VTE_SHIFT) & 0xff));

    }

    final public InternalDataTypeEnum getInternalDataTypeEnum() {

        return InternalDataTypeEnum.valueOf((byte) ((flags & DTE_MASK) & 0xff));

    }

    /**
     * Helper method decodes a flags byte as found in a statement index key to
     * an {@link InternalValueTypeEnum}.
     * 
     * @param flags
     *            The flags byte.
     * 
     * @return The {@link InternalValueTypeEnum}
     */
    static final public InternalValueTypeEnum getInternalValueTypeEnum(
            final byte flags) {

        return InternalValueTypeEnum
                .valueOf((byte) (((flags & VTE_MASK) >>> VTE_SHIFT) & 0xff));

    }

    /**
     * Helper method decodes a flags byte as found in a statement index key to
     * an {@link InternalDataTypeEnum}.
     * 
     * @param flags
     *            The flags byte.
     * @return The {@link InternalDataTypeEnum}
     */
    static public InternalDataTypeEnum getInternalDataTypeEnum(final byte flags) {

        return InternalDataTypeEnum.valueOf((byte) ((flags & DTE_MASK) & 0xff));

    }

    final public boolean isLiteral() {

        return (flags & VTE_MASK) >>> VTE_SHIFT == InternalValueTypeEnum.LITERAL.v;

    }

    final public boolean isBNode() {

        return (flags & VTE_MASK) >>> VTE_SHIFT == InternalValueTypeEnum.BNODE.v;

    }

    final public boolean isURI() {

        return (flags & VTE_MASK) >>> VTE_SHIFT == InternalValueTypeEnum.URI.v;

    }

    final public boolean isStatement() {

        return (flags & VTE_MASK) >>> VTE_SHIFT == InternalValueTypeEnum.STATEMENT.v;

    }

    final public boolean isNumeric() {
        return getInternalDataTypeEnum().isNumeric();
    }

    final public boolean isUnsignedNumeric() {
        return getInternalDataTypeEnum().isUnsignedNumeric();
    }

    final public boolean isShortNumeric() {
        return getInternalDataTypeEnum().isShortNumeric();
    }

    final public boolean isBigNumeric() {
        return getInternalDataTypeEnum().isBigNumeric();
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

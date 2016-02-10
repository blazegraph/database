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
 * Created on Oct 29, 2015
 */

package com.bigdata.rdf.internal.impl.literal;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.io.LongPacker;
import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.DTEExtension;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * Internal value representing a packed long in the range [0;72057594037927935L].
 * Note that this is not the full range of long (negative values are not
 * supported and positive long values in [72057594037927936L;Long.MAX]
 * are not supported), the reason being that the compression technique
 * we're using is order preserving only for the valid range.
*/
public class PackedLongIV<V extends BigdataLiteral> 
        extends AbstractLiteralIV<V, Long>
            implements Serializable, Literal {

    private static final long serialVersionUID = 925868533758851987L;

//  private static final transient Logger log = Logger.getLogger(CompressedTimestampIV.class);

    public static final URI PACKED_LONG = new URIImpl("http://www.bigdata.com/rdf/datatype#packedLong");


    /**
     *  The {@link PackedLongIV} uses the {@link LongPacker} to compress values.
     *  {@link LongPacker#packLong(long, byte[], com.bigdata.io.LongPacker.IByteBuffer)}
     *  is order preserving whenever the first byte is 0. Since the IV relies on an
     *  order preserving encoding, we restrict the supported range to values in
     *  [0;MAX_POS_LONG_WITH_LEADING_ZERO_BYTE], where MAX_POS_LONG_WITH_LEADING_ZERO_BYTE
     *  corresponds to the 64bit string 00000000111111111111111..., which equals
     *  72057594037927935L. The value was (statically) verified as follows:
     *  <code>
          long v1 = 72057594037927935L;
          long v2 = 72057594037927936L;
          System.out.println(( ( v1 >> 56 ) != 0 )); // condition used in LongPacker
          System.out.println(( ( v2 >> 56 ) != 0 )); // condition used in LongPacker
       </code>
     * outputs "false" followed by "true".
     */
    public static final long MAX_POS_LONG_WITH_LEADING_ZERO_BYTE = 72057594037927935L;
    
    /**
     * The represented value
     */
    private final long value;

    /**
     * The cached materialized BigdataValue for this InetAddress.
     */
    private transient V literal;
    
    @Override
    public IV<V, Long> clone(final boolean clearCache) {

        final PackedLongIV<V> tmp = new PackedLongIV<V>(value);
        
        if (!clearCache) {

            tmp.setValue(getValueCache());
            
        }
        
        return tmp;

    }
    
    /**
     * Ctor with string value specified.
     */
    public PackedLongIV(final String value) {

        this(Long.valueOf(value));
        
    }

    /**
     * Ctor with internal value specified.
     */
    public PackedLongIV(final long value) {

        super(DTE.Extension);

        if (value<0 || value>MAX_POS_LONG_WITH_LEADING_ZERO_BYTE) {
            throw new IllegalArgumentException("long value out of range: " + value);
        }
        this.value = value;
        
    }

    /**
     * Returns the inline value.
     */
    @Override
    public Long getInlineValue() throws UnsupportedOperationException {
        return value;
    }

    /**
     * Returns the Literal representation of this IV.
     */
    @SuppressWarnings("unchecked")
    public V asValue(final LexiconRelation lex) {
        if (literal == null) {
            literal = (V) lex.getValueFactory().createLiteral(getLabel(), PACKED_LONG);
            literal.setIV(this);
        }
        return literal;
    }

    /**
     * Return the byte length for the byte[] encoded representation of this
     * internal value.  Depends on the byte length of the encoded inline value.
     */
    @Override
    public int byteLength() {
        return 1 /* flags */ + 1 /* DTEExtension */ + LongPacker.getByteLength(value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
    
    @Override
    public int hashCode() {
        return (int)value;
    }

    @Override
    public String getLabel() {
        return String.valueOf(value);
    }
    
    /**
     * Two {@link PackedLongIV} are equal if their InetAddresses are equal.
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof PackedLongIV) {
            return value==((PackedLongIV<?>) o).value;
        }
        return false;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public int _compareTo(IV o) {

        if (value < ((PackedLongIV<?>)o).value) {
            return -1;
        } else if (value>((PackedLongIV<?>)o).value) {
            return 1;
        } else {
            return 0;
        }

    }

    @Override
    public DTEExtension getDTEX() {

        return DTEExtension.PACKED_LONG;

    }
    
    /**
     * Implement {@link Literal#booleanValue()}.
     */
    @Override
    public boolean booleanValue() {
        return value>0;
    }

    /**
     * Implement {@link Literal#shortValue()}.
     */
    @Override
    public short shortValue() {
        return (short)value;
    }

    /**
     * Implement {@link Literal#intValue()}.
     */
    @Override
    public int intValue() {
        return (int)value;
    }

    /**
     * Implement {@link Literal#longValue()}.
     */
    @Override
    public long longValue() {
        return value;
    }

    /**
     * Implement {@link Literal#floatValue()}.
     */
    @Override
    public float floatValue() {
        return (float)value;
    }

    /**
     * Implement {@link Literal#doubleValue()}.
     */
    @Override
    public double doubleValue() {
        return (double)value;
    }

    /**
     * Implement {@link Literal#integerValue()}.
     */
    @Override
    public BigInteger integerValue() {
        return BigInteger.valueOf(value);
    }

    /**
     * Implement {@link Literal#decimalValue()}.
     */
    @Override
    public BigDecimal decimalValue() {
        return BigDecimal.valueOf(value);
    }

}

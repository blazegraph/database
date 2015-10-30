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
 * Created on Oct 29, 2015
 */

package com.bigdata.rdf.internal.impl.literal;

import java.io.Serializable;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.io.LongPacker;
import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.DTEExtension;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * Internal value representing a packed long. The value must be a
 * positive long value smaller than Long.MAX_VALUE-1 (otherwise an
 * {@link IllegalArgumentException} is thrown at construction time.
 * It is packed using the {@link LongPacker} utilities in an order
 * preserving way.
 */
public class PackedLongIV<V extends BigdataLiteral> 
        extends AbstractLiteralIV<V, Long>
            implements Serializable, Literal {

    private static final long serialVersionUID = 925868533758851987L;

//  private static final transient Logger log = Logger.getLogger(CompressedTimestampIV.class);

    public static final URI PACKED_LONG = new URIImpl("http://www.bigdata.com/rdf/datatype#packedLong");

    /**
     *  Long.MAX_VALUE-1 is internally represented as 0111111111...
     *  -> this is the bigges value that can be handled by the LongPacker
     */
    public static final long MAX_LONG_WITHOUT_LEADING_1 = Long.MAX_VALUE-1;
    
    
    /**
     * The represented value
     */
    private final long value;

    /**
     * The cached materialized BigdataValue for this InetAddress.
     */
    private transient V literal;
    

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

        if (value<0 || value>MAX_LONG_WITHOUT_LEADING_1) {
            throw new IllegalArgumentException("long value out of range: " + value);
        }
        this.value = value;
        
    }

    /**
     * Returns the inline value.
     */
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
    public int byteLength() {
        return 1 /* flags */ + 1 /* DTEExtension */ + LongPacker.getByteLength(value);
    }

    public String toString() {
        return String.valueOf(value);
    }
    
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
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof PackedLongIV) {
            return value==((PackedLongIV<?>) o).value;
        }
        return false;
    }

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
    
    /**
     * Encode this internal value into the supplied key builder.  Emits the
     * flags, following by the encoded byte[] representing the packed long.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public IKeyBuilder encode(final IKeyBuilder keyBuilder) {

        // First emit the flags byte -> this seems to break things
//        keyBuilder.appendSigned(flags());
        
        // Second, emit the ID of the extensions
        keyBuilder.append(DTEExtension.PACKED_LONG.v());
        
        // Third, emit the packed long's byte value
        ((KeyBuilder)keyBuilder).pack(value);
        
        return keyBuilder;
            
    }

}
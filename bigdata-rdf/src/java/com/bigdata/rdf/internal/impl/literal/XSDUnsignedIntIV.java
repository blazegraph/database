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

package com.bigdata.rdf.internal.impl.literal;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;

/** Implementation for inline <code>xsd:unsignedInt</code>. */
public class XSDUnsignedIntIV<V extends BigdataLiteral> extends
        AbstractLiteralIV<V, Long> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    /**
     * The unsigned int value.
     */
    private final int value;

    /**
     * The unsigned int value.
     */
    public int rawValue() {

        return value;
        
    }

    public XSDUnsignedIntIV(final int value) {
        
        super(DTE.XSDUnsignedInt);
        
        this.value = value;
        
    }
    
    /**
     * Promote the unsigned int into a signed long.
     */
    final long promote() {

        long v = value;
        
        if (v < 0) {

            v = v - 0x80000000;

        } else {
            
            v = 0x80000000 + v;
            
        }

        return v;

    }

    final public Long getInlineValue() {
        
        return promote();
        
    }

	@SuppressWarnings("unchecked")
	public V asValue(final LexiconRelation lex) {
		V v = getValueCache();
		if (v == null) {
			final BigdataValueFactory f = lex.getValueFactory();
			v = (V) f.createLiteral(value);
			v.setIV(this);
			setValue(v);
		}
		return v;
    }

    @Override
    final public long longValue() {
        return (long) promote();
    }

    @Override
    public boolean booleanValue() {
        return promote() == 0 ? false : true;
    }

    @Override
    public byte byteValue() {
        return (byte) promote();
    }

    @Override
    public double doubleValue() {
        return (double) promote();
    }

    @Override
    public float floatValue() {
        return (float) promote();
    }

    @Override
    public int intValue() {
        return (int) promote();
    }

    @Override
    public short shortValue() {
        return (short) promote();
    }
    
    @Override
    public String stringValue() {
        return Long.toString(promote());
    }

    @Override
    public BigDecimal decimalValue() {
        return BigDecimal.valueOf(promote());
    }

    @Override
    public BigInteger integerValue() {
        return BigInteger.valueOf(promote());
    }

    public boolean equals(final Object o) {
        if(this==o) return true;
        if(o instanceof XSDUnsignedIntIV<?>) {
            return this.value == ((XSDUnsignedIntIV<?>) o).value;
        }
        return false;
    }
    
    /**
     * Return the hash code of the int value.
     * 
     * @see Integer#hashCode()
     */
    public int hashCode() {
        return (int) value;
    }

    public int byteLength() {
        return 1 + Bytes.SIZEOF_INT;
    }

    @Override
    public int _compareTo(final IV o) {
         
        final long value = promote();
        
        final long value2 = ((XSDUnsignedIntIV) o).promote();
        
        return value == value2 ? 0 : value < value2 ? -1 : 1;
        
    }
    
}
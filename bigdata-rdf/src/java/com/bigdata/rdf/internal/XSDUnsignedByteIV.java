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

package com.bigdata.rdf.internal;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;

/** Implementation for inline <code>xsd:unsignedByte</code>. */
public class XSDUnsignedByteIV<V extends BigdataLiteral> extends
        AbstractLiteralIV<V, Short> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    /**
     * The unsigned byte value.
     */
    private final byte value;

    /**
     * The unsigned byte value.
     */
    byte rawValue() {

        return value;
        
    }
    
    /**
     * 
     * @param value
     *            The unsigned byte value.
     */
    public XSDUnsignedByteIV(final byte value) {
        
        super(DTE.XSDUnsignedByte);
        
        this.value = value;
        
    }

    final public Short getInlineValue() {

        return promote();

    }

    /**
     * Promote the unsigned byte into a signed short.
     */
    final short promote() {
        
        int i = value;
        
        if (i < 0) {

            i = i - 0x80;

        } else {
            
            i = i + 0x80;
            
        }
        
        return (short)(i & 0xff);

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
        return Short.toString(promote());
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
        if(o instanceof XSDUnsignedByteIV<?>) {
            return this.value == ((XSDUnsignedByteIV<?>) o).value;
        }
        return false;
    }
    
    /**
     * Return the hash code of the byte value.
     * 
     * @see Byte#hashCode()
     */
    public int hashCode() {
        return (int) value;
    }

    public int byteLength() {
        return 1 + 1;
    }

    @Override
    protected int _compareTo(final IV o) {
         
        final short value = promote();
        
        final short value2 = ((XSDUnsignedByteIV) o).promote();
        
        return value == value2 ? 0 : value < value2 ? -1 : 1;
        
    }

}

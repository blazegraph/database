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

import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;

/** Implementation for inline <code>xsd:double</code>. */
public class XSDDoubleInternalValue<V extends BigdataLiteral> extends
        AbstractDatatypeLiteralInternalValue<V, Double> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private final double value;

    public XSDDoubleInternalValue(final double value) {
        
        super(DTE.XSDDouble);
        
        this.value = value;
        
    }

    final public Double getInlineValue() {
        return value;
    }

    @SuppressWarnings("unchecked")
    public V asValue(final BigdataValueFactory f) {
        final V v = (V) f.createLiteral(value);
        v.setIV(this);
        return v;
    }

    @Override
    final public float floatValue() {
        return (float) value;
    }

    @Override
    public boolean booleanValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte byteValue() {
        return (byte) value;
    }

    @Override
    public double doubleValue() {
        return value;
    }

    @Override
    public int intValue() {
        return (int) value;
    }

    @Override
    public long longValue() {
        return (long) value;
    }

    @Override
    public short shortValue() {
        return (short) value;
    }

    @Override
    public BigDecimal decimalValue() {
        return BigDecimal.valueOf(value);
    }

    @Override
    public BigInteger integerValue() {
        return BigInteger.valueOf((long) value);
    }

    @Override
    public String stringValue() {
        return Double.toString(value);
    }
    
    public boolean equals(final Object o) {
        if(this==o) return true;
        if(o instanceof XSDDoubleInternalValue<?>) {
            return this.value == ((XSDDoubleInternalValue<?>) o).value;
        }
        return false;
    }
    
    /**
     * Return the hash code of the double value.
     * 
     * @see Double#hashCode()
     */
    public int hashCode() {

        final long t = Double.doubleToLongBits(value);

        return (int) (t ^ (t >>> 32));

    }

    public int byteLength() {
        return 1 + Bytes.SIZEOF_DOUBLE;
    }
    
    @Override
    protected int _compareTo(IV o) {
         
        final double value2 = ((XSDDoubleInternalValue) o).value;
        
        return value == value2 ? 0 : value < value2 ? -1 : 1;
        
    }
    

}
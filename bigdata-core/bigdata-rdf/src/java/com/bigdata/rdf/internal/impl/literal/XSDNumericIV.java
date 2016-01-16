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

package com.bigdata.rdf.internal.impl.literal;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.openrdf.model.Literal;

import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.util.Bytes;

/** 
 * Implementation for inline <code>xsd:byte</code>, <code>xsd:short</code>,
 * <code>xsd:int</code>, <code>xsd:long</code>, <code>xsd:float</code>,
 * <code>xsd:decimal</code>. 
 */
public class XSDNumericIV<V extends BigdataLiteral> extends
        NumericIV<V, Number> implements Literal {

    /**
     * 
     */
    private static final long serialVersionUID = 2330208360371357672L;
    
    private final Number number;

    public IV<V, Number> clone(final boolean clearCache) {

        final XSDNumericIV<V> tmp = new XSDNumericIV<V>(number, getDTE());

        if (!clearCache) {

            tmp.setValue(getValueCache());
            
        }
        
        return tmp;

    }

    private XSDNumericIV(final Number number, final DTE dte) {
        
        super(dte);
        
        this.number = number;
        
    }
    
    public XSDNumericIV(final byte number) {
        this(number, DTE.XSDByte);
    }

    public XSDNumericIV(final short number) {
        this(number, DTE.XSDShort);
    }

    public XSDNumericIV(final int number) {
        this(number, DTE.XSDInt);
    }

    public XSDNumericIV(final long number) {
        this(number, DTE.XSDLong);
    }

    public XSDNumericIV(final float number) {
        this(number, DTE.XSDFloat);
    }

    public XSDNumericIV(final double number) {
        this(number, DTE.XSDDouble);
    }

    final public Number getInlineValue() {
        return number;
    }

    @Override
    public boolean booleanValue() {
    	
    	switch (getDTE()) {
	        case XSDByte:
	            return byteValue() != 0;
	        case XSDShort:
	            return shortValue() != 0;
	        case XSDInt:
	            return intValue() != 0;
	        case XSDLong:
	            return longValue() != 0;
	        default:
	            return super.booleanValue();
		}
    	
    }
    
    @Override
    public byte byteValue()    {
        return number.byteValue();
    }

    @Override
    public short shortValue() {
        return number.shortValue();
    }

    @Override
    public int intValue() {
        return number.intValue();
    }

    @Override
    public long longValue() {
        return number.longValue();
    }

    @Override
    public float floatValue() {
        return number.floatValue();
    }

    @Override
    public double doubleValue() {
        return number.doubleValue();
    }
    
    @Override
    public BigInteger integerValue() {
        return BigInteger.valueOf(longValue());
    }

    @Override
    public BigDecimal decimalValue() {
    	
    	switch (getDTE()) {
	        case XSDByte:
	        case XSDShort:
	        case XSDInt:
	        case XSDLong:
	        	return BigDecimal.valueOf(longValue());
	        case XSDFloat:
	        case XSDDouble:
	        	return BigDecimal.valueOf(doubleValue());
	        default:
                throw new RuntimeException("unknown DTE");
		}
        
    }
    
    @SuppressWarnings("unchecked")
    public V asValue(final LexiconRelation lex) {

        V v = getValueCache();
        
        if( v == null) {
            
            switch(getDTE()) {
                case XSDByte:
                    v = (V) lex.getValueFactory().createLiteral(byteValue());
                    break;
                case XSDShort:
                    v = (V) lex.getValueFactory().createLiteral(shortValue());
                    break;
                case XSDInt:
                    v = (V) lex.getValueFactory().createLiteral(intValue());
                    break;
                case XSDLong:
                    v = (V) lex.getValueFactory().createLiteral(longValue());
                    break;
                case XSDFloat:
                    v = (V) lex.getValueFactory().createLiteral(floatValue());
                    break;
                case XSDDouble:
                    v = (V) lex.getValueFactory().createLiteral(doubleValue());
                    break;
                default:
                    throw new RuntimeException("unknown DTE");
            }
            
            v.setIV(this);
            
            setValue(v);
            
        }

        return v;
        
    }

    @Override
    public boolean equals(final Object o) {
    	
        if (o == this) 
        	return true;
        
        if(o instanceof XSDNumericIV<?>) {
        	
            final XSDNumericIV<?> n = (XSDNumericIV<?>) o;
            
            return this.getDTE() == n.getDTE() && this.number.equals(n.number);
            
        }
        
        return false;
        
    }
    
    @Override
    public int _compareTo(final IV o) {
         
    	final XSDNumericIV<?> n = (XSDNumericIV<?>) o;
    	
    	switch (getDTE()) {
	        case XSDByte:
	        	return byteValue() == n.byteValue() ? 0 : 
    				byteValue() < n.byteValue() ? -1 : 1;
	        case XSDShort:
	        	return shortValue() == n.shortValue() ? 0 : 
    				shortValue() < n.shortValue() ? -1 : 1;
	        case XSDInt:
	        	return intValue() == n.intValue() ? 0 : 
    				intValue() < n.intValue() ? -1 : 1;
	        case XSDLong:
	        	return longValue() == n.longValue() ? 0 : 
    				longValue() < n.longValue() ? -1 : 1;
	        case XSDFloat:
	            // Note: This handles NaN, etc.
	            return Float.compare(floatValue(), n.floatValue());
	        case XSDDouble:
	            // Note: This handles NaN, etc.
	            return Double.compare(doubleValue(), n.doubleValue());
	        default:
	            throw new RuntimeException("unknown DTE");
    	}
    	
    }
    
    public int hashCode() {
    	
    	switch (getDTE()) {
	        case XSDByte:
	            return (int) byteValue();
	        case XSDShort:
	            return (int) shortValue();
	        case XSDInt:
	            return intValue();
	        case XSDLong:
	            return (int) (longValue() ^ (longValue() >>> 32));
	        case XSDFloat:
	            return Float.floatToIntBits(floatValue());
	        case XSDDouble:
	            final long l = Double.doubleToLongBits(doubleValue());
	            return (int) (l ^ (l >>> 32));
	        default:
	            throw new RuntimeException("unknown DTE");
    	}
    	
    }

    public int byteLength() {
    	
    	switch (getDTE()) {
	        case XSDByte:
	            return 1 + Bytes.SIZEOF_BYTE;
	        case XSDShort:
	            return 1 + Bytes.SIZEOF_SHORT;
	        case XSDInt:
	            return 1 + Bytes.SIZEOF_INT;
	        case XSDLong:
	            return 1 + Bytes.SIZEOF_LONG;
	        case XSDFloat:
	            return 1 + Bytes.SIZEOF_FLOAT;
	        case XSDDouble:
	            return 1 + Bytes.SIZEOF_DOUBLE;
	        default:
	            throw new RuntimeException("unknown DTE");
    	}
    	
    }

}

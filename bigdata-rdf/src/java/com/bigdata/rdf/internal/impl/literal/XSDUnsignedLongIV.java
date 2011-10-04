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

/** Implementation for inline <code>xsd:unsignedLong</code>. */
public class XSDUnsignedLongIV<V extends BigdataLiteral> extends
        AbstractLiteralIV<V, BigInteger> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    /**
     * The unsigned long value.
     */
    private final long value;

    /**
     * The unsigned long value.
     */
    public long rawValue() {

        return value;
        
    }

    public XSDUnsignedLongIV(final long value) {
        
        super(DTE.XSDUnsignedLong);
        
        this.value = value;
        
    }

    /**
     * Promote the <code>unsigned long</code> into a signed {@link BigInteger}.
     */
    public final BigInteger promote() {

        long v = value;
        
        if (v < 0) {
            
            v = v + 0x8000000000000000L;

        } else {
            
            v = v - 0x8000000000000000L;
            
        }

        return BigInteger.valueOf(v);

//        final BigInteger v;
//        
//        if (value < 0) {
//
//            v = BigInteger.valueOf(value - 0x80000000);
//
//        } else {
//            
//            v = BigInteger.valueOf(0x80000000 + value);
//            
//        }
//
//        return v;

//        final BigInteger v = BigInteger.valueOf(value);
//
//        if (value < 0) {
//
//            v = v.subtract(BigInteger.valueOf(0x80000000)); // TODO extract constant.
//
//        } else {
//            
//            v = v.add(BigInteger.valueOf(0x80000000));// TODO extract constant.
//            
//        }
//        
//        return v;
        
    }

    final public BigInteger getInlineValue() {
        return promote();
    }

	@SuppressWarnings("unchecked")
	public V asValue(final LexiconRelation lex) {
		V v = getValueCache();
		if (v == null) {
			final BigdataValueFactory f = lex.getValueFactory();
			v = (V) f.createLiteral(value); // FIXME In all xsd:unsigned classes and unit test!
			v.setIV(this);
			setValue(v);
		}
		return v;
    }

    @Override
    final public long longValue() {
        return promote().longValue();
    }

    /*
     * From the spec: If the argument is a numeric type or a typed literal with
     * a datatype derived from a numeric type, the EBV is false if the operand
     * value is NaN or is numerically equal to zero; otherwise the EBV is true.
     */
    @Override
    public boolean booleanValue() {
        /*
         * TODO This can be optimized using the known signed representation of
         * the unsigned ZERO (this is true for all of the xsd:unsigned classes).
         */
        return value != UNSIGNED_ZERO ? true : false;
    }

    static private final long UNSIGNED_ZERO = 0x8000000000000000L;

    @Override
    public byte byteValue() {
        return (byte) promote().byteValue();
    }

    @Override
    public double doubleValue() {
        return (double) promote().doubleValue();
    }

    @Override
    public float floatValue() {
        return (float) promote().floatValue();
    }

    @Override
    public int intValue() {
        return (int) promote().intValue();
    }

    @Override
    public short shortValue() {
        return (short) promote().shortValue();
    }
    
    @Override
    public String stringValue() {
        return promote().toString();
    }

    @Override
    public BigDecimal decimalValue() {
        return new BigDecimal(promote());
    }

    @Override
    public BigInteger integerValue() {
        return promote();
    }

    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof XSDUnsignedLongIV<?>) {
            return this.value == ((XSDUnsignedLongIV<?>) o).value;
        }
        return false;
    }

    /**
     * Return the hash code of the long value.
     */
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }

    public int byteLength() {
        return 1 + Bytes.SIZEOF_LONG;
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public int _compareTo(final IV o) {
 
        final XSDUnsignedLongIV<?> t = (XSDUnsignedLongIV<?>) o;
        
        return value == t.value ? 0 : value < t.value ? -1 : 1;
        
    }
    
}

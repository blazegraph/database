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
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;

/** Implementation for inline <code>xsd:long</code>. */
public class XSDLongIV<V extends BigdataLiteral> extends
        AbstractLiteralIV<V, Long> {

    /**
     * 
     */
    private static final long serialVersionUID = -6972910330385311194L;
    
    private final long value;

    public XSDLongIV(final long value) {
        
        super(DTE.XSDLong);
        
        this.value = value;
        
    }

    final public Long getInlineValue() {
        return value;
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
        return value;
    }

    @Override
    public boolean booleanValue() {
        return value == 0 ? false : true;
    }

    @Override
    public byte byteValue() {
        return (byte) value;
    }

    @Override
    public double doubleValue() {
        return (double) value;
    }

    @Override
    public float floatValue() {
        return (float) value;
    }

    @Override
    public int intValue() {
        return (int) value;
    }

    @Override
    public short shortValue() {
        return (short) value;
    }
    
    @Override
    public String stringValue() {
        return Long.toString(value);
    }

    @Override
    public BigDecimal decimalValue() {
        return BigDecimal.valueOf(value);
    }

    @Override
    public BigInteger integerValue() {
        return BigInteger.valueOf(value);
    }

    public boolean equals(final Object o) {
        if(this==o) return true;
        if(o instanceof XSDLongIV<?>) {
            return this.value == ((XSDLongIV<?>) o).value;
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
    
    @Override
    protected int _compareTo(IV o) {
         
        final long value2 = ((XSDLongIV) o).value;
        
        return value == value2 ? 0 : value < value2 ? -1 : 1;
        
    }
    
}
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

/** Implementation for inline <code>xsd:boolean</code>. */
public class XSDBooleanIV<V extends BigdataLiteral> extends
        AbstractLiteralIV<V, Boolean> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    static public transient final XSDBooleanIV<BigdataLiteral> TRUE = 
    	new XSDBooleanIV<BigdataLiteral>(true);

    static public transient final XSDBooleanIV<BigdataLiteral> FALSE = 
    	new XSDBooleanIV<BigdataLiteral>(false);
    
    static public final XSDBooleanIV valueOf(final boolean b) {
    	return b ? TRUE : FALSE;
    }
    
    private final boolean value;

    public XSDBooleanIV(final boolean value) {
        
        super(DTE.XSDBoolean);
        
        this.value = value;
        
    }

    final public Boolean getInlineValue() {

        return value ? Boolean.TRUE : Boolean.FALSE;

    }

    @SuppressWarnings("unchecked")
    public V asValue(final LexiconRelation lex) {

    	V v = getValueCache();
    	
    	if( v == null) {
    		
    		v = (V) lex.getValueFactory().createLiteral(value);
    		
    		v.setIV(this);
    		
    		setValue(v);
    		
    	}

    	return v;
    	
    }

    @Override
    final public long longValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean booleanValue() {
        return value;
    }

    @Override
    public byte byteValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double doubleValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public float floatValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int intValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public short shortValue() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public String stringValue() {
        return Boolean.toString(value);
    }

    @Override
    public BigDecimal decimalValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BigInteger integerValue() {
        throw new UnsupportedOperationException();
    }

    public boolean equals(final Object o) {
        if(this==o) return true;
        if(o instanceof XSDBooleanIV<?>) {
            return this.value == ((XSDBooleanIV<?>) o).value;
        }
        return false;
    }
    
    /**
     * Return the hash code of the byte value.
     * 
     * @see Boolean#hashCode()
     */
    public int hashCode() {
        return value ? Boolean.TRUE.hashCode() : Boolean.FALSE.hashCode();
    }

    public int byteLength() {
        return 1 + 1;
    }

    @Override
    protected int _compareTo(IV o) {
         
        final boolean v = ((XSDBooleanIV) o).value;
        
        return (v == value ? 0 : (value ? 1 : -1));
        
    }
    

}
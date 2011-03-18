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
import java.util.UUID;

import org.openrdf.model.ValueFactory;

import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * Implementation for inline {@link UUID}s (there is no corresponding XML
 * Schema Datatype).
 */
public class UUIDLiteralIV<V extends BigdataLiteral> extends
        AbstractLiteralIV<V, UUID> {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private final UUID value;

    public UUIDLiteralIV(final UUID value) {
        
        super(DTE.UUID);

        if (value == null)
            throw new IllegalArgumentException();
        
        this.value = value;
        
    }

    final public UUID getInlineValue() {
        return value;
    }

	@SuppressWarnings("unchecked")
	public V asValue(final LexiconRelation lex) {
	
		V v = getValueCache();
		
		if (v == null) {
			
			final ValueFactory f = lex.getValueFactory();
			
			v = (V) f.createLiteral(value.toString(), //
					f.createURI(DTE.UUID.getDatatype()));
			
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
        throw new UnsupportedOperationException();
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
        return value.toString();
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
        if (this == o)
            return true;
        if (o instanceof UUIDLiteralIV<?>) {
            return this.value.equals(((UUIDLiteralIV<?>) o).value);
        }
        return false;
    }
    
    /**
     * Return the hash code of the {@link UUID}.
     */
    public int hashCode() {
        return value.hashCode();
    }

    public int byteLength() {
        return 1 + Bytes.SIZEOF_UUID;
    }

    @Override
    protected int _compareTo(IV o) {
         
        return value.compareTo(((UUIDLiteralIV) o).value);
        
    }
    
    
}
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
package com.bigdata.rdf.internal.impl;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import javax.xml.datatype.XMLGregorianCalendar;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Abstract base class for {@link IV}s which CAN NOT be fully materialized
 * from their inline representation.
 */
abstract public class AbstractNonInlineIV<V extends BigdataValue, T> 
		extends AbstractIV<V, T> implements Literal, BNode, URI  {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    protected AbstractNonInlineIV(final byte flags) {

        super(flags);

    }

    protected AbstractNonInlineIV(final VTE vte, final boolean extension,
            final DTE dte) {

        super(vte, false/* inline */, extension, dte);

    }

//    /**
//     * Callers must explicitly populate the value cache.
//     * <p>
//     * {@inheritDoc}
//     */
//    @Override
//    final public V setValue(V v) {
//    	
//    	return super.setValue(v);
//    	
//    }
    
    /**
     * Operation is not supported because this {@link IV} type is not 100%
     * inline. You MUST explicitly set the value cache.
     * <p>
     * {@inheritDoc}
     * 
     * @see #setValue(BigdataValue)
     */
    final public V asValue(final LexiconRelation lex) {
    
		return getValue();
//        throw new UnsupportedOperationException();
        
    }

    /**
     * Operation is not supported because this {@link IV} type is not 100%
     * inline.
     */
    final public T getInlineValue() {

        throw new UnsupportedOperationException();
        
    }

    /**
     * Always returns <code>false</code> since the RDF value is not inline.
     */
    @Override
    final public boolean isInline() {

        return false;
        
    }
    
    /**
     * Always returns <code>true</code> since the RDF value is not inline.
     */
    @Override
    final public boolean needsMaterialization() {
    	
    	return true;
    	
    }

    /**
     * Override default serialization to send the cached {@link BigdataValue}.
     */
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {

        out.defaultWriteObject();

        out.writeObject(getValueCache());

    }

    /**
     * Override default serialization to recover the cached {@link BigdataValue}
     * .
     */
    @SuppressWarnings("unchecked")
    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {

        in.defaultReadObject();

        final V v = (V) in.readObject();

        if (v != null) {
            // set the value cache.
            setValue(v);
        }

    }
    
    /**
     * Implements {@link Value#stringValue()}.
     */
	@Override
	public String stringValue() {
		
		return getValue().stringValue();
		
	}

    /**
     * Implements {@link URI#getLocalName()}.
     */
    public String getLocalName() {
    	
    	if (!isURI())
    		throw new ClassCastException();
    	
		return ((URI) getValue()).getLocalName();
    	
    }

    /**
     * Implements {@link URI#getNamespace()}.
     */
    public String getNamespace() {

        if (!isURI())
            throw new ClassCastException();

		return ((URI) getValue()).getNamespace();
    	
    }

    /**
     * Implements {@link BNode#getID()}.
     */
    public String getID() {

        if (!isBNode())
            throw new ClassCastException();

        return ((BNode) getValue()).getID();
    	
    }

    /**
     * Implements {@link Literal#getLabel()}.
     */
	@Override
	public String getLabel() {
		
		if (!isLiteral())
			throw new ClassCastException();
		
		return ((Literal) getValue()).getLabel();
		
	}

    /**
     * Implements {@link Literal#getDatatype()}.
     */
	@Override
	public URI getDatatype() {
		
		if (!isLiteral())
			throw new ClassCastException();
		
		return ((Literal) getValue()).getDatatype();
		
	}

    /**
     * Implements {@link Literal#getLanguage()}.
     */
	@Override
	public String getLanguage() {
		
		if (!isLiteral())
			throw new ClassCastException();
		
		return ((Literal) getValue()).getLanguage();
		
	}

    /**
     * Implements {@link Literal#booleanValue()}.
     */
	@Override
	public boolean booleanValue() {
		
		if (!isLiteral())
			throw new ClassCastException();
		
		return ((Literal) getValue()).booleanValue();
		
	}

    /**
     * Implements {@link Literal#byteValue()}.
     */
	@Override
	public byte byteValue() {
		
		if (!isLiteral())
			throw new ClassCastException();
		
		return ((Literal) getValue()).byteValue();
		
	}

    /**
     * Implements {@link Literal#shortValue()}.
     */
	@Override
	public short shortValue() {
		
		if (!isLiteral())
			throw new ClassCastException();
		
		return ((Literal) getValue()).shortValue();
		
	}

    /**
     * Implements {@link Literal#intValue()}.
     */
	@Override
	public int intValue() {
		
		if (!isLiteral())
			throw new ClassCastException();
		
		return ((Literal) getValue()).intValue();
		
	}

    /**
     * Implements {@link Literal#longValue()}.
     */
	@Override
	public long longValue() {
		
		if (!isLiteral())
			throw new ClassCastException();
		
		return ((Literal) getValue()).longValue();
		
	}

    /**
     * Implements {@link Literal#floatValue()}.
     */
	@Override
	public float floatValue() {
		
		if (!isLiteral())
			throw new ClassCastException();
		
		return ((Literal) getValue()).floatValue();
		
	}

    /**
     * Implements {@link Literal#doubleValue()}.
     */
	@Override
	public double doubleValue() {
		
		if (!isLiteral())
			throw new ClassCastException();
		
		return ((Literal) getValue()).doubleValue();
		
	}

    /**
     * Implements {@link Literal#integerValue()}.
     */
	@Override
	public BigInteger integerValue() {
		
		if (!isLiteral())
			throw new ClassCastException();
		
		return ((Literal) getValue()).integerValue();
		
	}

    /**
     * Implements {@link Literal#decimalValue()}.
     */
	@Override
	public BigDecimal decimalValue() {
		
		if (!isLiteral())
			throw new ClassCastException();
		
		return ((Literal) getValue()).decimalValue();
		
	}

    /**
     * Implements {@link Literal#calendarValue()}.
     */
	@Override
	public XMLGregorianCalendar calendarValue() {
		
		if (!isLiteral())
			throw new ClassCastException();
		
		return ((Literal) getValue()).calendarValue();
		
	}

}

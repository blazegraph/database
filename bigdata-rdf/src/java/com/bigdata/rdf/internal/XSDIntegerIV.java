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

import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;

/** Implementation for inline <code>xsd:integer</code>. */
public class XSDIntegerIV<V extends BigdataLiteral> extends
        AbstractLiteralIV<V, BigInteger> {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private final BigInteger value;
    private transient int byteLength;

    public XSDIntegerIV(final BigInteger value) {
        
        super(DTE.XSDInteger);

        if (value == null)
            throw new IllegalArgumentException();
        
        this.value = value;
        
    }

    final public BigInteger getInlineValue() {

        return value;
        
    }

    @SuppressWarnings("unchecked")
    public V asValue(final BigdataValueFactory f) {
        // @todo factory should cache the XSD URIs.
        final V v = (V) f.createLiteral(value.toString(),//
                f.createURI(DTE.XSDInteger.getDatatype()));
        v.setIV(this);
        return v;
    }

    @Override
    final public long longValue() {
        return value.longValue();
    }

    @Override
    public boolean booleanValue() {
        return value.equals(BigInteger.ZERO) ? false : true;
    }

    @Override
    public byte byteValue() {
        return value.byteValue();
    }

    @Override
    public double doubleValue() {
        return value.doubleValue();
    }

    @Override
    public float floatValue() {
        return value.floatValue();
    }

    @Override
    public int intValue() {
        return value.intValue();
    }

    @Override
    public short shortValue() {
        return value.shortValue();
    }
    
    @Override
    public String stringValue() {
        return value.toString();
    }

    @Override
    public BigDecimal decimalValue() {
        return new BigDecimal(value);
    }

    @Override
    public BigInteger integerValue() {
        return value;
    }

    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof XSDIntegerIV<?>) {
            return this.value.equals(((XSDIntegerIV<?>) o).value);
        }
        return false;
    }

    /**
     * Return the hash code of the {@link BigInteger}.
     */
    public int hashCode() {
        return value.hashCode();
    }

    public int byteLength() {

        if (byteLength == 0) {

            /*
             * Cache the byteLength if not yet set.
             */

            byteLength = 1 /* prefix */+ 2/* runLength */+ (value.bitLength() / 8 + 1)/* data */;

        }

        return byteLength;

    }
    
    @Override
    protected int _compareTo(IV o) {
        
        return value.compareTo(((XSDIntegerIV) o).value);
        
    }
    
}
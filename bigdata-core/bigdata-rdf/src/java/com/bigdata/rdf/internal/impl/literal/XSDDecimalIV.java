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

import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;

/** Implementation for inline <code>xsd:decimal</code>. */
public class XSDDecimalIV<V extends BigdataLiteral> extends
        NumericIV<V, BigDecimal> implements Literal {
    
    /**
	 * 
	 */
	private static final long serialVersionUID = -7549521965119949938L;
	
	private final BigDecimal value;
	
    private transient int byteLength;

    public IV<V, BigDecimal> clone(final boolean clearCache) {

        final XSDDecimalIV<V> tmp = new XSDDecimalIV<V>(value);

        // propagate transient state if available.
        tmp.byteLength = byteLength;

        if (!clearCache) {

            tmp.setValue(getValueCache());
            
        }
        
        return tmp;

    }

    public XSDDecimalIV(final BigDecimal value) {
        
        super(DTE.XSDDecimal);

        if (value == null)
            throw new IllegalArgumentException();
        
        this.value = value;
        
    }

    final public BigDecimal getInlineValue() {

        return value;
        
    }

    /**
     * Use toPlainString to avoid expression with exponent value that 
     * would imply xsd:double rather than xsd:decimal
     */
    @Override
    public String getLabel() {
        return value.toPlainString();
    }

    @Override
    public boolean booleanValue() {
        return value.equals(BigDecimal.ZERO) ? false : true;
    }

    @Override
    public byte byteValue() {
        return value.byteValue();
    }

    @Override
    public short shortValue() {
        return value.shortValue();
    }
    
    @Override
    public int intValue() {
        return value.intValue();
    }

    @Override
    final public long longValue() {
        return value.longValue();
    }

    @Override
    public float floatValue() {
        return value.floatValue();
    }

    @Override
    public double doubleValue() {
        return value.doubleValue();
    }

    @Override
    public BigInteger integerValue() {
        return value.toBigInteger();
    }

    @Override
    public BigDecimal decimalValue() {
        return value;
    }

    @SuppressWarnings("unchecked")
    public V asValue(final LexiconRelation lex) {
		V v = getValueCache();
		if (v == null) {
			final BigdataValueFactory f = lex.getValueFactory();
			v = (V) f.createLiteral(
					value.toPlainString(), DTE.XSDDecimal.getDatatypeURI());
			v.setIV(this);
			setValue(v);
		}
		return v;
    }

    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof XSDDecimalIV<?>) {
            // Note: This handles values which differ in precision.
            return this.value.compareTo(((XSDDecimalIV<?>) o).value) == 0;
//            return this.value.equals(((XSDDecimalIV<?>) o).value);
        }
        return false;
    }

    /**
     * Return the hash code of the value returned by
     * {@link BigDecimal#stripTrailingZeros()}.
     * <p>
     * Note: normalization is necessary to have a stable hash code when encoding
     * and decoding for much the same reason that we have to use
     * {@link BigDecimal#compareTo(BigDecimal)} in {@link #equals(Object)}.
     */
    public int hashCode() {
        if (hash == 0) {
            hash = value.stripTrailingZeros().hashCode();
        }
        return hash;
    }
    private int hash = 0;

    public int byteLength() {
        
        if (byteLength == 0) {

            /*
             * Cache the byteLength if not yet set.
             */
            byteLength = 1 /* flags */ + KeyBuilder.byteLength(value);

        }

        return byteLength;
        
    }
        
    @Override
    public int _compareTo(IV o) {
        
        return value.compareTo(((XSDDecimalIV) o).value);
        
    }

}

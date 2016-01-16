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

import javax.xml.datatype.XMLGregorianCalendar;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.datatypes.XMLDatatypeUtil;

import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.InlineLiteralIV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.AbstractInlineIV;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * Abstract base class for RDF datatype literals adds primitive data type
 * value access methods.
 * <p>
 * {@inheritDoc}
 * 
 * @see http://www.w3.org/TR/rdf-sparql-query/#FunctionMapping, The casting
 *      rules for SPARQL
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id: TestEncodeDecodeKeys.java 2753 2010-05-01 16:36:59Z
 *          thompsonbry $
 */
abstract public class AbstractLiteralIV<V extends BigdataLiteral, T>
        extends AbstractInlineIV<V, T> implements Literal, InlineLiteralIV<V, T> {

    /**
     * 
     */
    private static final long serialVersionUID = 5962615541158537189L;

    protected AbstractLiteralIV(final DTE dte) {

        super(VTE.LITERAL, dte);

    }

    /**
     * Implement {@link IV#needsMaterialization()}. Materialization not required
     * to answer the {@link Literal} interface methods.
     */
    @Override
    final public boolean needsMaterialization() {
    	return false;
    }
    
    /**
     * Implement {@link Value#stringValue()}.
     */
    @Override
    public String stringValue() {
    	return getLabel();
    }
    
    /**
     * Implement {@link Literal#getLabel()}.
     */
    @Override
    public String getLabel() {
    	return getInlineValue().toString();
    }
    
    /**
     * Implement {@link Literal#getDatatype()}.
     */
    @Override
    public URI getDatatype() {
        return getDTE() == DTE.Extension ? 
                getDTEX().getDatatypeURI() : getDTE().getDatatypeURI();
    }
    
    /**
     * Implement {@link Literal#getLanguage()}.
     */
    @Override
    public String getLanguage() {
    	return null;
    }
    
    /**
     * Implement {@link Literal#booleanValue()}.
     */
    @Override
	public boolean booleanValue() {
		return XMLDatatypeUtil.parseBoolean(getLabel());
	}

    /**
     * Implement {@link Literal#byteValue()}.
     */
    @Override
	public byte byteValue() {
		return XMLDatatypeUtil.parseByte(getLabel());
	}

    /**
     * Implement {@link Literal#shortValue()}.
     */
    @Override
	public short shortValue() {
		return XMLDatatypeUtil.parseShort(getLabel());
	}

    /**
     * Implement {@link Literal#intValue()}.
     */
    @Override
	public int intValue() {
		return XMLDatatypeUtil.parseInt(getLabel());
	}

    /**
     * Implement {@link Literal#longValue()}.
     */
    @Override
	public long longValue() {
		return XMLDatatypeUtil.parseLong(getLabel());
	}

    /**
     * Implement {@link Literal#floatValue()}.
     */
    @Override
	public float floatValue() {
		return XMLDatatypeUtil.parseFloat(getLabel());
	}

    /**
     * Implement {@link Literal#doubleValue()}.
     */
    @Override
	public double doubleValue() {
		return XMLDatatypeUtil.parseDouble(getLabel());
	}

    /**
     * Implement {@link Literal#integerValue()}.
     */
    @Override
	public BigInteger integerValue() {
		return XMLDatatypeUtil.parseInteger(getLabel());
	}

    /**
     * Implement {@link Literal#decimalValue()}.
     */
    @Override
	public BigDecimal decimalValue() {
		return XMLDatatypeUtil.parseDecimal(getLabel());
	}

    /**
     * Implement {@link Literal#calendarValue()}.
     */
    @Override
	public XMLGregorianCalendar calendarValue() {
		return XMLDatatypeUtil.parseCalendar(getLabel());
	}
	
    @Override
    public String toString() {
        return super.getDTE() + "(" + stringValue() + ")";
    }

//    /** Return the <code>boolean</code> value of <i>this</i> value. */
//    abstract public boolean booleanValue();
//
//    /**
//     * Return the <code>byte</code> value of <i>this</i> value.
//     * <p>
//     * Note: Java lacks unsigned data types. For safety, operations on
//     * unsigned XSD data types should be conducted after a widening
//     * conversion. For example, operations on <code>xsd:unsignedByte</code>
//     * should be performed using {@link #shortValue()}.
//     */
//    abstract public byte byteValue();
//
//    /**
//     * Return the <code>short</code> value of <i>this</i> value.
//     * <p>
//     * Note: Java lacks unsigned data types. For safety, operations on
//     * unsigned XSD data types should be conducted after a widening
//     * conversion. For example, operations on <code>xsd:unsignedShort</code>
//     * should be performed using {@link #intValue()}.
//     */
//    abstract public short shortValue();
//
//    /**
//     * Return the <code>int</code> value of <i>this</i> value.
//     * <p>
//     * Note: Java lacks unsigned data types. For safety, operations on
//     * unsigned XSD data types should be conducted after a widening
//     * conversion. For example, operations on <code>xsd:unsignedInt</code>
//     * should be performed using {@link #longValue()}.
//     */
//    abstract public int intValue();
//
//    /**
//     * Return the <code>long</code> value of <i>this</i> value.
//     * <p>
//     * Note: Java lacks unsigned data types. For safety, operations on
//     * unsigned XSD data types should be conducted after a widening
//     * conversion. For example, operations on <code>xsd:unsignedLong</code>
//     * should be performed using {@link #integerValue()}.
//     */
//    abstract public long longValue();
//
//    /** Return the <code>float</code> value of <i>this</i> value. */
//    abstract public float floatValue();
//
//    /** Return the <code>double</code> value of <i>this</i> value. */
//    abstract public double doubleValue();
//
//    /** Return the {@link BigInteger} value of <i>this</i> value. */
//    abstract public BigInteger integerValue();
//
//    /** Return the {@link BigDecimal} value of <i>this</i> value. */
//    abstract public BigDecimal decimalValue();
//    
//    abstract public XMLGregorianCalendar calendarValue();

}

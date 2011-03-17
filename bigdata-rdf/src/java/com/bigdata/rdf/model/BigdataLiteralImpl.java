/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Apr 16, 2008
 */

package com.bigdata.rdf.model;

import java.math.BigDecimal;
import java.math.BigInteger;

import javax.xml.datatype.XMLGregorianCalendar;

import org.openrdf.model.Literal;
import org.openrdf.model.datatypes.XMLDatatypeUtil;

/**
 * A literal. Use {@link BigdataValueFactory} to create instances of this class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataLiteralImpl extends BigdataValueImpl implements
        BigdataLiteral {

    /**
     * 
     */
    private static final long serialVersionUID = 2301819664179569810L;

    private final String label;
    private final String language;
    private final BigdataURI datatype;

    /**
     * Used by {@link BigdataValueFactoryImpl}.
     */
    BigdataLiteralImpl(final BigdataValueFactory valueFactory,
            final String label, final String language, final BigdataURI datatype) {

        super(valueFactory, null);

        if (label == null)
            throw new IllegalArgumentException();

        if (language != null && datatype != null)
            throw new IllegalArgumentException();
        
        this.label = label;
        
        // force to lowercase (Sesame does this too).
        this.language = (language != null ? language.toLowerCase().intern() : null);
//        this.language = language;
        
        this.datatype = datatype;
        
    }

    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append('\"');
        
        sb.append(label);
        
        sb.append('\"');

        if (language != null) {
            
            sb.append('@');
            
            sb.append(language);
            
        } else if (datatype != null) {
        
            sb.append("^^<");
            
            sb.append(datatype);
            
            sb.append('>');
            
        }
        
        return sb.toString();
        
    }
    
    public String stringValue() {
       
        return label;
        
    }

    final public String getLabel() {

        return label;
        
    }

    final public String getLanguage() {

        return language;
        
    }

    final public BigdataURI getDatatype() {

        return datatype;
        
    }

    final public int hashCode() {
        
        return label.hashCode();
        
    }
    
    final public boolean equals(Object o) {

        if (!(o instanceof Literal))
            return false;

        return equals((Literal)o);
        
    }
    
    final public boolean equals(final Literal o) {

        if (this == o)
            return true;
        
        if (o == null)
            return false;

        if (!label.equals(o.getLabel()))
            return false;

        if (language != null) {

            // the language code is case insensitive.
            return language.equalsIgnoreCase(o.getLanguage());

        } else if (o.getLanguage() != null) {

            return false;

        }

        if (datatype != null) {

            return datatype.equals(o.getDatatype());

        } else if (o.getDatatype() != null) {

            return false;
            
        }
        
        return true;
        
    }
    
    /*
     * XSD stuff.
     */
    
    final public boolean booleanValue() {

        return XMLDatatypeUtil.parseBoolean(label);

    }

    final public byte byteValue() {

        return XMLDatatypeUtil.parseByte(label);

    }

    final public short shortValue() {

        return XMLDatatypeUtil.parseShort(label);

    }

    final public int intValue() {

        return XMLDatatypeUtil.parseInt(label);

    }

    final public long longValue() {

        return XMLDatatypeUtil.parseLong(label);

    }

    final public float floatValue() {

        return XMLDatatypeUtil.parseFloat(label);

    }

    final public double doubleValue() {

        return XMLDatatypeUtil.parseDouble(label);

    }

    final public BigInteger integerValue() {

        return XMLDatatypeUtil.parseInteger(label);

    }

    final public BigDecimal decimalValue() {

        return XMLDatatypeUtil.parseDecimal(label);

    }

    final public XMLGregorianCalendar calendarValue() {

        return XMLDatatypeUtil.parseCalendar(label);

    }

}

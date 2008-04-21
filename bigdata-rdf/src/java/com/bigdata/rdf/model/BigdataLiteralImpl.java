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

import com.bigdata.rdf.model.OptimizedValueFactory._Literal;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataLiteralImpl extends BigdataValueImpl implements BigdataLiteral {

    /**
     * 
     */
    private static final long serialVersionUID = 2301819664179569810L;

    private final String label;
    private final String language;
    private final BigdataURIImpl datatype;

    public BigdataLiteralImpl(_Literal lit) {
        
        this(lit.term, lit.language, (lit.datatype == null ? null : new BigdataURIImpl(
                lit.datatype)), lit.termId);
        
    }

    public BigdataLiteralImpl(String label, long termId) {
        
        this(label, null, null, termId);
        
    }

    public BigdataLiteralImpl(String label, String language, long termId) {
        
        this(label, language, null, termId);
        
    }

    public BigdataLiteralImpl(String label, BigdataURIImpl datatype, long termId) {

        this(label, null, datatype, termId);
        
    }
    
    public BigdataLiteralImpl(String label, String language, BigdataURIImpl datatype, long termId) {
        
        super( termId );

        if (label == null)
            throw new IllegalArgumentException();

        if (language != null && datatype != null)
            throw new IllegalArgumentException();
        
        this.label = label;
        
        // @todo force to lowercase when defined?
//        this.language = (language != null ? language.toLowerCase() : null);
        this.language = language;
        
        this.datatype = datatype;
        
    }

    // @todo return the fully formatted literal.
    public String toString() {
        
        return label;
        
    }
    
    public String stringValue() {
       
        return label;
        
    }

    public String getLabel() {

        return label;
        
    }

    public String getLanguage() {

        return language;
        
    }

    public BigdataURI getDatatype() {

        return datatype;
        
    }

    public int hashCode() {
        
        return label.hashCode();
        
    }
    
    public boolean equals(Object o) {

        if (!(o instanceof Literal))
            return false;

        return equals((Literal)o);
        
    }
    
    public boolean equals(Literal o) {

        if (this == o)
            return true;
        
        if (o == null)
            return false;

        if (!label.equals(o.getLabel()))
            return false;

        if (language != null) {

            return language.equals(o.getLanguage());

        } else if (datatype != null) {

            return datatype.equals(o.getDatatype());
            
        }
        
        return true;
        
    }
    
    /*
     * XSD stuff.
     */
    
    public boolean booleanValue() {

        return XMLDatatypeUtil.parseBoolean(label);

    }

    public byte byteValue() {

        return XMLDatatypeUtil.parseByte(label);

    }

    public short shortValue() {

        return XMLDatatypeUtil.parseShort(label);

    }

    public int intValue() {

        return XMLDatatypeUtil.parseInt(label);

    }

    public long longValue() {

        return XMLDatatypeUtil.parseLong(label);

    }

    public float floatValue() {

        return XMLDatatypeUtil.parseFloat(label);

    }

    public double doubleValue() {

        return XMLDatatypeUtil.parseDouble(label);

    }

    public BigInteger integerValue() {

        return XMLDatatypeUtil.parseInteger(label);

    }

    public BigDecimal decimalValue() {

        return XMLDatatypeUtil.parseDecimal(label);

    }

    public XMLGregorianCalendar calendarValue() {

        return XMLDatatypeUtil.parseCalendar(label);

    }

}

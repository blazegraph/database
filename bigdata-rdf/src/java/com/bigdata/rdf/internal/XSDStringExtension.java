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

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * This implementation of {@link IExtension} supports fully inlined
 * <code>xsd:string</code> values.
 */
public class XSDStringExtension<V extends BigdataValue> implements IExtension<V> {

    private final BigdataURI xsdStringURI;
    private final int maxInlineStringLength;
    
    public XSDStringExtension(final IDatatypeURIResolver resolver,
            final int maxInlineStringLength) {

        if (resolver == null)
            throw new IllegalArgumentException();

        if (maxInlineStringLength < 0)
            throw new IllegalArgumentException();

        this.xsdStringURI = resolver.resolve(XSD.STRING);
        
        this.maxInlineStringLength = maxInlineStringLength;
        
    }
        
    public BigdataURI getDatatype() {
        
        return xsdStringURI;
        
    }
    
    public ExtensionIV createIV(final Value value) {
        
        if (value instanceof Literal == false)
            throw new IllegalArgumentException();
        
        if (value.stringValue().length() > maxInlineStringLength) {
            // Too large to inline.
            return null;
        }    
        
        final Literal lit = (Literal) value;
        
        final URI dt = lit.getDatatype();
        
        // Note: URI.stringValue() is efficient....
        if (dt == null || !XSD.STRING.stringValue().equals(dt.stringValue()))
            throw new IllegalArgumentException();
        
        final String s = value.stringValue();

        final InlineLiteralIV<BigdataLiteral> delegate = new InlineLiteralIV<BigdataLiteral>(//
                s, // label
                null, // no language
                null // no datatype
        );

        return new ExtensionIV<BigdataLiteral>(delegate, (TermId) getDatatype()
                .getIV());

    }
    
    public V asValue(final ExtensionIV iv, final BigdataValueFactory vf) {

        return (V) vf.createLiteral(iv.getDelegate().stringValue(),
                xsdStringURI);

    }

}

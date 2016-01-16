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

package com.bigdata.rdf.internal.impl.extensions;

import java.util.LinkedHashSet;
import java.util.Set;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.internal.IDatatypeURIResolver;
import com.bigdata.rdf.internal.IExtension;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.FullyInlineTypedLiteralIV;
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
        
    public Set<BigdataURI> getDatatypes() {
        
        final Set<BigdataURI> datatypes = new LinkedHashSet<BigdataURI>();
        datatypes.add(xsdStringURI);
        return datatypes;
        
    }
    
    public LiteralExtensionIV createIV(final Value value) {
        
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

        final FullyInlineTypedLiteralIV<BigdataLiteral> delegate = new FullyInlineTypedLiteralIV<BigdataLiteral>(//
                s, // label
                null, // no language
                null // no datatype
        );

        return new LiteralExtensionIV<BigdataLiteral>(delegate, xsdStringURI.getIV());

    }
    
    public V asValue(final LiteralExtensionIV iv, final BigdataValueFactory vf) {

        return (V) vf.createLiteral(iv.getDelegate().stringValue(),
                xsdStringURI);

    }

}

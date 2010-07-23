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
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.impl.URIImpl;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.BD;

/**
 * This implementation of {@link IExtension} implements inlining for literals
 * that represent time in milliseconds since the epoch.  The milliseconds are
 * encoded as an inline long.
 */
public class EpochExtension implements IExtension {

    /**
     * The datatype URI for the epoch extension.
     */
    public static final URI EPOCH = new URIImpl(BD.NAMESPACE + "Epoch");
    
    private BigdataURI epoch;
    
    public EpochExtension() {
    }
        
    public void resolveDatatype(final IDatatypeURIResolver resolver) {
        
        this.epoch = resolver.resolve(EPOCH);
        
    }
    
    public BigdataURI getDatatype() {
        
        return epoch;
        
    }
    
    /**
     * Attempts to convert the supplied value into an epoch representation.
     * Tests for a literal value with the correct datatype that can be converted 
     * to a positive long integer.  Encodes the long in a delegate 
     * {@link XSDLongIV}, and returns an {@link ExtensionIV} to wrap the native
     * type.
     */
    public ExtensionIV createIV(final Value value) {
        
        if (value instanceof Literal == false)
            throw new IllegalArgumentException();
        
        final Literal lit = (Literal) value;
        
        final URI dt = lit.getDatatype();
        
        if (dt == null || !EPOCH.stringValue().equals(dt.stringValue()))
            throw new IllegalArgumentException();
        
        final String s = value.stringValue();
        
        final long l = XMLDatatypeUtil.parseLong(s);
        
        // can't have negative epoch values
        if (l < 0)
            return null;
        
        final AbstractLiteralIV delegate = new XSDLongIV(l);

        return new ExtensionIV(delegate, (TermId) getDatatype().getIV());
        
    }
    
    /**
     * Use the string value of the {@link ExtensionIV} (which defers to the
     * string value of the native type) to create a literal with the epoch
     * datatype. 
     */
    public Value asValue(final ExtensionIV iv, final BigdataValueFactory vf) {
        
        return vf.createLiteral(iv.stringValue(), epoch);
        
    }
    
}

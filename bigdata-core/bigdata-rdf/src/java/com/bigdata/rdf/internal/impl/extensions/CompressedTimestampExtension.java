/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
/*
 * Created on Oct 29, 2015
 */

package com.bigdata.rdf.internal.impl.extensions;

import java.util.Collections;
import java.util.Set;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.internal.IDatatypeURIResolver;
import com.bigdata.rdf.internal.IExtension;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.PackedLongIV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * Effective, packed representation of timestamps typed as
 * <code><http://www.bigdata.com/rdf/datatype#compressedTimestamp></code>.
 */
@SuppressWarnings("rawtypes")
public class CompressedTimestampExtension<V extends BigdataValue> implements IExtension<V> {

    private final BigdataURI datatype;
    
    public static final URI COMPRESSED_TIMESTAMP = 
        new URIImpl("http://www.bigdata.com/rdf/datatype#compressedTimestamp");

    
    public CompressedTimestampExtension(final IDatatypeURIResolver resolver) {
        datatype = resolver.resolve(COMPRESSED_TIMESTAMP);
    }
    
    public Set<BigdataURI> getDatatypes() {
        
        return Collections.singleton(datatype);
        
    }
    
    /**
     * Convert the supplied value into an internal representation as PackedLongIV.
     */
    @SuppressWarnings("unchecked")
    public LiteralExtensionIV createIV(final Value value) {
        
        if (value instanceof Literal == false)
            throw new IllegalArgumentException();
        
        final Literal lit = (Literal) value;
        
        final AbstractLiteralIV delegate = new PackedLongIV(Long.parseLong(lit.getLabel()));
        return new LiteralExtensionIV(delegate, datatype.getIV());

    }
    
    
    @SuppressWarnings("unchecked")
    public V asValue(final LiteralExtensionIV iv, final BigdataValueFactory vf) {
        
        AbstractLiteralIV delegate = iv.getDelegate();
        if (delegate==null || !(delegate instanceof PackedLongIV)) {
            throw new IllegalArgumentException();
        }
        
        final PackedLongIV pIv = (PackedLongIV)delegate;
        return (V) vf.createLiteral(
            String.valueOf(pIv.getInlineValue()), CompressedTimestampExtension.COMPRESSED_TIMESTAMP);

    }

}

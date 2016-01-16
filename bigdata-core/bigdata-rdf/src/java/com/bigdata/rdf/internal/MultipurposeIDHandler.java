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
package com.bigdata.rdf.internal;

import java.util.UUID;

import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.FullyInlineTypedLiteralIV;
import com.bigdata.rdf.internal.impl.literal.IPv4AddrIV;
import com.bigdata.rdf.internal.impl.literal.UUIDLiteralIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * Inline URI handler for IDs of multiple types.
 */
public class MultipurposeIDHandler extends InlineURIHandler {

    /**
     * Maximum length of string text to inline if the localName cannot be parsed
     * into a UUID, Numeric, or IPv4 address.
     */
    private final int maxTextLen;
    
    public MultipurposeIDHandler(final String namespace) {
        this(namespace, Integer.MAX_VALUE);
    }

    /**
     * Supply the maximum length of string text to inline if the localName
     * cannot be parsed into a UUID, Numeric, or IPv4 address.
     */
    public MultipurposeIDHandler(final String namespace, final int maxTextLen) {
        super(namespace);
        this.maxTextLen = maxTextLen;
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected AbstractLiteralIV createInlineIV(final String localName) {
        
        try {
            return new UUIDLiteralIV<>(UUID.fromString(localName));
        } catch (Exception ex) {
            // ok, not a uuid
        }
        
        try {
            return new XSDNumericIV(Byte.parseByte(localName));
        } catch (Exception ex) {
            // ok, not a byte
        }
        
        try {
            return new XSDNumericIV(Short.parseShort(localName));
        } catch (Exception ex) {
            // ok, not a short
        }
        
        try {
            return new XSDNumericIV(Integer.parseInt(localName));
        } catch (Exception ex) {
            // ok, not a int
        }
        
        try {
            return new XSDNumericIV(Long.parseLong(localName));
        } catch (Exception ex) {
            // ok, not a long
        }
        
        try {
            return new XSDNumericIV(Float.parseFloat(localName));
        } catch (Exception ex) {
            // ok, not a float
        }

        try {
            return new XSDNumericIV(Double.parseDouble(localName));
        } catch (Exception ex) {
            // ok, not a double
        }

        try {
            return new IPv4AddrIV(localName);
        } catch (Exception ex) {
            // ok, not an ip address
        }
        
        if (localName.length() < maxTextLen) {
            // just use a UTF encoded string, this is expensive
            return new FullyInlineTypedLiteralIV<BigdataLiteral>(localName);
        }
        
        return null;
        
    }

}

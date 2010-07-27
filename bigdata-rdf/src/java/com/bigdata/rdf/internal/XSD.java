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

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

public interface XSD {
    
    String NAMESPACE = "http://www.w3.org/2001/XMLSchema#";
    
    URI BOOLEAN = new URIImpl(NAMESPACE
            + "boolean");

    URI BYTE = new URIImpl(NAMESPACE
            + "byte");

    URI SHORT = new URIImpl(NAMESPACE
            + "short");

    URI INT = new URIImpl(NAMESPACE
            + "int");

    URI LONG = new URIImpl(NAMESPACE
            + "long");

    URI UNSIGNED_BYTE = new URIImpl(NAMESPACE
            + "unsignedByte");

    URI UNSIGNED_SHORT = new URIImpl(NAMESPACE
            + "unsignedShort");

    URI UNSIGNED_INT = new URIImpl(NAMESPACE
            + "unsignedInt");

    URI UNSIGNED_LONG = new URIImpl(NAMESPACE
            + "unsignedLong");
    
    URI FLOAT = new URIImpl(NAMESPACE
            + "float");

    URI DOUBLE = new URIImpl(NAMESPACE
            + "double");

    URI INTEGER = new URIImpl(NAMESPACE
            + "integer");

    URI DECIMAL = new URIImpl(NAMESPACE
            + "decimal");

    URI UUID = new URIImpl(NAMESPACE
            + "uuid");

    
}

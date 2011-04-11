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
import org.openrdf.model.vocabulary.XMLSchema;

/**
 * Collects various XSD URIs as constants.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 * 
 * @see XMLSchema
 */
public class XSD extends XMLSchema {
    
//    String NAMESPACE = "http://www.w3.org/2001/XMLSchema#";
    
//    URI BOOLEAN = XMLSchema.BOOLEAN;
//
//    URI BYTE = XMLSchema.BYTE;
//
//    URI SHORT = XMLSchema.SHORT;
//
//    URI INT = XMLSchema.INT;
//
//    URI LONG = XMLSchema.LONG;
//
//    URI UNSIGNED_BYTE = XMLSchema.UNSIGNED_BYTE;
//
//    URI UNSIGNED_SHORT = XMLSchema.UNSIGNED_SHORT;
//
//    URI UNSIGNED_INT = XMLSchema.UNSIGNED_INT;
//
//    URI UNSIGNED_LONG = XMLSchema.UNSIGNED_LONG;
//    
//    URI FLOAT = XMLSchema.FLOAT;
//
//    URI DOUBLE = XMLSchema.DOUBLE;
//
//    URI INTEGER = XMLSchema.INTEGER;
//
//    URI DECIMAL = XMLSchema.DECIMAL;

	/**
	 * @todo This really should not be in the XSD namespace since it is not part
	 *       of the XML Schema Datatypes specification.
	 */
	static public final URI UUID = new URIImpl(NAMESPACE + "uuid");

//	URI DATETIME = XMLSchema.DATETIME;
    
}

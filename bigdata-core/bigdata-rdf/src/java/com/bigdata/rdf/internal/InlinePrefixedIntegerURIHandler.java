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

import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * 
 * Utility IV to generate IVs for URIs in the form of http://example./org/value/STRPREFIX1234234513
 * where the localName of the URI is a string  prefix followed by an integer  value.
 * 
 * You should extend this class with implementation for specific instances of URIs that follow
 * this form such as:  http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID_1234234 would be
 * create as
 * 
 * InlinePrefixedIntegerURIHandler handler = new InlinePrefixedIntegerURIHandler("http://rdf.ncbi.nlm.nih.gov/pubchem/compound/","CID_");
 * 
 * 
 */

public class InlinePrefixedIntegerURIHandler extends
		InlineSignedIntegerURIHandler {

	private String prefix = null;

	public InlinePrefixedIntegerURIHandler(String namespace, String prefix) {
		super(namespace);
		this.prefix = prefix;
	}

	@Override
	@SuppressWarnings("rawtypes")
	protected AbstractLiteralIV createInlineIV(String localName) {
		if (!localName.startsWith(this.prefix)) {
			return null;
		}
	
		final String intValue = localName.substring(this.prefix.length(), localName.length());
				
		return super.createInlineIV(intValue);
	}

	@Override
	public String getLocalNameFromDelegate(
			AbstractLiteralIV<BigdataLiteral, ?> delegate) {
		return this.prefix + super.getLocalNameFromDelegate(delegate);
	}
}

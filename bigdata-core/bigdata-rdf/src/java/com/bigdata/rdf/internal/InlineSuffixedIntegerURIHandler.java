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
 * Utility IV to generate IVs for URIs in the form of
 * http://example.org/value/STRPREFIX1234234513 where the localName of the URI
 * is a string prefix followed by an integer value.
 * 
 * You should extend this class with implementation for specific instances of
 * URIs that follow this form such as:
 * http://rdf.ncbi.nlm.nih.gov/pubchem/compound/1234234_CID would be created as
 * 
 * <code>
 * InlineSuffixedIntegerURIHandler handler = new InlineSuffixedIntegerURIHandler( "http://rdf.ncbi.nlm.nih.gov/pubchem/compound/","_CID");
 * </code>
 * 
 * This has support for overloading on a single namespace {@link InlineLocalNameIntegerURIHandler}. 
 * 
 * @author beebs
 */

public class InlineSuffixedIntegerURIHandler extends
		InlineLocalNameIntegerURIHandler implements ISuffixedURIHandler {

	private String suffix = null;

	public InlineSuffixedIntegerURIHandler(final String namespace, final String suffix) {
		super(namespace);
		this.suffix = suffix;
	}

	public InlineSuffixedIntegerURIHandler(final String namespace,
			final String suffix, final int id) {
		super(namespace);
		this.suffix = suffix;
		this.packedId = id;
	}

	@Override
	@SuppressWarnings("rawtypes")
	protected AbstractLiteralIV createInlineIV(final String localName) {
		if (!localName.endsWith(this.suffix)) {
			return null;
		}
		
		final String intVal = getPackedValueString(localName.substring(0, localName.length()
				- this.suffix.length()));
		
		return super.createInlineIV(intVal);
	}

	@Override
	public String getLocalNameFromDelegate(
			final AbstractLiteralIV<BigdataLiteral, ?> delegate) {

		return getUnpackedValueFromString(super
				.getLocalNameFromDelegate(delegate)) + this.suffix;
	}

	public String getSuffix() {
		return suffix;
	}
}

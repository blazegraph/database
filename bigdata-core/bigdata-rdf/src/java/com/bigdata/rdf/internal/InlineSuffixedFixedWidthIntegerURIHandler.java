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
 * Utility IV to generate IVs for URIs in the form of http://example.org/value/1234234513STRSUFFIX
 * where the localName of the URI is a string  suffix followed by an integer value with fixed width.
 * 
 * You should extend this class with implementation for specific instances of URIs that follow
 * this form such as:  http://rdf.ncbi.nlm.nih.gov/pubchem/compound/1234234_CID would be
 * created as:
 * 
 * <code>
 * InlineSuffixedFixedWidthIntegerURIHandler handler = new InlineSuffixedFixedWidthIntegerURIHandler("http://rdf.ncbi.nlm.nih.gov/pubchem/compound/","_CID", 7);
 * </code>
 * 
 * This has support for overloading on a single namespace {@link InlineLocalNameIntegerURIHandler}. 
 * 
 * @author beebs
 * 
 */

public class InlineSuffixedFixedWidthIntegerURIHandler extends
		InlineLocalNameIntegerURIHandler implements ISuffixedURIHandler {

	private String suffix = null;
	private int width = 0;

	public InlineSuffixedFixedWidthIntegerURIHandler(final String namespace,
			final String suffix, final int width) {
		super(namespace);
		this.suffix = suffix;
		this.width = width;
	}

	public InlineSuffixedFixedWidthIntegerURIHandler(final String namespace,
			final String suffix, final int width, final int id) {
		super(namespace);
		this.suffix = suffix;
		this.width = width;
		this.packedId = id;
	}

	@Override
	@SuppressWarnings("rawtypes")
	protected AbstractLiteralIV createInlineIV(String localName) {
		if (!localName.endsWith(this.suffix)) {
			return null;
		}
	
		final String intValue =localName.substring(0, localName.length() - this.suffix.length());
		
		return super.createInlineIV(getPackedValueString(intValue));
	}

	@Override
	public String getLocalNameFromDelegate(
			AbstractLiteralIV<BigdataLiteral, ?> delegate) {

		final String intStr = super.getLocalNameFromDelegate(delegate);

		final int intVal = (int) getUnpackedValueFromString(intStr);

		final String localName = String.format("%0" + width + "d", intVal) + this.suffix;

		return localName;
	}

	public String getSuffix() {
		return suffix;
	}

	public void setSuffix(String suffix) {
		this.suffix = suffix;
	}
}

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
 * Utility IV to generate IVs for URIs in the form of http://example.org/value/STRPREFIX1234234513STRSUFFIX
 * where the localName of the URI is a string  prefix followed by an integer  value followed by a string suffix.
 * 
 * You should extend this class with implementation for specific instances of URIs that follow
 * this form such as:  http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID_000123_SUFFIX would be
 * created as
 *
 * <code>
 * InlinePrefixedSuffixedIntegerURIHandler handler = new InlinePrefixedSuffixedIntegerURIHandler("http://rdf.ncbi.nlm.nih.gov/pubchem/compound/","CID_","_SUFFIX");
 * </code>
 * 
 * This has support for overloading on a single namespace {@link InlineLocalNameIntegerURIHandler}. 
 * 
 * @author beebs
 * 
 */

public class InlinePrefixedSuffixedIntegerURIHandler extends
		InlineLocalNameIntegerURIHandler implements IPrefixedURIHandler, ISuffixedURIHandler {

	private String prefix = null;
	private String suffix = null;

	public InlinePrefixedSuffixedIntegerURIHandler(final String namespace,
			final String prefix, final String suffix) {
		super(namespace);
		this.prefix = prefix;
		this.suffix = suffix;
	}

	public InlinePrefixedSuffixedIntegerURIHandler(final String namespace,
			final String prefix, final String suffix, final int id) {
		super(namespace);
		this.prefix = prefix;
		this.suffix = suffix;
		this.packedId = id;
	}

	@Override
	@SuppressWarnings("rawtypes")
	protected AbstractLiteralIV createInlineIV(String localName) {
		if (!localName.startsWith(this.prefix) || !localName.endsWith(suffix)) {
			return null;
		}

		final String intValue = localName.substring(this.prefix.length(),
				localName.length() - this.suffix.length());
				
		return super.createInlineIV(getPackedValueString(intValue));
	}

	@Override
	public String getLocalNameFromDelegate(
			AbstractLiteralIV<BigdataLiteral, ?> delegate) {
		return this.prefix
				+ getUnpackedValueFromString(super
						.getLocalNameFromDelegate(delegate)) + suffix;
	}

	public String getPrefix() {
		return prefix;
	}

	public String getSuffix() {
		return suffix;
	}
}

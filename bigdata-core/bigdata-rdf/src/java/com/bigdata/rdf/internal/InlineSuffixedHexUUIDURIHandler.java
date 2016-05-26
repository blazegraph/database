package com.bigdata.rdf.internal;

import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * 
 * Inline URI Handler to handle URI's in the form of a Hex UUID with a suffix such as:
 * 
 *  <pre>
 *   http://blazegraph.com/element/010072F0000038090100000000D56C9E_SUFFIX
 *  </pre>
 *  
 *  {@link https://jira.blazegraph.com/browse/BLZG-1937}
 * 
 * @author beebs
 *
 */
public class InlineSuffixedHexUUIDURIHandler extends InlineHexUUIDURIHandler {

	private String suffix;

	public InlineSuffixedHexUUIDURIHandler(final String namespace, final String suffix) {
		super(namespace);

		this.suffix = suffix;
	}
	
	@SuppressWarnings("rawtypes")
    protected AbstractLiteralIV createInlineIV(final String localName) {
		if(localName.endsWith(suffix)) {
			return super.createInlineIV(localName.substring(0, localName.length() - this.suffix.length()));
		}
		
		return null; //fall through
	}

	@Override
	public String getLocalNameFromDelegate(
			AbstractLiteralIV<BigdataLiteral, ?> delegate) {

		final String localName = super.getLocalNameFromDelegate(delegate)
				+ suffix;
		return localName;
	}

}

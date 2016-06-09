package com.bigdata.rdf.internal;

import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * 
 * Inline URI Handler to handle URI's in the form of a UUID with a suffix such as:
 * 
 *  <pre>
 *   http://blazegraph.com/element/1ae004cf-0f48-469f-8a94-01339afaec41_SUFFIX
 *  </pre>
 *  
 *  {@link https://jira.blazegraph.com/browse/BLZG-1937}
 * 
 * @author beebs
 *
 */
public class InlineSuffixedUUIDURIHandler extends InlineUUIDURIHandler {

	private String suffix;

	public InlineSuffixedUUIDURIHandler(final String namespace, final String suffix) {
		super(namespace);

		this.suffix = suffix;
	}
	
	@SuppressWarnings("rawtypes")
    protected AbstractLiteralIV createInlineIV(final String localName) {

		if(localName.endsWith(suffix)) {
			final String l2 = localName.substring(0, localName.length() - this.suffix.length());
			return super.createInlineIV(l2);
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

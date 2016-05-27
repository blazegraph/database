package com.bigdata.rdf.internal;

import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * 
 * Inline URI Handler to handle URI's in the form of a UUID with a prefix and a suffix such as:
 * 
 *  <pre>
 *   http://blazegraph.com/element/prefix_1ae004cf-0f48-469f-8a94-01339afaec41_SUFFIX
 *  </pre>
 *  
 *  {@link https://jira.blazegraph.com/browse/BLZG-1937}
 * 
 * @author beebs
 *
 */
public class InlinePrefixedSuffixedUUIDURIHandler extends InlineUUIDURIHandler {

	private String prefix;
	private String suffix;

	public InlinePrefixedSuffixedUUIDURIHandler(final String namespace, final String prefix, final String suffix) {
		super(namespace);
		this.prefix = prefix;
		this.suffix = suffix;
	}
	
	@SuppressWarnings("rawtypes")
    protected AbstractLiteralIV createInlineIV(final String localName) {

		if(localName.startsWith(prefix) && localName.endsWith(suffix)) {
			final String l2 = localName.substring(this.prefix.length(), localName.length() - this.suffix.length());
			return super.createInlineIV(l2);
		}
		
		return null; //fall through
	}

	@Override
	public String getLocalNameFromDelegate(
			AbstractLiteralIV<BigdataLiteral, ?> delegate) {

		final String localName = prefix + super.getLocalNameFromDelegate(delegate)
				+ suffix;
		return localName;
	}

}

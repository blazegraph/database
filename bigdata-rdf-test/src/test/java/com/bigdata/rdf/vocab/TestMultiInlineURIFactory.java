package com.bigdata.rdf.vocab;

import com.bigdata.rdf.internal.InlineHexUUIDURIHandler;
import com.bigdata.rdf.internal.InlineSuffixedHexUUIDURIHandler;
import com.bigdata.rdf.internal.InlineURIFactory;

public class TestMultiInlineURIFactory extends InlineURIFactory {

	public TestMultiInlineURIFactory() {
		super();

		/*
		 * Examples of how to configure Hex-encoded UUID based URIs for
		 * inlining. You may also do this with integers with prefixes,
		 * suffixes, or a combination.
		 * 
		 * Each namespace inlined must have a corresponding vocabulary
		 * declaration.
		 */

		// http://blazegraph.com/Data#Position_010072F0000038090100000000D56C9E
		// http://blazegraph.com/Data#Position_010072F0000038090100000000D56C9E_TaxCost
		// http://blazegraph.com/Data#Position_010072F0000038090100000000D56C9E_UnrealizedGain
		// http://blazegraph.com/Data#Position_010072F0000038090100000000D56C9E_WashSale

		this.addHandler(new InlineHexUUIDURIHandler(
				"http://blazegraph.com/Data#Position_"));

		this.addHandler(new InlineSuffixedHexUUIDURIHandler(
				"http://blazegraph.com/Data#Position_", "_TaxCost"));

		this.addHandler(new InlineSuffixedHexUUIDURIHandler(
				"http://blazegraph.com/Data#Position_", "_UnrealizedGain"));

		this.addHandler(new InlineSuffixedHexUUIDURIHandler(
				"http://blazegraph.com/Data#Position_", "_WashSale"));
	}


}

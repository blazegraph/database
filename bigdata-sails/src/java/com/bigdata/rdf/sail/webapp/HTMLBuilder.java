package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.io.Writer;

/**
 * Variant of {@link XMLBuilder} for HTML output.
 * 
 * @author Martyn Cutcher
 */
public class HTMLBuilder extends XMLBuilder {

	public HTMLBuilder(final Writer w) throws IOException {

		super(false/* isXML */, null/* encoding */, w);

	}

	public HTMLBuilder(final String encoding, final Writer w)
			throws IOException {

		super(false/* isXML */, encoding, w);

	}
	
	public Node body() throws IOException {

		return root("html").node("body");
		
    }

}
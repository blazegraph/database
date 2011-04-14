package com.bigdata.rdf.sail.webapp;

import java.io.IOException;

/**
 * Variant of {@link XMLBuilder} for HTML output.
 * 
 * @author Martyn Cutcher
 */
public class HTMLBuilder extends XMLBuilder {

    public HTMLBuilder() throws IOException {
        super(false);
    }

    public Node body() throws IOException {
        return root("html").node("body");
    }

}
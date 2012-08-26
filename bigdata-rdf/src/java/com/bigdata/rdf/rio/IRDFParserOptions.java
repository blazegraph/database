package com.bigdata.rdf.rio;

import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.helpers.RDFParserBase;

/**
 * Instances of this interface may be used to configure options on an
 * {@link RDFParser}. The options all have the defaults specified by
 * {@link RDFParserBase}.
 */
public interface IRDFParserOptions {

    /**
     * Return <code>true</code> if the parser should verify the data.
     */
    boolean getVerifyData();

    /**
     * Return <code>true</code> if the parser should stop at the first error and
     * <code>false</code> if it should continue processing.
     */
    boolean getStopAtFirstError();

    /**
     * Return <code>true</code> if the parser should preserve blank node IDs.
     */
    boolean getPreserveBNodeIDs();

    /**
     * Return the {@link DatatypeHandling} mode for the parser.
     */
    DatatypeHandling getDatatypeHandling();

    /**
     * Sets the datatype handling mode (default is
     * {@link DatatypeHandling#VERIFY}).
     */
    void setDatatypeHandling(final DatatypeHandling datatypeHandling);

    /**
     * Set whether the parser should preserve bnode identifiers specified in the
     * source (default is <code>false</code>).
     */
    void setPreserveBNodeIDs(final boolean preserveBNodeIDs);

    /**
     * Sets whether the parser should stop immediately if it finds an error in
     * the data (default value is <code>true</code>).
     */
    void setStopAtFirstError(final boolean stopAtFirstError);

    /**
     * Sets whether the parser should verify the data it parses (default value
     * is <code>true</code>).
     */
    void setVerifyData(final boolean verifyData);

}
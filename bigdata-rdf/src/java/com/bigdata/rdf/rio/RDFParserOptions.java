/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
/*
 * Created on Jun 18, 2010
 */

package com.bigdata.rdf.rio;

import java.io.Serializable;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.helpers.RDFParserBase;

/**
 * Instances of this class may be used to configure options on an
 * {@link RDFParser}. The options all have the defaults specified by
 * {@link RDFParserBase}.
 * <p>
 * Note: synchronization on the class instance is used to ensure safe
 * publication of the member field values.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RDFParserOptions implements Serializable {

    protected static final Logger log = Logger.getLogger(RDFParserOptions.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    static public interface Options {
        
        /**
         * Optional boolean property may be used to turn on data verification in
         * the RIO parser (default is {@value #DEFAULT_PARSER_VERIFY_DATA}).
         */
        String VERIFY_DATA = RDFParserOptions.class.getName() + ".verifyData";

        String DEFAULT_VERIFY_DATA = "false";

        /**
         * Optional boolean property may be used to set
         * {@link RDFParserBase#setPreserveBNodeIDs(boolean)} (default is
         * {@value #DEFAULT_PRESERVE_BNODE_IDS}).
         */
        public static final String PRESERVE_BNODE_IDS = RDFParserOptions.class
                .getName()
                + ".preserveBNodeIDs";

        public static final String DEFAULT_PRESERVE_BNODE_IDS = "false";

        /**
         * Optional boolean property may be used to set
         * {@link RDFParserBase#setStopAtFirstError(boolean)} (default is
         * {@value #DEFAULT_STOP_AT_FIRST_ERROR}).
         */
        String STOP_AT_FIRST_ERROR = RDFParserOptions.class.getName()
                + ".stopAtFirstError";

        String DEFAULT_STOP_AT_FIRST_ERROR = "true";

        /**
         * Optional boolean property may be used to set
         * {@link RDFParserBase#setDatatypeHandling(DatatypeHandling)} (default
         * is {@value #DEFAULT_DATATYPE_HANDLING})).
         */
        String DATATYPE_HANDLING = RDFParserOptions.class.getName()
                + ".datatypeHandling";

        String DEFAULT_DATATYPE_HANDLING = DatatypeHandling.VERIFY.toString();

    }

    private DatatypeHandling datatypeHandling = DatatypeHandling.VERIFY;

    private boolean preserveBNodeIDs = false;

    private boolean stopAtFirstError = true;

    private boolean verifyData = true;

    synchronized public boolean getVerifyData() {
        return verifyData;
    }
    
    synchronized public boolean getStopAtFirstError() {
        return stopAtFirstError;
    }
    
    synchronized public boolean getPreserveBNodeIDs() {
        return preserveBNodeIDs;
    }

    synchronized public DatatypeHandling getDatatypeHandling() {
        return datatypeHandling;
    }
    
    public RDFParserOptions() {

    }

    public RDFParserOptions(final Properties properties) {

        // verifyData
        setVerifyData(Boolean
                .parseBoolean(properties.getProperty(
                        Options.VERIFY_DATA,
                        Options.DEFAULT_VERIFY_DATA)));

        if (log.isInfoEnabled())
            log.info(Options.VERIFY_DATA + "=" + getVerifyData());

        // stopAtFirstError
        setStopAtFirstError(Boolean.parseBoolean(properties.getProperty(
                Options.STOP_AT_FIRST_ERROR,
                Options.DEFAULT_STOP_AT_FIRST_ERROR)));

        if (log.isInfoEnabled())
            log.info(Options.STOP_AT_FIRST_ERROR + "="
                    + getStopAtFirstError());

        // datatypeHandling
        setDatatypeHandling(DatatypeHandling.valueOf(properties.getProperty(
                Options.DATATYPE_HANDLING,
                Options.DEFAULT_DATATYPE_HANDLING)));

        if (log.isInfoEnabled())
            log.info(Options.DATATYPE_HANDLING + "="
                    + getDatatypeHandling());

        setPreserveBNodeIDs(Boolean
                .parseBoolean(properties.getProperty(
                        Options.PRESERVE_BNODE_IDS,
                        Options.DEFAULT_PRESERVE_BNODE_IDS)));

    }
    
    public RDFParserOptions(//
            final boolean verifyData,
            final boolean preserveBlankNodeIDs, //
            final boolean stopAtFirstError,//
            final DatatypeHandling datatypeHandling//
            ) {

        if (datatypeHandling == null)
            throw new IllegalArgumentException();
        
        this.verifyData = verifyData;
        this.preserveBNodeIDs = preserveBlankNodeIDs;
        this.stopAtFirstError = stopAtFirstError;
        this.datatypeHandling = datatypeHandling;
        
    }
    
    public synchronized String toString() {
        return super.toString() + //
                "{verifyData=" + verifyData + //
                ",preserveBNodeIDS=" + preserveBNodeIDs + //
                ",stopAtFirstError=" + stopAtFirstError + //
                ",datatypeHandling=" + datatypeHandling + //
                "}";
    }

    /**
     * Sets the datatype handling mode (default is
     * {@link DatatypeHandling#VERIFY}).
     */
    synchronized public void setDatatypeHandling(
            final DatatypeHandling datatypeHandling) {
        
        if (datatypeHandling == null)
            throw new IllegalArgumentException();
        
        this.datatypeHandling = datatypeHandling;
        
    }

    /**
     * Set whether the parser should preserve bnode identifiers specified in the
     * source (default is <code>false</code>).
     */
    synchronized public void setPreserveBNodeIDs(final boolean preserveBNodeIDs) {
        this.preserveBNodeIDs = preserveBNodeIDs;
    }

    /**
     * Sets whether the parser should stop immediately if it finds an error in
     * the data (default value is <code>true</code>).
     */
    synchronized public void setStopAtFirstError(final boolean stopAtFirstError) {
        this.stopAtFirstError = stopAtFirstError;
    }

    /**
     * Sets whether the parser should verify the data it parses (default value
     * is <code>true</code>).
     */
    synchronized public void setVerifyData(final boolean verifyData) {
        this.verifyData = verifyData;
    }

    /**
     * Apply the options to the parser.
     * 
     * @param p
     *            The parser.
     */
    synchronized public void apply(final RDFParser p) {
        p.setDatatypeHandling(datatypeHandling);
        p.setPreserveBNodeIDs(preserveBNodeIDs);
        p.setStopAtFirstError(stopAtFirstError);
        p.setVerifyData(verifyData);
    }

}

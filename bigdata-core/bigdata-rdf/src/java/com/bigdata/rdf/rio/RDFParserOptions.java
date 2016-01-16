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
public class RDFParserOptions implements Serializable, IRDFParserOptions {

    private static final Logger log = Logger.getLogger(RDFParserOptions.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    static public interface Options {
        
        /**
         * Optional boolean property may be used to turn on data verification in
         * the RIO parser (default is {@value #DEFAULT_VERIFY_DATA}).
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

// @see http://trac.blazegraph.com/ticket/1055 (Change RDFParser configuration to use BasicaParserSettings)
        String DEFAULT_STOP_AT_FIRST_ERROR = "false";

        /**
         * Optional boolean property may be used to set
         * {@link RDFParserBase#setDatatypeHandling(DatatypeHandling)} (default
         * is {@value #DEFAULT_DATATYPE_HANDLING})).
         */
        String DATATYPE_HANDLING = RDFParserOptions.class.getName()
                + ".datatypeHandling";                

// @see http://trac.blazegraph.com/ticket/1055 (Change RDFParser configuration to use BasicaParserSettings)
        String DEFAULT_DATATYPE_HANDLING = DatatypeHandling.IGNORE.toString();

    }

// @see http://trac.blazegraph.com/ticket/1055 (Change RDFParser configuration to use BasicaParserSettings)
    private DatatypeHandling datatypeHandling = DatatypeHandling.IGNORE;

    private boolean preserveBNodeIDs = Boolean.valueOf(Options.DEFAULT_PRESERVE_BNODE_IDS);

    private boolean stopAtFirstError = Boolean.valueOf(Options.DEFAULT_STOP_AT_FIRST_ERROR);

    private boolean verifyData = Boolean.valueOf(Options.DEFAULT_VERIFY_DATA);

    /* (non-Javadoc)
     * @see com.bigdata.rdf.rio.IRDFParserOptions#getVerifyData()
     */
    @Override
    synchronized public boolean getVerifyData() {
        return verifyData;
    }
    
    /* (non-Javadoc)
     * @see com.bigdata.rdf.rio.IRDFParserOptions#getStopAtFirstError()
     */
    @Override
    synchronized public boolean getStopAtFirstError() {
        return stopAtFirstError;
    }
    
    /* (non-Javadoc)
     * @see com.bigdata.rdf.rio.IRDFParserOptions#getPreserveBNodeIDs()
     */
    @Override
    synchronized public boolean getPreserveBNodeIDs() {
        return preserveBNodeIDs;
    }

    /* (non-Javadoc)
     * @see com.bigdata.rdf.rio.IRDFParserOptions#getDatatypeHandling()
     */
    @Override
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
    
    @Override
    public synchronized String toString() {
        return super.toString() + //
                "{verifyData=" + verifyData + //
                ",preserveBNodeIDs=" + preserveBNodeIDs + //
                ",stopAtFirstError=" + stopAtFirstError + //
                ",datatypeHandling=" + datatypeHandling + //
                "}";
    }

    /* (non-Javadoc)
     * @see com.bigdata.rdf.rio.IRDFParserOptions#setDatatypeHandling(org.openrdf.rio.RDFParser.DatatypeHandling)
     */
    @Override
    synchronized public void setDatatypeHandling(
            final DatatypeHandling datatypeHandling) {
        
        if (datatypeHandling == null)
            throw new IllegalArgumentException();
        
        this.datatypeHandling = datatypeHandling;
        
    }

    /* (non-Javadoc)
     * @see com.bigdata.rdf.rio.IRDFParserOptions#setPreserveBNodeIDs(boolean)
     */
    @Override
    synchronized public void setPreserveBNodeIDs(final boolean preserveBNodeIDs) {
        this.preserveBNodeIDs = preserveBNodeIDs;
    }

    /* (non-Javadoc)
     * @see com.bigdata.rdf.rio.IRDFParserOptions#setStopAtFirstError(boolean)
     */
    @Override
    synchronized public void setStopAtFirstError(final boolean stopAtFirstError) {
        this.stopAtFirstError = stopAtFirstError;
    }

    /* (non-Javadoc)
     * @see com.bigdata.rdf.rio.IRDFParserOptions#setVerifyData(boolean)
     */
    @Override
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

    /**
     * Utility method applies the options to the {@link RDFParser}.
     * 
     * @param opts
     *            The options.
     * @param p
     *            The parser.
     */
    public static void apply(final IRDFParserOptions opts, final RDFParser p) {
        p.setDatatypeHandling(opts.getDatatypeHandling());
        p.setPreserveBNodeIDs(opts.getPreserveBNodeIDs());
        p.setStopAtFirstError(opts.getStopAtFirstError());
        p.setVerifyData(opts.getVerifyData());
    }

    @Override
    public boolean equals(final Object o) {

        if (this == o)
            return true;

        if (!(o instanceof IRDFParserOptions))
            return false;

        final IRDFParserOptions t = (IRDFParserOptions) o;

        if (verifyData != t.getVerifyData())
            return false;

        if (preserveBNodeIDs != t.getPreserveBNodeIDs())
            return false;

        if (stopAtFirstError != t.getStopAtFirstError())
            return false;

        if (!datatypeHandling.equals(getDatatypeHandling()))
            return false;

        return true;

    }
    
}

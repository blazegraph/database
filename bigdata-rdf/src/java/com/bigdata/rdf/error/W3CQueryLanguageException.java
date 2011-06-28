/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Feb 11, 2011
 */

package com.bigdata.rdf.error;

import java.util.Formatter;

/**
 * Exception Base class for errors defined by the W3C for XQuery, XPath, and
 * SPARQL.
 * 
 * @see http://www.w3.org/TR/xquery/#errors
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class W3CQueryLanguageException extends RuntimeException {

    /**
     * Namespace for the error URIs.
     */
    protected static final transient String err = "http://www.w3.org/2005/xqt-errors";
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /** Language family for errors. */
    public static enum LanguageFamily {
        /**
         * XQuery
         */
        XQ,
        /**
         * XPath
         */
        XP,
        /**
         * SPARQL
         */
        SP
    }
    
    /** Error category. */
    public static enum ErrorCategory {
        /** Static analysis error. */
        ST,
        /** Dynamic (runtime) error. */
        DY,
        /** Type error. */
        TY
    };

    /**
     * The {@link LanguageFamily}.
     */
    public final LanguageFamily languageFamily;

    /**
     * The {@link ErrorCategory}.
     */
    public final ErrorCategory errorCategory;

    /**
     * The four digit error code.
     */
    public final int errorCode;

    /**
     * 
     * @param languageFamily
     *            The {@link LanguageFamily}.
     * @param errorCategory
     *            The {@link ErrorCategory}.
     * @param errorCode
     *            The four digit error code.
     * @param msg
     *            The <em>URI</em> corresponding to the error. Frequently used
     *            errors should use
     *            {@link #toURI(LanguageFamily, ErrorCategory, int)} to define
     *            the URI statically to avoid heap churn.
     */
    public W3CQueryLanguageException(LanguageFamily languageFamily,
            ErrorCategory errorCategory, int errorCode, String msg) {
        
        super(msg == null ? toURI(languageFamily, errorCategory, errorCode)
                : msg);
        
        this.languageFamily = languageFamily;
        
        this.errorCategory = errorCategory;
        
        this.errorCode = errorCode;
        
    }

    /**
     * Return the URI for the given error. This is used to avoid the runtime
     * creation of strings for frequently thrown errors, such as type errors.
     * Various subclasses use this method to declare concrete URIs which are
     * then passed into their constructors for specific kinds of errors.
     * 
     * @param languageFamily
     *            The {@link LanguageFamily}.
     * @param errorCategory
     *            The {@link ErrorCategory}.
     * @param errorCode
     *            The four digit error code.
     * 
     * @return The URI.
     */
    static protected String toURI(LanguageFamily languageFamily,
            ErrorCategory errorCategory, int errorCode) {

        if (errorCode >= 10000 || errorCode < 0)
            throw new IllegalArgumentException();

        final StringBuffer sb = new StringBuffer(4);

        final Formatter f = new Formatter(sb);

        f.format("%04d", errorCode);

        return err + languageFamily + errorCategory + sb.toString();

    }

    // public static void main(String[] x) {
    //
    // System.err.println(toURI(LanguageFamily.SP,ErrorCategory.TY,120));
    //        
    // }

}

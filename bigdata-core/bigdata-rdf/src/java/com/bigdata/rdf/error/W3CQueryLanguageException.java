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
 * Created on Feb 11, 2011
 */

package com.bigdata.rdf.error;

import java.util.Formatter;

import com.bigdata.util.NV;

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
     */
    protected W3CQueryLanguageException(final LanguageFamily languageFamily,
            final ErrorCategory errorCategory, final int errorCode) {

        super(toURI(languageFamily, errorCategory, errorCode, null/* params */));
        
        this.languageFamily = languageFamily;
        
        this.errorCategory = errorCategory;
        
        this.errorCode = errorCode;
        
    }

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
    protected W3CQueryLanguageException(final LanguageFamily languageFamily,
            final ErrorCategory errorCategory, final int errorCode,
            final String msg) {

        super(msg == null ? toURI(languageFamily, errorCategory, errorCode,
                null/* params */) : msg);
        
        this.languageFamily = languageFamily;
        
        this.errorCategory = errorCategory;
        
        this.errorCode = errorCode;
        
    }

    /**
     * 
     @param languageFamily
     *            The {@link LanguageFamily}.
     * @param errorCategory
     *            The {@link ErrorCategory}.
     * @param errorCode
     *            The four digit error code.
     * @param params
     *            Additional parameters for the error message (optional and may
     *            be <code>null</code>). When non-<code>null</code> and
     *            non-empty, the parameters are appended to the constructed URI
     *            as query parameters.
     */
    protected W3CQueryLanguageException(final LanguageFamily languageFamily,
            final ErrorCategory errorCategory, final int errorCode,
            final NV[] params) {
        
        super(toURI(languageFamily, errorCategory, errorCode, params));
        
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
     * @param params
     *            Additional parameters for the error message (optional and may
     *            be <code>null</code>). When non-<code>null</code> and
     *            non-empty, the parameters are appended to the constructed URI
     *            as query parameters.
     * 
     * @return The URI.
     */
    static protected String toURI(final LanguageFamily languageFamily,
            final ErrorCategory errorCategory, final int errorCode,
            final NV[] params) {

        if (errorCode >= 10000 || errorCode < 0)
            throw new IllegalArgumentException();

        final String uri;
        {
            
            final StringBuffer sb = new StringBuffer(4);

            final Formatter f = new Formatter(sb);

            f.format("%04d", errorCode);

            uri = err + languageFamily + errorCategory + sb.toString();
            
        }

        if (params == null || params.length == 0) {

            return uri;
            
        }

        // Add query parameters.
        {

            final StringBuilder sb = new StringBuilder();

            sb.append(uri);

            for (int i = 0; i < params.length; i++) {

                final NV nv = params[i];
                
                sb.append(i == 0 ? '?' : '&');

                sb.append(encode(nv.getName()));

                sb.append('=');
                
                sb.append(encode(nv.getValue()));

            }
            
            return sb.toString();
            
        }

    }

    /**
     * Safe UTF-8 encoder (handles the highly unlikely exception that can be
     * thrown as well as a <code>null</code> value).
     * 
     * @param s
     *            The string.
     * 
     * @return The encoded string.
     */
    private static String encode(final String s) {
        
        if (s == null)
            return encode("null");
        
//        try {
//
//            return URLEncoder.encode(s, "UTF-8");
//            
//        } catch (UnsupportedEncodingException ex) {
//            
//            return s;
//            
//        }
        /*
         * I prefer the plain text of the string as this is really a QName and
         * not a navigable URI.
         */
        return s;

    }
    
    // public static void main(String[] x) {
    //
    // System.err.println(toURI(LanguageFamily.SP,ErrorCategory.TY,120));
    //        
    // }

}

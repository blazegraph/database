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

import org.openrdf.model.URI;

import com.bigdata.util.NV;

/**
 * A SPARQL dynamic (runtime) error.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SparqlDynamicErrorException extends W3CQueryLanguageException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Generic type error.
     */
    static public String SPARQL_DYNAMIC_ERROR_0000 = toURI(0);

    /**
     * Error thrown when a graph exists in a context in which it is presumed
     * that it should not pre-exist.
     * 
     * @see GraphExistsException
     */
    static public String SPARQL_DYNAMIC_ERROR_0001 = toURI(1);

    /**
     * Error thrown when a graph is empty in a context in which it is presumed
     * that it should not be empty.
     * 
     * @see GraphEmptyException
     */
    static public String SPARQL_DYNAMIC_ERROR_0002 = toURI(2);

    /**
     * Error thrown when a named solution set exists in a context in which it is
     * presumed that it should not pre-exist.
     * 
     * @see SolutionSetExistsException
     */
    static public String SPARQL_DYNAMIC_ERROR_0003 = toURI(3);

    /**
     * Error thrown when a named solution set does not exist in a context in
     * which it is presumed that it should exist.
     * 
     * @see SolutionSetDoesNotExistException
     */
    static public String SPARQL_DYNAMIC_ERROR_0004 = toURI(4);

    /**
     * Error thrown when the Content-Type is not recognized or can not be
     * handled.
     * 
     * @see UnknownContentTypeException
     */
    static public String SPARQL_DYNAMIC_ERROR_0005 = toURI(5);

    /**
     * Generic SPARQL dynamic error.
     * 
     * @see #SPARQL_DYNAMIC_ERROR_0000
     */
    public SparqlDynamicErrorException() {

        this(0/* errorCode */, SPARQL_DYNAMIC_ERROR_0000);

    }

    /**
     * @param errorCode
     *            The four digit error code.
     * @param uri
     *            The uri of the error.
     */
    protected SparqlDynamicErrorException(final int errorCode, final String uri) {

        super(LanguageFamily.SP, ErrorCategory.DY, errorCode, uri);

    }
    
    /**
     * @param errorCode
     *            The four digit error code.
     * @param params
     *            Addition parameters for the error (optional).
     */
    protected SparqlDynamicErrorException(final int errorCode, final NV[] params) {

        super(LanguageFamily.SP, ErrorCategory.DY, errorCode, params);

    }

    /**
     * @param errorCode
     *            The four digit error code.
     * @param uri
     *            The uri of the error.
     * @param params
     *            Additional parameters for the error message (optional and may
     *            be <code>null</code>). When non-<code>null</code> and
     *            non-empty, the parameters are appended to the constructed URI
     *            as query parameters.
     */
    protected SparqlDynamicErrorException(final int errorCode,
            final String uri, final NV[] params) {

        super(LanguageFamily.SP, ErrorCategory.DY, errorCode, params);

    }

    static protected String toURI(final int errorCode) {

        return W3CQueryLanguageException.toURI(LanguageFamily.SP,
                ErrorCategory.DY, errorCode, null/* params */);

    }

    /**
     * Error thrown when a graph exists in a context which presumes that it
     * should not pre-exist.
     */
    static public class GraphExistsException extends SparqlDynamicErrorException {

        private static final long serialVersionUID = 1L;

        public GraphExistsException(final URI graphUri) {

            super(0001/* errorCode */, SPARQL_DYNAMIC_ERROR_0001,
                    new NV[] { new NV("uri", graphUri.stringValue()) });

        }

    }

    /**
     * Error thrown when a graph is empty in a context which presumes that it
     * should not be empty.
     */
    static public class GraphEmptyException extends SparqlDynamicErrorException {

        private static final long serialVersionUID = 1L;

        public GraphEmptyException(final URI graphUri) {

            super(0002/* errorCode */, SPARQL_DYNAMIC_ERROR_0002,
                    new NV[] { new NV("uri", graphUri.stringValue()) });

        }

    }

    /**
     * Error thrown when a named solution set exists in a context which presumes
     * that it should not pre-exist.
     */
    static public class SolutionSetExistsException extends SparqlDynamicErrorException {

        private static final long serialVersionUID = 1L;

        public SolutionSetExistsException(final String solutionSet) {

            super(0003/* errorCode */, SPARQL_DYNAMIC_ERROR_0003,
                    new NV[] { new NV("name", solutionSet) });

        }

    }

    /**
     * Error thrown when an named solution set does not exist in a context which
     * presumes that it should exist.
     */
    static public class SolutionSetDoesNotExistException extends SparqlDynamicErrorException {

        private static final long serialVersionUID = 1L;

        public SolutionSetDoesNotExistException(final String solutionSet) {

            super(0004/* errorCode */, SPARQL_DYNAMIC_ERROR_0004,
                    new NV[] { new NV("name", solutionSet) });

        }

    }

    /**
     * Error thrown when the Content-Type is not recognized or can not be
     * handled.
     */
    static public class UnknownContentTypeException extends SparqlDynamicErrorException {

        private static final long serialVersionUID = 1L;

        public UnknownContentTypeException(final String contentType) {

            super(0005/* errorCode */, SPARQL_DYNAMIC_ERROR_0005,
                    new NV[] { new NV("Content-Type", contentType) });

        }

    }

}

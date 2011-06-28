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

/**
 * A SPARQL type error.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SparqlTypeErrorException extends W3CQueryLanguageException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Generic type error.
     */
    static public String SPARQL_TYPE_ERROR_0000 = toURI(0);

    /**
     * Type error used to indicate an unbound variable.
     */
    static public String SPARQL_TYPE_ERROR_0001 = toURI(1);

    /**
     * Generic SPARQL type error.
     * 
     * @see #SPARQL_TYPE_ERROR_0000
     */
    public SparqlTypeErrorException() {

		this(0/* errorCode */, SPARQL_TYPE_ERROR_0000);

    }

	/**
	 * Type error thrown when there is an unbound variable.
	 */
	static public class UnboundVarException extends SparqlTypeErrorException {

		private static final long serialVersionUID = 1L;

		public UnboundVarException() {

			super(0001/* errorCode */, SPARQL_TYPE_ERROR_0001);

		}

	}

	/**
	 * @param errorCode
	 *            The four digit error code.
	 * @param uri
	 *            The uri of the error.
	 */
	protected SparqlTypeErrorException(final int errorCode, final String uri) {

        super(LanguageFamily.SP, ErrorCategory.TY, errorCode, null/* msg */);

    }

    static protected String toURI(final int errorCode) {

        return W3CQueryLanguageException.toURI(LanguageFamily.SP,
                ErrorCategory.TY, errorCode);

    }

}

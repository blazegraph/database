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
 * Created on Dec 9, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

/**
 * An instance of this exception is thrown when a graph context is not well
 * formed. For example, the following is illegal:
 * 
 * <pre>
 * GRAPH uri1 { GRAPH uri2 ... }
 * </pre>
 * 
 * where uri1 != uri2.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class InvalidGraphContextException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * 
     */
    public InvalidGraphContextException() {
    }

    /**
     * @param message
     */
    public InvalidGraphContextException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public InvalidGraphContextException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public InvalidGraphContextException(String message, Throwable cause) {
        super(message, cause);
    }

}

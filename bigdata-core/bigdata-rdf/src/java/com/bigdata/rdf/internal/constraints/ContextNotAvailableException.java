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
 * Created on Mar 16, 2012
 */

package com.bigdata.rdf.internal.constraints;

/**
 * This generally indicates a failure to propagate the context wrapper for the
 * binding set to a new binding set during a copy (projection), bind (join),
 * etc. It could also indicate a failure to wrap binding sets when they are
 * vectored into an operator after being received at a node on a cluster.
 * 
 * TODO This exception can also be thrown during query optimization since the
 * necessary context is not available at that point. That should be fixed, but
 * it is a static method invocation so we would have to touch a lot of code.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ContextNotAvailableException extends UnsupportedOperationException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * 
     */
    public ContextNotAvailableException() {
    }

    /**
     * @param message
     */
    public ContextNotAvailableException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public ContextNotAvailableException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public ContextNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }

}

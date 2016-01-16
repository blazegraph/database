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
 * Created on Nov 16, 2011
 */

package com.bigdata.bop.join;

/**
 * An exception thrown when a hash join does not have any join variables and is
 * considering the cross product of two many solutions as a result.
 * <p>
 * This exception normally indicates a problem with the query plan since a hash
 * join without any join variables will consider the full M x N cross product of
 * the solutions. If that cross product is too large, then the join can become
 * CPU bound and the query will make only very slow progress.
 *
 * @see HashJoinAnnotations#NO_JOIN_VARS_LIMIT
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class UnconstrainedJoinException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * 
     */
    public UnconstrainedJoinException() {
    }

    /**
     * @param message
     */
    public UnconstrainedJoinException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public UnconstrainedJoinException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public UnconstrainedJoinException(String message, Throwable cause) {
        super(message, cause);
    }

}

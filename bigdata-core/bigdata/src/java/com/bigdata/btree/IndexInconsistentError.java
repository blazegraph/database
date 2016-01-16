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
 * Created on Sep 2, 2014
 */
package com.bigdata.btree;

/**
 * Error marks an mutable index as in an inconsistent state. The index MUST be
 * reloaded from the current checkpoint record.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/1005"> Invalidate BTree objects
 *      if error occurs during eviction </a>
 */
public class IndexInconsistentError extends Error {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public IndexInconsistentError() {
    }

    public IndexInconsistentError(String message) {
        super(message);
    }

    public IndexInconsistentError(Throwable cause) {
        super(cause);
    }

    public IndexInconsistentError(String message, Throwable cause) {
        super(message, cause);
    }

    public IndexInconsistentError(String message, Throwable cause,
            boolean enableSuppression, boolean writableStackTrace) {

        super(message, cause, enableSuppression, writableStackTrace);
        
    }

}

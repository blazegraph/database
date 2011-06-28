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
 * Created on Jun 2, 2010
 */

package com.bigdata.quorum;


/**
 * An instance of this class is thrown when the caller is awaiting a quorum
 * event and the quorum is asynchronously closed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AsynchronousQuorumCloseException extends QuorumException {

    /**
     * 
     */
    private static final long serialVersionUID = 829579638730180109L;

    /**
     * 
     */
    public AsynchronousQuorumCloseException() {
    }

    /**
     * @param message
     */
    public AsynchronousQuorumCloseException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public AsynchronousQuorumCloseException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public AsynchronousQuorumCloseException(String message, Throwable cause) {
        super(message, cause);
    }

}

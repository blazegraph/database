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
package com.bigdata.ha.pipeline;

import java.io.IOException;


/**
 * An exception thrown by the {@link HAReceiveService} when replication to the
 * downstream service fails. The root cause can be an RMI error (can not connect
 * or connection lost), a socket channel write error (can not connect,
 * connection lost, etc.), or even a transitive error from further down the
 * write pipeline. This exception DOES NOT decisively indicate the problem is
 * with the immediate downstream service. The caller must inspect the root cause
 * to make this determination. However, this exception DOES indicate that the
 * problem is with downstream replication rather than with the receipt or
 * handling of the payload on this service.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class ImmediateDownstreamReplicationException extends IOException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public ImmediateDownstreamReplicationException() {
    }

    public ImmediateDownstreamReplicationException(String message) {
        super(message);
    }

    public ImmediateDownstreamReplicationException(Throwable cause) {
        super(cause);
    }

    public ImmediateDownstreamReplicationException(String message, Throwable cause) {
        super(message, cause);
    }

}

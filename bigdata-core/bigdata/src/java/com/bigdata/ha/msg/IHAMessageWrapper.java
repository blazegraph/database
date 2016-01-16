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
package com.bigdata.ha.msg;

import com.bigdata.ha.pipeline.HAReceiveService;

/**
 * Glue interface wraps the {@link IHALogRequest}, {@link IHASendState}, and
 * {@link IHAWriteMessage} interfaces exposes the requires {@link IHAMessage}
 * interface to the {@link HAReceiveService}. This class is never persisted (it
 * does NOT get written into the HALog files). It just let's us handshake with
 * the {@link HAReceiveService} and get back out the original
 * {@link IHAWriteMessage} as well as the optional {@link IHALogRequest}
 * message.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IHAMessageWrapper {

    /**
     * Return the optional {@link IHASyncRequest}. When available, this provides
     * information about the service request that resulted in the transmission
     * of the payload along the pipeline.
     */
    IHASyncRequest getHASyncRequest();

    /**
     * Return information about the state of the sending service.
     */
    IHASendState getHASendState();

    /**
     * Return the message that describes the payload that will be replicated
     * along the pipeline.
     */
    IHAWriteMessageBase getHAWriteMessage();

}

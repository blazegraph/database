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
 * Glue class wraps the {@link IHAWriteMessage} and the {@link IHALogRequest}
 * message and exposes the requires {@link IHAMessage} interface to the
 * {@link HAReceiveService}. This class is never persisted. It just let's us
 * handshake with the {@link HAReceiveService} and get back out the original
 * {@link IHAWriteMessage} as well as the optional {@link IHALogRequest}
 * message.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HAMessageWrapper extends HAWriteMessageBase implements
        IHAMessageWrapper {

    private static final long serialVersionUID = 1L;

    private final IHASyncRequest req;
    private final IHASendState snd;
    private final IHAWriteMessageBase msg;

    public HAMessageWrapper(final IHASyncRequest req, final IHASendState snd,
            final IHAWriteMessageBase msg) {

        // Use size and checksum from real IHAWriteMessage.
        super(msg.getSize(), msg.getChk());

        this.req = req; // MAY be null;
        this.snd = snd;
        this.msg = msg;

    }

    @Override
    public IHASyncRequest getHASyncRequest() {
        return req;
    }

    @Override
    public IHASendState getHASendState() {
        return snd;
    }

    @Override
    public IHAWriteMessageBase getHAWriteMessage() {
        return msg;
    }

    /**
     * Return the {@link IHASendState#getMarker()} iff there is an associated
     * {@link IHASendState} and otherwise <code>null</code>.
     */
    public byte[] getMarker() {

        return snd == null ? null : snd.getMarker();
        
    }

}

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

import com.bigdata.journal.IRootBlockView;

/**
 * Interface for a response for a request to send the backing file in 
 * support of a disaster rebuild.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IHASendStoreResponse extends IHAMessage {

    /**
     * Root block ZERO (0) for the service whose backing store was sent
     * down the write pipeline in response to the request.  
     * 
     * @return The root block and never <code>null</code>.
     */
    IRootBlockView getRootBlock0();

    /**
     * Root block ONE (1) for the service whose backing store was sent
     * down the write pipeline in response to the request.
     * 
     * @return The root block and never <code>null</code>.
     */
    IRootBlockView getRootBlock1();

    /**
     * The #of bytes that were sent in fulfillment of the request.
     */
    long getByteCount();

    /**
     * The #of write cache blocks that were sent in fulfillment of the request.
     */
    long getBlockCount();

}

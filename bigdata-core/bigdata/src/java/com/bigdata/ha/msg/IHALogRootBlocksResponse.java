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
 * Interface for a response requesting the opening and closing root blocks for
 * an HA Log file.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IHALogRootBlocksResponse extends IHAMessage {

    /**
     * The root block that opens the HA Log file for the specified commit point.
     * 
     * @return The root block and never <code>null</code>.
     */
    IRootBlockView getOpenRootBlock();

    /**
     * The root block that closes the HA Log file for the specified commit
     * point.
     * 
     * @return The root block and never <code>null</code>.
     */
    IRootBlockView getCloseRootBlock();

//    /**
//     * The #of write cache blocks that can be read from that file.
//     */
//    long getWriteBlockCount();
    
}

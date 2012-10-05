/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.ha.msg;

import java.util.concurrent.TimeUnit;

import com.bigdata.ha.HACommitGlue;
import com.bigdata.journal.IRootBlockView;

/**
 * Message used by the {@link HACommitGlue} interface to indicate that the
 * recipient should save a reference to the caller's root block, flush writes to
 * the backing channel and acknowledge "yes" if ready to commit. If the node can
 * not prepare for any reason, then it must return "no".
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IHA2PhasePrepareMessage {

    /**
     * <code>true</code> iff the service was recognized as being joined with the
     * met quorum at the time that the prepare message was prepared.
     * <p>
     * Note: This is used to support atomic decisions about whether or not a
     * service was joined with the met quorum at the time that the leader
     * decided to commit. Services that are in the pipeline and resynchronizing
     * will either be joined or not for the purposes of a given 2-phase commit
     * based on this flag.
     */
    boolean isJoinedService();
    
    /**
     * <code>true</code> if this is rootBlock0 for the leader.
     */
    boolean isRootBlock0();

    /**
     * The new root block.
     */
    IRootBlockView getRootBlock();

    /**
     * How long to wait for the other services to prepare.
     */
    long getTimeout();

    /**
     * The unit for the timeout.
     */
    TimeUnit getUnit();
}

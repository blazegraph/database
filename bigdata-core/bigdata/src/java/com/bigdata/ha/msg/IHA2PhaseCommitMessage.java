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

import com.bigdata.ha.HACommitGlue;

/**
 * Message used for commit in 2-phase commit protocol. The receiver should
 * Commit using the root block from the corresponding prepare message. It is an
 * error if a commit message is observed without the corresponding prepare
 * message.
 * 
 * @see HACommitGlue#commit2Phase(IHACommit2PhaseMessage)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IHA2PhaseCommitMessage extends IHA2PhaseCommitProtocolMessage {

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
     * The commit time that will be assigned to the new commit point.
     */
    long getCommitTime();
    
    /**
     * Return <code>true</code> iff all services voted "YES" for PREPARE. When
     * <code>false</code>, not all services will participate in this commit (but
     * the commit will still be performed).
     */
    boolean didAllServicesPrepare();

    /**
     * When <code>true</code> the COMMIT message will fail within the
     * commit2Phase implementation. An exception will be thrown immeditely
     * before the new root block is written onto the journal.
     * <p>
     * Note: This is for unit tests only.
     */
    boolean failCommit_beforeWritingRootBlockOnJournal();

    /**
     * When <code>true</code> the COMMIT message will fail within the
     * commit2Phase implementation. An exception will be thrown immeditely
     * before the closing root block is written onto the HALog file.
     * <p>
     * Note: This is for unit tests only.
     */
    boolean failCommit_beforeClosingHALog();

}

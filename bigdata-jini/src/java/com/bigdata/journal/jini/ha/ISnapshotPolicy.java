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
package com.bigdata.journal.jini.ha;

/**
 * The policy that specifies when a new snapshot will be taken. The decision to
 * take a snapshot is a local decision. The snapshot is assumed to be written to
 * local disk. Offsite replication of the snapshots and HALog files is STRONGLY
 * recommended.
 * <p>
 * Each snapshot is a full backup of the journal. Incremental backups (HALog
 * files) are created for each transaction. Older snapshots and HALog files will
 * be removed once automatically based on the {@link IRestorePolicy}.
 * <p>
 * A policy may be used that never makes snaphots. This can support both
 * read-only deployments and deployments where snapshots are scheduled by an
 * external process, such as a <code>cron</code> job.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface ISnapshotPolicy {

    /**
     * Initialize the policy.
     */
    void init(HAJournal jnl);
    
}

/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * The restore policy identifies the first commit point whose backups MUST NOT
 * be released. Restore begins the most recent snapshot LTE the desired restore
 * point and applies zero or more HALog files until the desired commit point has
 * been restored. Therefore, the {@link IRestorePolicy} will pin the most recent
 * snapshot LTE the computed earliest possible restore point and any HALog files
 * for commit points GT that snapshot. A snapshot is taken automatically when
 * the service first starts (and joins with a met quorum). This provides the
 * initial full backup. Incremental logs are then retained until the next
 * snapshot, at which point the {@link IRestorePolicy} may allow the older
 * snapshot(s) and HALog files to be aged out.
 * <p>
 * The policy may be based on the age of the commit point, the number of
 * intervening commit points, etc. A policy that always returns ZERO (0) will
 * never release any backups.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IRestorePolicy {

    /**
     * Return the earlist commit point whose supporting backup files MUST NOT be
     * released in order to preseve the ability to restore that commit point
     * from local backup files. The commit point is a commitCounter value. The
     * first valid commit counter is ONE (1). A return value of ZERO (0) is
     * therefore less than any possible commit point and will cause all commit
     * points to be retained. The special value {@link Long#MAX_VALUE} may be
     * used to indicate that NO commit points will be retained (it is greater
     * than any possible commit counter).
     * <p>
     * Note: It is possible that new HALog files and new snapshots may appear
     * concurrently during the evaluation of this method. This is unavoidable
     * since both the leader and followers must evaluate this method in order to
     * decide whether to release existing snapshot and HALog files. However, it
     * is guaranteed that only one thread at a time on a given service will make
     * the decision to release snapshots or HALog files. Since HALogs and
     * snapshots may spring into existence concurrent with that decision, the
     * {@link IRestorePolicy} can make decisions that retain slightly more
     * information than would be required if the policy could have been
     * evaluated atomicically with respect to the existing HALog files and
     * snapshot files. This transient boundary condition can not result in fewer
     * files being retained than are necessary to satisify the policy.
     * 
     * @param jnl
     *            The journal.
     * 
     * @return The earliest commit point whose supporting backup files must not
     *         be released.
     */
    long getEarliestRestorableCommitPoint(HAJournal jnl);
    
}

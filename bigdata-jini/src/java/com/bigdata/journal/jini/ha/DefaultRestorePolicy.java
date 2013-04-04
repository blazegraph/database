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

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.journal.IRootBlockView;

/**
 * The default restore policy. This policy supports three different criteria for
 * deciding when snapshots (aka full backups) and HALogs (aka write ahead log
 * files for each commit point aka incremental backups) may be purged.
 * <dl>
 * <dt>minSnapshotAgeMillis</dt>
 * <dd>The minimum age of a snapshot before it may be deleted.</dd>
 * <dt>minSnapshots</dt>
 * <dd>The minimum number of snapshot files (aka full backups) that must be
 * retained.</dd>
 * <dt>minRestorePoints</dt>
 * <dd>The minimum number of commit points that must be restorable from backup.
 * This explicitly controls the number of HALog files that will be retained. It
 * also implicitly controls the number of snapshot files that will be retained
 * since an HALog file will pin the newest snapshot whose commit counter is LTE
 * to the the closing commit counter on that HALog file.</dd>
 * </dl>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class DefaultRestorePolicy implements IRestorePolicy {

    private static final Logger log = Logger
            .getLogger(DefaultRestorePolicy.class);

    /**
     * The default minimum age of a snapshot before it may be purged (7 days).
     */
    public static final long DEFAULT_MIN_SNAPSHOT_AGE_MILLIS = TimeUnit.DAYS
            .toMillis(7);
    
    /**
     * The default minimum #of snapshots which must be retained (
     * {@value #DEFAULT_MIN_SNAPSHOTS}).
     */
    public static final int DEFAULT_MIN_SNAPSHOTS = 1;
    
    /**
     * The default minimum #of commit points that must be retained (
     * {@value #DEFAULT_MIN_RESTORE_POINTS}).
     */
    public static final int DEFAULT_MIN_RESTORE_POINTS = 0;
    
    /**
     * The minimum age of a snapshot before it may be purged.
     */
    private final long minSnapshotAgeMillis;

    /**
     * The minimum #of snapshots that must be retained.
     */
    private final int minSnapshots;

    /**
     * The minimum #of restore points (HALogs) that must be retained.
     */
    private final int minRestorePoints;
    
    @Override
    public String toString() {

        return DefaultRestorePolicy.class.getSimpleName()//
                + "{minSnapshotAge=" + minSnapshotAgeMillis + "ms"//
                + ",minSnapshots=" + minSnapshots //
                + ",minRestorePoints=" + minRestorePoints //
                + "}";

    }
    
    /**
     * The default is to keep local backups on hand for 7 days.
     */
    public DefaultRestorePolicy() {

        this(DEFAULT_MIN_SNAPSHOT_AGE_MILLIS, DEFAULT_MIN_SNAPSHOTS,
                DEFAULT_MIN_RESTORE_POINTS);

    }

    /**
     * Create a policy that determines when local backups may be purged. The
     * policy will retain local backups unless all of the criteria are
     * satisified.
     * 
     * @param minSnapshotAgeMillis
     *            The minimum age of a snapshot (in milliseconds) before it may
     *            be purged.
     */
    public DefaultRestorePolicy(final long minSnapshotAgeMillis) {

        this(minSnapshotAgeMillis, DEFAULT_MIN_SNAPSHOTS,
                DEFAULT_MIN_RESTORE_POINTS);

    }

    /**
     * Create a policy that determines when local backups may be purged. The
     * policy will retain local backups unless all of the criteria are
     * satisified.
     * 
     * @param minSnapshotAgeMillis
     *            The minimum age of a snapshot (in milliseconds) before it may
     *            be purged.
     * @param minSnapshots
     *            The minimum number of snapshots (aka full backups) that must
     *            be retained locally.
     * @param minRestorePoints
     *            The minimum number of restore points (aka HALog files) that
     *            must be retained locally. If an HALog is pinned by this
     *            parameter, then the oldest snapshot LTE the commit counter of
     *            that HALog is also pinned, as are all HALog files GTE the
     *            snapshot and LT the HALog.
     */
    public DefaultRestorePolicy(final long minSnapshotAgeMillis,
            final int minSnapshots, final int minRestorePoints) {

        if (minSnapshotAgeMillis < 0)
            throw new IllegalArgumentException(
                    "minSnapshotAgeMillis must be GTE ZERO (0), not "
                            + minSnapshotAgeMillis);

        if (minSnapshots < 1)
            throw new IllegalArgumentException(
                    "minSnapshots must be GTE ONE (1), not " + minSnapshots);

        if (minRestorePoints < 0)
            throw new IllegalArgumentException(
                    "minRestorePoints must be GTE ZERO (0), not "
                            + minRestorePoints);

        this.minSnapshotAgeMillis = minSnapshotAgeMillis;

        this.minSnapshots = minSnapshots;

        this.minRestorePoints = minRestorePoints;

    }

    /**
     * This finds and returns the commit counter for the most recent snapshot
     * whose commit time is LTE <code>now - millis</code>, where <i>millis</i>
     * is the #of milliseconds specified by the constructor for this policy.
     */
    private long getEarliestRestorableCommitCounterByAge(final HAJournal jnl,
            final long commitCounterOnJournal) {

        final long now = System.currentTimeMillis();

        final long then = now - minSnapshotAgeMillis;

        final IRootBlockView rootBlock = jnl.getSnapshotManager().find(then);

        if (rootBlock == null) {

            // There are no snapshots.
            return commitCounterOnJournal;
            
        }
        
        return rootBlock.getCommitCounter();

    }

    /**
     * This finds the snapshot that is <i>minSnapshots</i> back and returns its
     * commit counter. If there are fewer than <i>minSnapshots</i>, then this
     * returns ZERO (0).
     */
    private long getEarliestRestorableCommitCounterBySnapshots(
            final HAJournal jnl, final long commitCounterOnJournal) {

        if (minSnapshots == 0) {

            return commitCounterOnJournal;

        }
        
        final IRootBlockView rootBlock = jnl.getSnapshotManager()
                .getSnapshotByReverseIndex(minSnapshots);

        if (rootBlock == null) {

            // There are fewer than minSnapshots snapshots.
            return 0L;
            
        }
        
        return rootBlock.getCommitCounter();
        
    }

    /**
     * Find the oldest snapshot that is at least <i>minRestorePoints</i> old and
     * returns its commit counter. If there is no such snapshot, then this
     * returns ZERO (0).
     */
    private long getEarliestRestorableCommitCounterByHALogs(
            final HAJournal jnl, final long commitCounterOnJournal) {

        // The commit point that is [minRestorePoints] old.
        final long desiredCommitCounter = commitCounterOnJournal
                - minRestorePoints;

        if (desiredCommitCounter <= 0) {

            // There are fewer than this many commit points on the journal.
            return 0L;

        }

        // Find the oldest snapshot LTE that commitCounter.
        final IRootBlockView rootBlock = jnl.getSnapshotManager()
                .findByCommitCounter(desiredCommitCounter);

        if (rootBlock == null) {

            return commitCounterOnJournal;

        }

        return rootBlock.getCommitCounter();

    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: We use three different criteria here. Of necessity, this decision
     * can not be atomic. Therefore, it is possible for new snapshots and HALogs
     * to be created while we are concurrently evaluating those criteria.
     * However, this concurrency can never result in fewer files being retained
     * than are required by this policy.
     */
    @Override
    public long getEarliestRestorableCommitPoint(final HAJournal jnl) {

        // Current commit point on the journal.
        final long commitCounterOnJournal = jnl.getRootBlockView()
                .getCommitCounter();

        final long commitCounterByAge = getEarliestRestorableCommitCounterByAge(
                jnl, commitCounterOnJournal);

        final long commitCounterBySnapshots = getEarliestRestorableCommitCounterBySnapshots(
                jnl, commitCounterOnJournal);

        final long commitCounterByHALogs = getEarliestRestorableCommitCounterByHALogs(
                jnl, commitCounterOnJournal);

        final long ret = Math.min(commitCounterByAge,
                Math.min(commitCounterBySnapshots, commitCounterByHALogs));

        if (log.isInfoEnabled()) {

            log.info("policy=" + this + ", commitCounterOnJournal="
                    + commitCounterOnJournal + ", commitCounterByAge="
                    + commitCounterByAge + ", commitCounterBySnapshots="
                    + commitCounterBySnapshots + ", commitCounterByHALogs="
                    + commitCounterByHALogs
                    + ", effectiveCommitCounterReported=" + ret);

        }

        return ret;

    }

}

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

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.jini.ha.SnapshotIndex.ISnapshotRecord;

/**
 * The default restore policy. This policy supports three different criteria for
 * deciding when snapshots (aka full backups) and HALogs (aka write ahead log
 * files for each commit point aka incremental backups) may be purged.
 * <dl>
 * <dt>minRestoreAgeMillis</dt>
 * <dd>The minimum restore period (in milliseconds). Snapshots and/or HALog
 * files will be retained to ensure the ability to restore a commit point this
 * far in the past.</dd>
 * <dt>minSnapshots</dt>
 * <dd>The minimum number of snapshot files (aka full backups) that must be
 * retained (positive integer).
 * <p>
 * This must be a positive integer. If the value were ZERO a snapshot would be
 * purged as soon as it is taken. That would not provide an opportunity to make
 * a copy of the snapshot, rendering the snapshot mechanism useless.
 * <p>
 * If <code>minSnapshots:=1</code> then a snapshot, once taken, will be retained
 * until the next snapshot is taken. Further, the existence of the shapshot will
 * cause HALog files for commit points GT that snapshot to accumulate until the
 * next snapshot. This will occur regardless of the value of the other
 * parameters. Thus, if you occasionally take snapshots and move them offsite,
 * you must REMOVE the snapshot by hand in order to allow the retained HALogs to
 * be reclaimed as well.
 * <p>
 * This concern does not arise if you are taking periodic snapshots.</dd>
 * <dt>minRestorePoints</dt>
 * <dd>The minimum number of commit points that must be restorable from backup.
 * This explicitly controls the number of HALog files that will be retained
 * (non-negative). It also implicitly controls the number of snapshot files that
 * will be retained since an HALog file will pin the newest snapshot whose
 * commit counter is LTE to the the closing commit counter on that HALog file.</dd>
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
    private final long minRestoreAgeMillis;

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
                + "{minRestoreAge=" + minRestoreAgeMillis + "ms"//
                + ",minSnapshots=" + minSnapshots //
                + ",minRestorePoints=" + minRestorePoints //
                + "}";

    }
    
    /**
     * The default is to keep local backups on hand for 7 days. A minimum of
     * {@value #DEFAULT_MIN_SNAPSHOTS} will be retained. A minimum of
     * {@value #DEFAULT_MIN_RESTORE_POINTS} restore points will be retained.
     */
    public DefaultRestorePolicy() {

        this(DEFAULT_MIN_SNAPSHOT_AGE_MILLIS, DEFAULT_MIN_SNAPSHOTS,
                DEFAULT_MIN_RESTORE_POINTS);

    }

    /**
     * Create a policy that determines when local backups may be purged. A
     * minimum of {@value #DEFAULT_MIN_SNAPSHOTS} will be retained. A minimum of
     * {@value #DEFAULT_MIN_RESTORE_POINTS} restore points will be retained.
     * 
     * @param minRestoreAgeMillis
     *            The minimum age of a snapshot (in milliseconds) before it may
     *            be purged.
     */
    public DefaultRestorePolicy(final long minRestoreAgeMillis) {

        this(minRestoreAgeMillis, DEFAULT_MIN_SNAPSHOTS,
                DEFAULT_MIN_RESTORE_POINTS);

    }

    /**
     * Create a policy that determines when local backups may be purged. The
     * policy will retain local backups unless all of the criteria are
     * satisified.
     * 
     * @param minRestoreAgeMillis
     *            The minimum age of a snapshot (in milliseconds) before it may
     *            be purged (non-negative integer).
     * @param minSnapshots
     *            The minimum number of snapshots (aka full backups) that must
     *            be retained locally (positive integer).
     *            <p>
     *            This must be a positive integer. If the value were ZERO a
     *            snapshot would be purged as soon as it is taken. That would
     *            not provide an opportunity to make a copy of the snapshot,
     *            rendering the snapshot mechanism useless.
     *            <p>
     *            If <code>minSnapshots:=1</code> then a snapshot, once taken,
     *            will be retained until the next snapshot is taken. Further,
     *            the existence of the shapshot will cause HALog files for
     *            commit points GT that snapshot to accumulate until the next
     *            snapshot. This will occur regardless of the value of the other
     *            parameters. Thus, if you occasionally take snapshots and move
     *            them offsite, you must REMOVE the snapshot by hand in order to
     *            allow the retained HALogs to be reclaimed as well.
     *            <p>
     *            This concern does not arise if you are taking periodic
     *            snapshots.
     * @param minRestorePoints
     *            The minimum number of restore points (aka HALog files) that
     *            must be retained locally (non-negative integer). If an HALog
     *            is pinned by this parameter, then the oldest snapshot LTE the
     *            commit counter of that HALog is also pinned, as are all HALog
     *            files GTE the snapshot and LT the HALog.
     */
    public DefaultRestorePolicy(final long minRestoreAgeMillis,
            final int minSnapshots, final int minRestorePoints) {

        if (minRestoreAgeMillis < 0)
            throw new IllegalArgumentException(
                    "minRestoreAgeMillis must be GTE ZERO (0), not "
                            + minRestoreAgeMillis);

        if (minSnapshots < 1) {
            /*
             * This must be a positive integer. If the value were ZERO a
             * snapshot would be purged as soon as it is taken. That would not
             * provide an opportunity to make a copy of the snapshot, rendering
             * the snapshot mechanism useless.
             * 
             * If minSnapshots:=1 then a snapshot, once taken, will be retained
             * until the next snapshot is taken. Further, the existence of the
             * shapshot will cause HALog files for commit points GT that
             * snapshot to accumulate until the next snapshot. This will occur
             * regardless of the value of the other parameters. Thus, if you
             * occasionally take snapshots and move them offsite, you must
             * REMOVE the snapshot by hand in order to allow the retained HALogs
             * to be reclaimed as well.
             * 
             * This concern does not arise if you are taking periodic snapshots.
             */
            throw new IllegalArgumentException(
                    "minSnapshots must be GTE ONE (1), not " + minSnapshots);
        }

        if (minRestorePoints < 0)
            throw new IllegalArgumentException(
                    "minRestorePoints must be GTE ZERO (0), not "
                            + minRestorePoints);

        this.minRestoreAgeMillis = minRestoreAgeMillis;

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

        // The current time.
        final long now = System.currentTimeMillis();

        // A moment [minRestoreAge] milliseconds ago.
        final long then = now - minRestoreAgeMillis;

        // The root block for the snapshot with a commitTime LTE [then].
        final ISnapshotRecord sr = jnl.getSnapshotManager().find(then);
        final IRootBlockView rootBlock = sr == null ? null : sr.getRootBlock();

        if (rootBlock == null) {

            // There are no matching snapshots.
            return commitCounterOnJournal;
            
        }
        
        if (log.isInfoEnabled())
            log.info("minRestoreAgeMillis=" + minRestoreAgeMillis + ", now="
                    + now + ", then=" + then + ", rootBlock=" + rootBlock);
        
        return rootBlock.getCommitCounter();

    }

    /**
     * This finds the snapshot that is <i>minSnapshots</i> back and returns its
     * commit counter. If there are fewer than <i>minSnapshots</i>, then this
     * returns ZERO (0).
     */
    private long getEarliestRestorableCommitCounterBySnapshots(
            final HAJournal jnl, final long commitCounterOnJournal) {

        final ISnapshotRecord r = jnl.getSnapshotManager()
                .getSnapshotByReverseIndex(minSnapshots - 1);

        if (r == null) {

            // There are fewer than minSnapshots snapshots.
            return 0L;
            
        }
        
        return r.getRootBlock().getCommitCounter();
        
    }

    /**
     * Find the oldest snapshot that is at least <i>minRestorePoints</i> old and
     * returns its commit counter. If there is no such snapshot, then this
     * returns ZERO (0).
     */
    private long getEarliestRestorableCommitCounterByCommitPoints(
            final HAJournal jnl, final long commitCounterOnJournal) {

        // The commit point that is [minRestorePoints] old.
        final long desiredCommitCounter = commitCounterOnJournal
                - minRestorePoints;

        if (desiredCommitCounter <= 0) {

            // There are fewer than this many commit points on the journal.
            return 0L;

        }

        // Find the oldest snapshot LTE that commitCounter.
        final ISnapshotRecord r = jnl.getSnapshotManager().findByCommitCounter(
                desiredCommitCounter);

        if (r == null) {

            return commitCounterOnJournal;

        }

        return r.getRootBlock().getCommitCounter();

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

        final long commitCounterRetainedByAge = getEarliestRestorableCommitCounterByAge(
                jnl, commitCounterOnJournal);

        final long commitCounterRetainedBySnapshotCount = getEarliestRestorableCommitCounterBySnapshots(
                jnl, commitCounterOnJournal);

        final long commitCounterRetainedByHALogCount = getEarliestRestorableCommitCounterByCommitPoints(
                jnl, commitCounterOnJournal);

        /*
         * Take the minimum of those values. This is the commit counter that
         * will be retained.
         * 
         * Snapshot files and HALogs GTE this commit counter will not be
         * released.
         */
        final long commitCounterRetained = Math.min(commitCounterRetainedByAge,
                Math.min(commitCounterRetainedBySnapshotCount,
                        commitCounterRetainedByHALogCount));

        if (log.isInfoEnabled()) {

            log.info("policy="
                    + this
                    + //
                    ", commitCounterOnJournal="
                    + commitCounterOnJournal //
                    + ", commitCounterByAge="
                    + commitCounterRetainedByAge //
                    + ", commitCounterBySnapshots="
                    + commitCounterRetainedBySnapshotCount //
                    + ", commitCounterByHALogs="
                    + commitCounterRetainedByHALogCount//
                    + ", effectiveCommitCounterRetained="
                    + commitCounterRetained);

        }

        return commitCounterRetained;

    }

}

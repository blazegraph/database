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

import java.io.File;
import java.util.Calendar;

import net.jini.config.Configuration;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.halog.HALogReader;
import com.bigdata.ha.halog.IHALogReader;
import com.bigdata.journal.CommitCounterUtility;

/**
 * HA5 test suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestHA5JournalServerWithHALogs extends AbstractHA5JournalServerTestCase {

    /**
     * We need to set the time at which the {@link DefaultSnapshotPolicy} runs
     * to some point in the future in order to avoid test failures due to
     * violated assumptions when the policy runs up self-triggering (based on
     * the specified run time) during a CI run.
     * <p>
     * We do this by adding one hour to [now] and then converting it into the
     * 'hhmm' format as an integer.
     * 
     * @return The "never run" time as hhmm.
     */
    static protected String getNeverRunSnapshotTime() {
        
        // Right now.
        final Calendar c = Calendar.getInstance();
        
        // Plus an hour.
        c.add(Calendar.HOUR_OF_DAY, 1);
        
        // Get the hour.
        final int hh = c.get(Calendar.HOUR_OF_DAY);
        
        // And the minutes.
        final int mm = c.get(Calendar.MINUTE);
        
        // Format as hhmm.
        final String neverRun = "" + hh + (mm < 10 ? "0" : "") + mm;

        return neverRun;
        
    }

    @Override
    protected int replicationFactor() {

        return 5;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: This overrides some {@link Configuration} values for the
     * {@link HAJournalServer} in order to establish conditions suitable for
     * testing the {@link ISnapshotPolicy} and {@link IRestorePolicy}.
     */
    @Override
    protected String[] getOverrides() {
        
        /*
         * We need to set the time at which the DefaultSnapshotPolicy runs to
         * some point in the Future in order to avoid test failures due to
         * violated assumptions when the policy runs up self-triggering (based
         * on the specified run time) during a CI run.
         */
        final String neverRun = getNeverRunSnapshotTime();
        
        return new String[]{
                "com.bigdata.journal.jini.ha.HAJournalServer.restorePolicy=new com.bigdata.journal.jini.ha.DefaultRestorePolicy()",
                "com.bigdata.journal.jini.ha.HAJournalServer.snapshotPolicy=new com.bigdata.journal.jini.ha.DefaultSnapshotPolicy("+neverRun+",0)",
                "com.bigdata.journal.jini.ha.HAJournalServer.replicationFactor="+replicationFactor(),
        };
        
    }
    
    public TestHA5JournalServerWithHALogs() {
    }

    public TestHA5JournalServerWithHALogs(String name) {
        super(name);
    }

    /**
     * This is a unit test for the ability to silently remove a logically empty
     * HALog file. Three services are started in sequence (A,B,C). A series of
     * small commits are applied to the quorum. (C) is then shutdown. A
     * logically empty HALog file should exist on each service for the next
     * commit point. However, since this might have been removed on C when it
     * was shutdown, we copy the logically empty HALog file from (A) to (C). We
     * then do one more update. C is then restarted. We verify that C restarts
     * and that the logically empty HALog file has been replaced by an HALog
     * file that has the same digest as the HALog file for that commit point on
     * (A,B).
     * <p>
     * Note: We can not reliably observe that the logically HALog file was
     * removed during startup. However, this is not critical. What is critical
     * is that the logically empty HALog file (a) does not prevent (C) from
     * starting; (b) is replaced by the correct HALog data from the quorum
     * leader; and (c) that (C) resynchronizes with the met quorum and joins
     * causing a fully met quorum.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/679" >
     *      HAJournalServer can not restart due to logically empty log files
     *      </a>
     */
    public void test_startABCDE_logicallyEmptyLogFileDeletedOnRestartC() throws Exception {

        final ABCDE abc = new ABCDE(true/* sequential */);

        final HAGlue serverA = abc.serverA, serverB = abc.serverB;
        HAGlue serverC = abc.serverC;
        HAGlue serverD = abc.serverD;
        HAGlue serverE = abc.serverD;

        // Verify quorum is FULLY met.
        awaitFullyMetQuorum();

        // await the KB create commit point to become visible on each service.
        awaitCommitCounter(1L, new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

        // Verify binary equality of ALL HALog files.
        assertHALogDigestsEquals(1L/* firstCommitCounter */,
                1/* lastCommitCounter */, new HAGlue[] { serverA, serverB,
                        serverC, serverD, serverE });

        /*
         * Do a series of small commits.
         */

        final int NSMALL = 5;
        
        for (int i = 1/* createKB */; i <= NSMALL; i++) {

            simpleTransaction();
            
        }

        final long commitCounter1 = 1 + NSMALL; // AKA (6)

        // await the commit points to become visible.
        awaitCommitCounter(commitCounter1,
                new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

        // Verify binary equality of ALL HALog files.
//        assertHALogDigestsEquals(1L/* firstCommitCounter */, commitCounter1,
//                new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

        /*
         * Verify the expected #of HALogs on each service.
         * 
         * Note: This is (lastCommitCounter+1) since an empty HALog was created
         * for the next commit point.
         */
        awaitLogCount(getHALogDirA(), commitCounter1 + 1);
        awaitLogCount(getHALogDirB(), commitCounter1 + 1);
        awaitLogCount(getHALogDirC(), commitCounter1 + 1);
        awaitLogCount(getHALogDirD(), commitCounter1 + 1);
        awaitLogCount(getHALogDirE(), commitCounter1 + 1);

        /*
         * Shutdown C.
         * 
         * Note: This might cause the empty HALog file on (C) to be deleted.
         * That is Ok, since we will copy the desired empty HALOg from (A) to
         * (C), thus enforcing the desired test condition.
         */
        shutdownC();
        
        /*
         * Verify that there is an empty HALog file on (A) for the next
         * commit point.
         */
        
        // The next commit point.
        final long commitCounter2 = commitCounter1 + 1; // AKA (7)
        
        // The HALog for that next commit point.
        final File fileA = CommitCounterUtility.getCommitCounterFile(
                getHALogDirA(), commitCounter2, IHALogReader.HA_LOG_EXT);
        
        // Verify HALog file for next commit point on A is logically empty.
        {
            assertTrue(fileA.exists());
            final IHALogReader r = new HALogReader(fileA);
            assertTrue(r.isEmpty());
            assertFalse(r.isLive());
            r.close();
            assertTrue(fileA.exists());
        }

        // The name of that HALog file on (C).
        final File fileC = CommitCounterUtility.getCommitCounterFile(
                getHALogDirC(), commitCounter2, IHALogReader.HA_LOG_EXT);

        // Copy that empty HALog file to (C).
        copyFile(fileA, fileC, false/* append */);

        /*
         * Do another transaction. This will cause the HALog file for that
         * commit point to be non-empty on A.
         */
        simpleTransaction();
        
        /*
         * Await the commit points to become visible.
         * 
         * Note: This is (lastCommitCounter+1) since an empty HALog was created
         * for the next commit point.
         */
        awaitCommitCounter(commitCounter2, new HAGlue[] { serverA, serverB, serverD, serverE });

        // Verify the expected #of HALogs on each service.
        awaitLogCount(getHALogDirA(), commitCounter2 + 1);
        awaitLogCount(getHALogDirB(), commitCounter2 + 1);
        awaitLogCount(getHALogDirD(), commitCounter2 + 1);
        awaitLogCount(getHALogDirE(), commitCounter2 + 1);
        awaitLogCount(getHALogDirC(), commitCounter2);

        // Verify HALog file for next commit point on A is NOT empty.
        {
            assertTrue(fileA.exists());
            final IHALogReader r = new HALogReader(fileA);
            assertFalse(r.isEmpty());
            assertFalse(r.isLive());
            r.close();
            assertTrue(fileA.exists());
        }

        // Verify HALog file for next commit point on C is logically empty.
        {
            assertTrue(fileC.exists());
            final IHALogReader r = new HALogReader(fileC);
            assertTrue(r.isEmpty());
            assertFalse(r.isLive());
            r.close();
            assertTrue(fileC.exists());
        }

        /*
         * Restart (C). It should start without complaint. The logically empty
         * HALog file should be replaced by the corresponding file from (A) by
         * the time the quorum fully meets. At this point all services will have
         * the same digests for all HALog files.
         */

        // Restart C.
        serverC = startC();
        
        // Wait until the quorum is fully met.
        awaitFullyMetQuorum();
        
        // await the commit points to become visible.
        awaitCommitCounter(commitCounter2,
                new HAGlue[] { serverA, serverB, serverD, serverE, serverC });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });
        
        // Verify binary equality of ALL HALog files.
        assertHALogDigestsEquals(1L/* firstCommitCounter */,
                commitCounter2 /* lastCommitCounter */, new HAGlue[] { serverA,
                        serverB, serverD, serverE, serverC });

        /*
         * Verify the expected #of HALogs on each service.
         * 
         * Note: Each service will have an empty HALog for the next commit
         * point.
         */
        awaitLogCount(getHALogDirA(), commitCounter2+1);
        awaitLogCount(getHALogDirB(), commitCounter2+1);
        awaitLogCount(getHALogDirD(), commitCounter2+1);
        awaitLogCount(getHALogDirE(), commitCounter2+1);
        awaitLogCount(getHALogDirC(), commitCounter2+1);

    }
    
    
    /**
     * This is a unit test for the ability to silently remove a physically empty
     * HALog file. Three services are started in sequence (A,B,C). A series of
     * small commits are applied to the quorum. (C) is then shutdown. A
     * logically empty HALog file should exist on each service for the next
     * commit point. We now overwrite that file with a physically empty HALog
     * file (zero length). We then do one more update. C is then restarted. We
     * verify that C restarts and that the logically empty HALog file has been
     * replaced by an HALog file that has the same digest as the HALog file for
     * that commit point on (A,B).
     * <p>
     * Note: We can not reliably observe that the physically HALog file was
     * removed during startup. However, this is not critical. What is critical
     * is that the physically empty HALog file (a) does not prevent (C) from
     * starting; (b) is replaced by the correct HALog data from the quorum
     * leader; and (c) that (C) resynchronizes with the met quorum and joins
     * causing a fully met quorum.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/679" >
     *      HAJournalServer can not restart due to logically empty log files
     *      </a>
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/775" >
     *      HAJournal start() </a>
     */
    public void test_startABCDE_physicallyEmptyLogFileDeletedOnRestartC() throws Exception {

        final ABCDE abc = new ABCDE(true/* sequential */);

        final HAGlue serverA = abc.serverA, serverB = abc.serverB;
        HAGlue serverC = abc.serverC;
        HAGlue serverD = abc.serverD;
        HAGlue serverE = abc.serverE;

        // Verify quorum is FULLY met.
        awaitFullyMetQuorum();

        // await the KB create commit point to become visible on each service.
        awaitCommitCounter(1L, new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

        // Verify binary equality of ALL HALog files.
        assertHALogDigestsEquals(1L/* firstCommitCounter */,
                1/* lastCommitCounter */, new HAGlue[] { serverA, serverB,
                        serverC, serverD, serverE });

        /*
         * Do a series of small commits.
         */

        final int NSMALL = 5;
        
        for (int i = 1/* createKB */; i <= NSMALL; i++) {

            simpleTransaction();
            
        }

        final long commitCounter1 = 1 + NSMALL; // AKA (6)

        // await the commit points to become visible.
        awaitCommitCounter(commitCounter1,
                new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

        // Verify binary equality of ALL HALog files.
        assertHALogDigestsEquals(1L/* firstCommitCounter */, commitCounter1,
                new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

        /*
         * Verify the expected #of HALogs on each service.
         * 
         * Note: This is (lastCommitCounter+1) since an empty HALog was created
         * for the next commit point.
         */
        awaitLogCount(getHALogDirA(), commitCounter1 + 1);
        awaitLogCount(getHALogDirB(), commitCounter1 + 1);
        awaitLogCount(getHALogDirC(), commitCounter1 + 1);
        awaitLogCount(getHALogDirD(), commitCounter1 + 1);
        awaitLogCount(getHALogDirE(), commitCounter1 + 1);

        /*
         * Shutdown C.
         * 
         * Note: This might cause the empty HALog file on (C) to be deleted.
         * That is Ok, since we will copy the desired empty HALOg from (A) to
         * (C), thus enforcing the desired test condition.
         */
        shutdownC();
        
        /*
         * Verify that there is an empty HALog file on (A) for the next
         * commit point.
         */
        
        // The next commit point.
        final long commitCounter2 = commitCounter1 + 1; // AKA (7)
        
        // The HALog for that next commit point.
        final File fileA = CommitCounterUtility.getCommitCounterFile(
                getHALogDirA(), commitCounter2, IHALogReader.HA_LOG_EXT);
        
        // Verify HALog file for next commit point on A is logically empty.
        {
            assertTrue(fileA.exists());
            final IHALogReader r = new HALogReader(fileA);
            assertTrue(r.isEmpty());
            assertFalse(r.isLive());
            r.close();
            assertTrue(fileA.exists());
        }

        // The name of that HALog file on (C).
        final File fileC = CommitCounterUtility.getCommitCounterFile(
                getHALogDirC(), commitCounter2, IHALogReader.HA_LOG_EXT);

//        // Copy that empty HALog file to (C).
//        copyFile(fileA, fileC, false/* append */);
        
        // delete the logically empty file (if it exists).
        if (fileC.exists() && !fileC.delete())
            fail("Could not delete: fileC=" + fileC);

        // create the physically empty file.
        if (!fileC.createNewFile())
            fail("Could not create: fileC=" + fileC);

        /*
         * Do another transaction. This will cause the HALog file for that
         * commit point to be non-empty on A.
         */
        simpleTransaction();
        
        /*
         * Await the commit points to become visible.
         * 
         * Note: This is (lastCommitCounter+1) since an empty HALog was created
         * for the next commit point.
         */
        awaitCommitCounter(commitCounter2, new HAGlue[] { serverA, serverB, serverD, serverE });

        // Verify the expected #of HALogs on each service.
        awaitLogCount(getHALogDirA(), commitCounter2 + 1);
        awaitLogCount(getHALogDirB(), commitCounter2 + 1);
        awaitLogCount(getHALogDirD(), commitCounter2 + 1);
        awaitLogCount(getHALogDirE(), commitCounter2 + 1);
        awaitLogCount(getHALogDirC(), commitCounter2);

        // Verify HALog file for next commit point on A is NOT empty.
        {
            assertTrue(fileA.exists());
            final IHALogReader r = new HALogReader(fileA);
            assertFalse(r.isEmpty());
            assertFalse(r.isLive());
            r.close();
            assertTrue(fileA.exists());
        }

        // Verify HALog file for next commit point on C is phsyically empty.
        {
            assertTrue(fileC.exists());
            assertEquals(0L, fileC.length());
        }

        /*
         * Restart (C). It should start without complaint. The logically empty
         * HALog file should be replaced by the corresponding file from (A) by
         * the time the quorum fully meets. At this point all services will have
         * the same digests for all HALog files.
         */

        // Restart C.
        serverC = startC();
        
        // Wait until the quorum is fully met.
        awaitFullyMetQuorum();
        
        // await the commit points to become visible.
        awaitCommitCounter(commitCounter2,
                new HAGlue[] { serverA, serverB, serverD, serverE, serverC });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverD, serverE, serverC });
        
        // Verify binary equality of ALL HALog files.
        assertHALogDigestsEquals(1L/* firstCommitCounter */,
                commitCounter2 /* lastCommitCounter */, new HAGlue[] { serverA,
                        serverB, serverD, serverE, serverC });

        /*
         * Verify the expected #of HALogs on each service.
         * 
         * Note: Each service will have an empty HALog for the next commit
         * point.
         */
        awaitLogCount(getHALogDirA(), commitCounter2+1);
        awaitLogCount(getHALogDirB(), commitCounter2+1);
        awaitLogCount(getHALogDirC(), commitCounter2+1);
        awaitLogCount(getHALogDirD(), commitCounter2+1);
        awaitLogCount(getHALogDirE(), commitCounter2+1);

    }
    
    /**
     * Unit test for a situation in which A B C D and E start. A quorum meets and the
     * final service resyncs with the met quorum. The quorum then fully meets.
     * Once the fully met quorum is stable, C is then restarted. This test
     * exercises a code path that handles the case where C is current, but is
     * forced into RESYNC in case there are writes in progress on the leader.
     * <p>
     * Note: In this version of the test, the HALog files are NOT purged at each
     * commit of the fully met quorum.
     */
    public void testStartABCDE_restartE() throws Exception {
        
        final ABCDE x = new ABCDE(true/*sequential*/);
        
        final long token = awaitFullyMetQuorum();
        
        // Now run several transactions
        final int NTX = 5;
        for (int i = 0; i < NTX; i++)
            simpleTransaction();

        // wait until the commit point is registered on all services.
        awaitCommitCounter(NTX + 1L, new HAGlue[] { x.serverA, x.serverB,
        		x.serverC, x.serverD, x.serverE });
        
        /*
         * The same number of HALog files should exist on all services.
         * 
         * Note: the restore policy is setup such that we are NOT purging the HALog
         * files at each commit of a fully met quorum.
         */
        awaitLogCount(getHALogDirA(), NTX + 2L);
        awaitLogCount(getHALogDirB(), NTX + 2L);
        awaitLogCount(getHALogDirC(), NTX + 2L);
        awaitLogCount(getHALogDirD(), NTX + 2L);
        awaitLogCount(getHALogDirE(), NTX + 2L);
        
        // shutdown E - final service
        shutdownE();
        
        // wait for C to be gone from zookeeper.
        awaitPipeline(new HAGlue[] { x.serverA, x.serverB, x.serverC, x.serverD });
        awaitMembers(new HAGlue[] { x.serverA, x.serverB, x.serverC, x.serverD });
        awaitJoined(new HAGlue[] { x.serverA, x.serverB, x.serverC, x.serverD });
        
        // restart C.
        /*final HAGlue serverC =*/ startE();
        
        // wait until the quorum fully meets again (on the same token).
        assertEquals(token, awaitFullyMetQuorum());

        // Verify expected HALog files.
        awaitLogCount(getHALogDirA(), NTX + 2L);
        awaitLogCount(getHALogDirB(), NTX + 2L);
        awaitLogCount(getHALogDirC(), NTX + 2L);
        awaitLogCount(getHALogDirD(), NTX + 2L);
        awaitLogCount(getHALogDirE(), NTX + 2L);
        
    }

    /**
     * Unit test for a situation in which A B C D and E start. A quorum mets and the
     * third service resyncs with the met quorum. The quorum then fully meets.
     * Once the fully met quorum is stable, B is then restarted. The pipeline is
     * reorganized when B is shutdown but the quorum does not break. This test
     * exercises a code path that handles the case where B is current, but is
     * forced into RESYNC in case there are writes in progress on the leader.
     * <p>
     * Note: In this version of the test, the HALog files are NOT purged at each
     * commit of the fully met quorum.
     */
    public void testStartABCDE_restartB() throws Exception {
        
        final ABCDE x = new ABCDE(true/*sequential*/);
        
        final long token = awaitFullyMetQuorum();
        
        // Now run several transactions
        final int NTX = 5;
        for (int i = 0; i < NTX; i++)
            simpleTransaction();

        // wait until the commit point is registered on all services.
        awaitCommitCounter(NTX + 1L, new HAGlue[] { x.serverA, x.serverB,
                x.serverC, x.serverD, x.serverE });
        
        /*
         * The same number of HALog files should exist on all services.
         * 
         * Note: the restore policy is setup such that we are purging the HALog
         * files at each commit of a fully met quorum.
         */
        awaitLogCount(getHALogDirA(), NTX + 2L);
        awaitLogCount(getHALogDirB(), NTX + 2L);
        awaitLogCount(getHALogDirC(), NTX + 2L);
        awaitLogCount(getHALogDirD(), NTX + 2L);
        awaitLogCount(getHALogDirE(), NTX + 2L);
        
        // shutdown B.
        shutdownB();
        
        // wait for B to be gone from zookeeper.
        awaitPipeline(new HAGlue[] { x.serverA, x.serverC, x.serverD, x.serverE });
        awaitMembers(new HAGlue[] { x.serverA, x.serverC, x.serverD, x.serverE });
        awaitJoined(new HAGlue[] { x.serverA, x.serverC, x.serverD, x.serverE });
        
        // restart B.
        /*final HAGlue serverB =*/ startB();
        
        // wait until the quorum fully meets again (on the same token).
        assertEquals(token, awaitFullyMetQuorum());

        // Verify expected HALog files.
        awaitLogCount(getHALogDirA(), NTX + 2L);
        awaitLogCount(getHALogDirB(), NTX + 2L);
        awaitLogCount(getHALogDirC(), NTX + 2L);
        awaitLogCount(getHALogDirD(), NTX + 2L);
        awaitLogCount(getHALogDirE(), NTX + 2L);
        
    }

    /**
     * Unit test for a situation in which A B C D and E start. A quorum mets and the
     * third service resyncs with the met quorum. The quorum then fully meets.
     * Once the fully met quorum is stable, A is then restarted. The pipeline is
     * reorganized when A is shutdown and a new leader is elected. This test
     * exercises a code path that handles the case where A is current, but is
     * forced into RESYNC in case there are writes in progress on the leader.
     * <p>
     * Note: In this version of the test, the HALog files are NOT purged at each
     * commit of the fully met quorum.
     */
    public void testStartABCDE_restartA() throws Exception {
        
        final ABCDE x = new ABCDE(true/*sequential*/);
        
        final long token = awaitFullyMetQuorum();
        
        // Now run several transactions
        final int NTX = 5;
        for (int i = 0; i < NTX; i++)
            simpleTransaction();

        // wait until the commit point is registered on all services.
        awaitCommitCounter(NTX + 1L, new HAGlue[] { x.serverA, x.serverB,
                x.serverC, x.serverD, x.serverE });
        
        /*
         * The same number of HALog files should exist on all services.
         * 
         * Note: the restore policy is setup such that we are NOT purging the HALog
         * files at each commit of a fully met quorum.
         */
        awaitLogCount(getHALogDirA(), NTX + 2L);
        awaitLogCount(getHALogDirB(), NTX + 2L);
        awaitLogCount(getHALogDirC(), NTX + 2L);
        awaitLogCount(getHALogDirD(), NTX + 2L);
        awaitLogCount(getHALogDirE(), NTX + 2L);
        
        // shutdown A.
        shutdownA();
        
        // wait for A to be gone from zookeeper.
//        awaitPipeline(new HAGlue[] { x.serverA, x.serverC });
//        awaitMembers(new HAGlue[] { x.serverA, x.serverC });
//        awaitJoined(new HAGlue[] { x.serverA, x.serverC });
        
        // since the leader failed over, the quorum meets on a new token.
        final long token2 = awaitNextQuorumMeet(token);
        
        // restart A.
        /*final HAGlue serverA =*/ startA();

        // wait until the quorum fully meets again (on the same token).
        assertEquals(token2, awaitFullyMetQuorum());

        // Verify expected HALog files.
        awaitLogCount(getHALogDirA(), NTX + 2L);
        awaitLogCount(getHALogDirB(), NTX + 2L);
        awaitLogCount(getHALogDirC(), NTX + 2L);
        awaitLogCount(getHALogDirD(), NTX + 2L);
        awaitLogCount(getHALogDirE(), NTX + 2L);
    }

}

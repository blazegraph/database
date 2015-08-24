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
/*
 * Created on Oct 31, 2012
 */
package com.bigdata.journal.jini.ha;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import net.jini.config.Configuration;

import com.bigdata.ha.HACommitGlue;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.ha.halog.HALogReader;
import com.bigdata.ha.halog.IHALogReader;
import com.bigdata.ha.msg.IHA2PhasePrepareMessage;
import com.bigdata.journal.CommitCounterUtility;
import com.bigdata.journal.FileMetadata;
import com.bigdata.journal.jini.ha.HAJournalTest.HAGlueTest;
import com.bigdata.util.InnerCause;

/**
 * Test suite when we are using the {@link DefaultSnapshotPolicy} and
 * {@link DefaultRestorePolicy}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see TestHA3RestorePolicy
 */
public class TestHA3JournalServerWithHALogs extends AbstractHA3BackupTestCase {

    public TestHA3JournalServerWithHALogs() {
    }

    public TestHA3JournalServerWithHALogs(String name) {
        super(name);
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
        };
        
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
    public void test_startABC_logicallyEmptyLogFileDeletedOnRestartC() throws Exception {

        final ABC abc = new ABC(true/* sequential */);

        final HAGlue serverA = abc.serverA, serverB = abc.serverB;
        HAGlue serverC = abc.serverC;

        // Verify quorum is FULLY met.
        awaitFullyMetQuorum();

        // await the KB create commit point to become visible on each service.
        awaitCommitCounter(1L, new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL HALog files.
        assertHALogDigestsEquals(1L/* firstCommitCounter */,
                1/* lastCommitCounter */, new HAGlue[] { serverA, serverB,
                        serverC });

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
                new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL HALog files.
        assertHALogDigestsEquals(1L/* firstCommitCounter */, commitCounter1,
                new HAGlue[] { serverA, serverB, serverC });

        /*
         * Verify the expected #of HALogs on each service.
         * 
         * Note: This is (lastCommitCounter+1) since an empty HALog was created
         * for the next commit point.
         */
        awaitLogCount(getHALogDirA(), commitCounter1 + 1);
        awaitLogCount(getHALogDirB(), commitCounter1 + 1);
        awaitLogCount(getHALogDirC(), commitCounter1 + 1);

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
        awaitCommitCounter(commitCounter2, new HAGlue[] { serverA, serverB });

        // Verify the expected #of HALogs on each service.
        awaitLogCount(getHALogDirA(), commitCounter2 + 1);
        awaitLogCount(getHALogDirB(), commitCounter2 + 1);
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
                new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });
        
        // Verify binary equality of ALL HALog files.
        assertHALogDigestsEquals(1L/* firstCommitCounter */,
                commitCounter2 /* lastCommitCounter */, new HAGlue[] { serverA,
                        serverB, serverC });

        /*
         * Verify the expected #of HALogs on each service.
         * 
         * Note: Each service will have an empty HALog for the next commit
         * point.
         */
        awaitLogCount(getHALogDirA(), commitCounter2+1);
        awaitLogCount(getHALogDirB(), commitCounter2+1);
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
    public void test_startABC_physicallyEmptyLogFileDeletedOnRestartC() throws Exception {

        final ABC abc = new ABC(true/* sequential */);

        final HAGlue serverA = abc.serverA, serverB = abc.serverB;
        HAGlue serverC = abc.serverC;

        // Verify quorum is FULLY met.
        awaitFullyMetQuorum();

        // await the KB create commit point to become visible on each service.
        awaitCommitCounter(1L, new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL HALog files.
        assertHALogDigestsEquals(1L/* firstCommitCounter */,
                1/* lastCommitCounter */, new HAGlue[] { serverA, serverB,
                        serverC });

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
                new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL HALog files.
        assertHALogDigestsEquals(1L/* firstCommitCounter */, commitCounter1,
                new HAGlue[] { serverA, serverB, serverC });

        /*
         * Verify the expected #of HALogs on each service.
         * 
         * Note: This is (lastCommitCounter+1) since an empty HALog was created
         * for the next commit point.
         */
        awaitLogCount(getHALogDirA(), commitCounter1 + 1);
        awaitLogCount(getHALogDirB(), commitCounter1 + 1);
        awaitLogCount(getHALogDirC(), commitCounter1 + 1);

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
        awaitCommitCounter(commitCounter2, new HAGlue[] { serverA, serverB });

        // Verify the expected #of HALogs on each service.
        awaitLogCount(getHALogDirA(), commitCounter2 + 1);
        awaitLogCount(getHALogDirB(), commitCounter2 + 1);
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
                new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });
        
        // Verify binary equality of ALL HALog files.
        assertHALogDigestsEquals(1L/* firstCommitCounter */,
                commitCounter2 /* lastCommitCounter */, new HAGlue[] { serverA,
                        serverB, serverC });

        /*
         * Verify the expected #of HALogs on each service.
         * 
         * Note: Each service will have an empty HALog for the next commit
         * point.
         */
        awaitLogCount(getHALogDirA(), commitCounter2+1);
        awaitLogCount(getHALogDirB(), commitCounter2+1);
        awaitLogCount(getHALogDirC(), commitCounter2+1);

    }
    
    /**
     * This is a variant test for the ability to silently remove a corrupt HALog
     * file on restart when it is the HALog file for the first write set not yet
     * committed on the journal. Three services are started in sequence (A,B,C).
     * A series of small commits are applied to the quorum. (C) is then
     * shutdown. A logically empty HALog file should exist on each service for
     * the next commit point. However, since this might have been removed on C
     * when it was shutdown, we copy the logically empty HALog file from (A) to
     * (C). We then overwrite the root blocks of that logically empty HALog file
     * with junk. We then do one more update. C is then restarted. We verify
     * that C restarts and that the corrupt HALog file has been replaced
     * by an HALog file that has the same digest as the HALog file for that
     * commit point on (A,B).
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
    public void test_startABC_corruptLogFileDeletedOnRestartC() throws Exception {

        final ABC abc = new ABC(true/* sequential */);

        final HAGlue serverA = abc.serverA, serverB = abc.serverB;
        HAGlue serverC = abc.serverC;

        // Verify quorum is FULLY met.
        awaitFullyMetQuorum();

        // await the KB create commit point to become visible on each service.
        awaitCommitCounter(1L, new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL HALog files.
        assertHALogDigestsEquals(1L/* firstCommitCounter */,
                1/* lastCommitCounter */, new HAGlue[] { serverA, serverB,
                        serverC });

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
                new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL HALog files.
        assertHALogDigestsEquals(1L/* firstCommitCounter */, commitCounter1,
                new HAGlue[] { serverA, serverB, serverC });

        /*
         * Verify the expected #of HALogs on each service.
         * 
         * Note: This is (lastCommitCounter+1) since an empty HALog was created
         * for the next commit point.
         */
        awaitLogCount(getHALogDirA(), commitCounter1 + 1);
        awaitLogCount(getHALogDirB(), commitCounter1 + 1);
        awaitLogCount(getHALogDirC(), commitCounter1 + 1);

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
         * Overwrite the root blocks of the HALog on (C).
         */
        {
            final OutputStream os = new FileOutputStream(fileC);
            try {
                final ByteBuffer buf = getRandomData(FileMetadata.headerSize0);
                final byte[] b = getBytes(buf);
                os.write(b);
                os.flush();
            } finally {
                os.close();
            }
        }

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
        awaitCommitCounter(commitCounter2, new HAGlue[] { serverA, serverB });

        // Verify the expected #of HALogs on each service.
        awaitLogCount(getHALogDirA(), commitCounter2 + 1);
        awaitLogCount(getHALogDirB(), commitCounter2 + 1);
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

        // Verify HALog file for next commit point on C is corrupt.
        {
            boolean ok = false;
            try {
                new HALogReader(fileC);
                ok = true;
            } catch(Throwable t) {
                // Note: Could be IOException, ChecksumError, or
                // RuntimeException.
            }
            if (ok)
                fail("HALog is not corrupt: " + fileC);
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
                new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });
        
        // Verify binary equality of ALL HALog files.
        assertHALogDigestsEquals(1L/* firstCommitCounter */,
                commitCounter2 /* lastCommitCounter */, new HAGlue[] { serverA,
                        serverB, serverC });

        /*
         * Verify the expected #of HALogs on each service.
         * 
         * Note: Each service will have an empty HALog for the next commit
         * point.
         */
        awaitLogCount(getHALogDirA(), commitCounter2+1);
        awaitLogCount(getHALogDirB(), commitCounter2+1);
        awaitLogCount(getHALogDirC(), commitCounter2+1);

    }
    
    /**
     * This is a unit test for the ability to correctly NOT remove a logically
     * empty HALog file when that HALog file is for the last commit point on the
     * Journal. Three services are started in sequence (A,B,C). A series of
     * small commits are applied to the quorum. (C) is then shutdown. A
     * logically empty HALog file should exist on each service for the next
     * commit point. We remove the HALog for the next commit point from (C) if
     * it exists. We then remove the HALog for the last durable commit point on
     * (C) and replace it with a physically empty HALog file. We then do one
     * more update. C is then restarted. We verify that C DOES NOT restart and
     * that the physically empty HALog file for the last durable commit point on
     * C has not been removed or updated.
     * 
     * TODO This is the staring place for adding the capability to automatically
     * replicate bad or missing historical HALog files from the quorum leader.
     * The tests exists now to ensure that the logic to remove a bad HALog on
     * startup will refuse to remove an HALog corresponding to the most recent
     * commit point on the Journal.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/679" >
     *      HAJournalServer can not restart due to logically empty log files
     *      </a>
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/775" >
     *      HAJournal start() </a>
     */
    public void test_startABC_missingHALogFileForLastCommitBlocksRestartC() throws Exception {

        final ABC abc = new ABC(true/* sequential */);

        final HAGlue serverA = abc.serverA, serverB = abc.serverB;
        HAGlue serverC = abc.serverC;

        // Verify quorum is FULLY met.
        awaitFullyMetQuorum();

        // await the KB create commit point to become visible on each service.
        awaitCommitCounter(1L, new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL HALog files.
        assertHALogDigestsEquals(1L/* firstCommitCounter */,
                1/* lastCommitCounter */, new HAGlue[] { serverA, serverB,
                        serverC });

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
                new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL HALog files.
        assertHALogDigestsEquals(1L/* firstCommitCounter */, commitCounter1,
                new HAGlue[] { serverA, serverB, serverC });

        /*
         * Verify the expected #of HALogs on each service.
         * 
         * Note: This is (lastCommitCounter+1) since an empty HALog was created
         * for the next commit point.
         */
        awaitLogCount(getHALogDirA(), commitCounter1 + 1);
        awaitLogCount(getHALogDirB(), commitCounter1 + 1);
        awaitLogCount(getHALogDirC(), commitCounter1 + 1);

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
        if (fileC.exists())
            if (!fileC.delete())
                fail("Could not remove HALog for open write set: " + fileC);

        // The HALog file on (C) for the last durable commit point on (C).
        final File fileCLastCommit = CommitCounterUtility.getCommitCounterFile(
                getHALogDirC(), commitCounter1, IHALogReader.HA_LOG_EXT);

        if (!fileCLastCommit.exists())
            fail("HALog for last commit not found: " + fileCLastCommit);

        if (!fileCLastCommit.delete())
            fail("Could not remove HALog for last commit: " + fileCLastCommit);

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
        awaitCommitCounter(commitCounter2, new HAGlue[] { serverA, serverB });

        // Verify the expected #of HALogs on each service.
        awaitLogCount(getHALogDirA(), commitCounter2 + 1);
        awaitLogCount(getHALogDirB(), commitCounter2 + 1);
        awaitLogCount(getHALogDirC(), commitCounter1 - 1);

        // Verify HALog file for next commit point on A is NOT empty.
        {
            assertTrue(fileA.exists());
            final IHALogReader r = new HALogReader(fileA);
            assertFalse(r.isEmpty());
            assertFalse(r.isLive());
            r.close();
            assertTrue(fileA.exists());
        }

        // Verify HALog files for last and next commit point on C are missing.
        {
            assertFalse(fileC.exists());
            assertFalse(fileCLastCommit.exists());
        }

        /*
         * Restart (C).
         * 
         * Note: This restart should fail. The number of HALog files on (C)
         * should be unchanged.
         */

        // Restart C.
        {
            boolean ok = false;
            try {
                serverC = startC();
                ok = true;
            } catch (Throwable t) {
                if (InnerCause.isInnerCause(t, InterruptedException.class))
                    // test interrupted? propagate interrupt.
                    throw new RuntimeException(t);
                // log message. refused start is expected.
                log.warn("C refused to start: " + t, t);
            }
            if (ok)
                fail("C should not have restarted.");
        }
        
        /*
         * Verify the expected #of HALogs on each service.
         * 
         * Note: Each service will have an empty HALog for the next commit
         * point.
         */
        awaitLogCount(getHALogDirA(), commitCounter2 + 1);
        awaitLogCount(getHALogDirB(), commitCounter2 + 1);
        awaitLogCount(getHALogDirC(), commitCounter1 - 1);

    }
    
    /**
     * Unit test for a situation in which A B and C start. A quorum mets and the
     * third service resyncs with the met quorum. The quorum then fully meets.
     * Once the fully met quorum is stable, C is then restarted. This test
     * exercises a code path that handles the case where C is current, but is
     * forced into RESYNC in case there are writes in progress on the leader.
     * <p>
     * Note: In this version of the test, the HALog files are NOT purged at each
     * commit of the fully met quorum.
     */
    public void testStartABC_restartC() throws Exception {
        
        final ABC x = new ABC(true/*sequential*/);
        
        final long token = awaitFullyMetQuorum();
        
        // Now run several transactions
        final int NTX = 5;
        for (int i = 0; i < NTX; i++)
            simpleTransaction();

        // wait until the commit point is registered on all services.
        awaitCommitCounter(NTX + 1L, new HAGlue[] { x.serverA, x.serverB,
                x.serverC });
        
        /*
         * The same number of HALog files should exist on all services.
         * 
         * Note: the restore policy is setup such that we are NOT purging the HALog
         * files at each commit of a fully met quorum.
         */
        awaitLogCount(getHALogDirA(), NTX + 2L);
        awaitLogCount(getHALogDirB(), NTX + 2L);
        awaitLogCount(getHALogDirC(), NTX + 2L);
        
        // shutdown C.
        shutdownC();
        
        // wait for C to be gone from zookeeper.
        awaitPipeline(new HAGlue[] { x.serverA, x.serverB });
        awaitMembers(new HAGlue[] { x.serverA, x.serverB });
        awaitJoined(new HAGlue[] { x.serverA, x.serverB });
        
        // restart C.
        /*final HAGlue serverC =*/ startC();
        
        // wait until the quorum fully meets again (on the same token).
        assertEquals(token, awaitFullyMetQuorum());

        // Verify expected HALog files.
        awaitLogCount(getHALogDirA(), NTX + 2L);
        awaitLogCount(getHALogDirB(), NTX + 2L);
        awaitLogCount(getHALogDirC(), NTX + 2L);
        
    }

    /**
     * Unit test for a situation in which A B and C start. A quorum mets and the
     * third service resyncs with the met quorum. The quorum then fully meets.
     * Once the fully met quorum is stable, B is then restarted. The pipeline is
     * reorganized when B is shutdown but the quorum does not break. This test
     * exercises a code path that handles the case where B is current, but is
     * forced into RESYNC in case there are writes in progress on the leader.
     * <p>
     * Note: In this version of the test, the HALog files are NOT purged at each
     * commit of the fully met quorum.
     */
    public void testStartABC_restartB() throws Exception {
        
        final ABC x = new ABC(true/*sequential*/);
        
        final long token = awaitFullyMetQuorum();
        
        // Now run several transactions
        final int NTX = 5;
        for (int i = 0; i < NTX; i++)
            simpleTransaction();

        // wait until the commit point is registered on all services.
        awaitCommitCounter(NTX + 1L, new HAGlue[] { x.serverA, x.serverB,
                x.serverC });
        
        /*
         * The same number of HALog files should exist on all services.
         * 
         * Note: the restore policy is setup such that we are purging the HALog
         * files at each commit of a fully met quorum.
         */
        awaitLogCount(getHALogDirA(), NTX + 2L);
        awaitLogCount(getHALogDirB(), NTX + 2L);
        awaitLogCount(getHALogDirC(), NTX + 2L);
        
        // shutdown B.
        shutdownB();
        
        // wait for B to be gone from zookeeper.
        awaitPipeline(new HAGlue[] { x.serverA, x.serverC });
        awaitMembers(new HAGlue[] { x.serverA, x.serverC });
        awaitJoined(new HAGlue[] { x.serverA, x.serverC });
        
        // restart B.
        /*final HAGlue serverB =*/ startB();
        
        // wait until the quorum fully meets again (on the same token).
        assertEquals(token, awaitFullyMetQuorum());

        // Verify expected HALog files.
        awaitLogCount(getHALogDirA(), NTX + 2L);
        awaitLogCount(getHALogDirB(), NTX + 2L);
        awaitLogCount(getHALogDirC(), NTX + 2L);
        
    }

    /**
     * Unit test for a situation in which A B and C start. A quorum mets and the
     * third service resyncs with the met quorum. The quorum then fully meets.
     * Once the fully met quorum is stable, A is then restarted. The pipeline is
     * reorganized when A is shutdown and a new leader is elected. This test
     * exercises a code path that handles the case where A is current, but is
     * forced into RESYNC in case there are writes in progress on the leader.
     * <p>
     * Note: In this version of the test, the HALog files are NOT purged at each
     * commit of the fully met quorum.
     */
    public void testStartABC_restartA() throws Exception {
        
        final ABC x = new ABC(true/*sequential*/);
        
        final long token = awaitFullyMetQuorum();
        
        // Now run several transactions
        final int NTX = 5;
        for (int i = 0; i < NTX; i++)
            simpleTransaction();

        // wait until the commit point is registered on all services.
        awaitCommitCounter(NTX + 1L, new HAGlue[] { x.serverA, x.serverB,
                x.serverC });
        
        /*
         * The same number of HALog files should exist on all services.
         * 
         * Note: the restore policy is setup such that we are NOT purging the HALog
         * files at each commit of a fully met quorum.
         */
        awaitLogCount(getHALogDirA(), NTX + 2L);
        awaitLogCount(getHALogDirB(), NTX + 2L);
        awaitLogCount(getHALogDirC(), NTX + 2L);
        
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
        
    }

    /**
     * Three services are started in [A,B,C] order. B is setup for
     * {@link HACommitGlue#prepare2Phase(IHA2PhasePrepareMessage)} to throw an
     * exception inside of the commit2Phase() method rather than at the external
     * RMI interface.
     * <p>
     * A simple transaction is performed. We verify that the transaction
     * completes successfully, that the quorum token is unchanged, and that
     * [A,C] both participated in the commit. We also verify that B is moved to
     * the end of the pipeline (by doing a serviceLeave and then re-entering the
     * pipeline) and that it resyncs with the met quorum and finally re-joins
     * with the met quorum. The quorum should not break across this test.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/760" >
     *      Review commit2Phase semantics when a follower fails </a>
     * 
     * @see TestHAJournalServerOverride#testStartABC_commit2Phase_B_failCommit_beforeWritingRootBlockOnJournal_HALogsPurgedAtCommit()
     * 
     *      TODO There should probably be different tests to examine how the HA
     *      cluster handles the case where we have fewer than the required
     *      number of services that correctly perform the commit. We should be
     *      able to write tests that actually cause the HA cluster meet on the
     *      previous commit point (majority fail to commit), the new commit
     *      point (majority commit), or on NO commit point (bare majority with
     *      3rd service at an earlier commit point, 2 services vote YES and one
     *      fails before writing the root block on the journal - this last case
     *      should cause generate an alert, even if only because the HA cluster
     *      is unable to form a quorum).
     * 
     *      TODO Write test where commit2Phase() fails after writing the root
     *      block on the journal but before writing the root block on the HALog.
     *      Use this to explore what can happen when the live HALog file is not
     *      properly closed. The journal will be at a commit point in advance of
     *      the most recent HALog file. It needs to either reach to the quorum
     *      and recover the root block (and any subsequent commits) from the
     *      leader. Note that it could cure the missing HALog root block locally
     *      as well, but better to find a common pattern.
     * 
     *      TODO Develop annotations on the commit2Phase protocol diagram that
     *      show the different tests that we have to examine the failure modes.
     * 
     *      TODO Explore GATHER failure modes.
     * 
     *      TODO Test failover reads.
     */
    public void testStartABC_commit2Phase_B_failCommit_beforeWritingRootBlockOnJournal_HALogsNotPurgedAtCommit()
            throws Exception {

        // Enforce the join order.
        final ABC startup = new ABC(true /*sequential*/);

        //HAJournalTest.dumpThreads();
        
        final long token = awaitFullyMetQuorum();

        // Should be one commit point.
        awaitCommitCounter(1L, startup.serverA, startup.serverB,
                startup.serverC);

        /*
         * Setup B to fail the "COMMIT" message (specifically, it will throw
         * back an exception rather than executing the commit.
         */
        ((HAGlueTest) startup.serverB)
                .failCommit_beforeWritingRootBlockOnJournal();

        /*
         * Simple transaction.
         * 
         * Note: B will fail the commit without laying down the root block and
         * will transition into the ERROR state. From there, it will move to
         * SeekConsensus and then RESYNC. While in RESYNC it will pick up the
         * missing HALog and commit point. Finally, it will transition into
         * RunMet.
         */
        simpleTransaction();

        // Verify quorum is unchanged.
        assertEquals(token, quorum.token());

        // Should be two commit points on {A,C}.
        awaitCommitCounter(2L, startup.serverA, startup.serverC);

        /*
         * Just one commit point on B
         * 
         * TODO This is a data race. It is only transiently true.
         */
        awaitCommitCounter(1L, startup.serverB);

        /*
         * B is NotReady
         * 
         * TODO This is a data race. It is only transiently true.
         */
        awaitHAStatus(startup.serverB, HAStatusEnum.NotReady);

        /*
         * The pipeline should be reordered. B will do a service leave, then
         * enter seek consensus, and then re-enter the pipeline.
         */
        awaitPipeline(new HAGlue[] { startup.serverA, startup.serverC,
                startup.serverB });

        awaitFullyMetQuorum();

        /*
         * There should be two commit points on {A,C,B} (note that this assert
         * does not pay attention to the pipeline order).
         */
        awaitCommitCounter(2L, startup.serverA, startup.serverC,
                startup.serverB);

        // B should be a follower again.
        awaitHAStatus(startup.serverB, HAStatusEnum.Follower);

        // quorum token is unchanged.
        assertEquals(token, quorum.token());

    }

}

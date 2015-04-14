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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Calendar;
import java.util.Iterator;
import java.util.UUID;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;

/**
 * FIXME This test suite has known limitations and the utility class that it
 * tests needs a code review and revision.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestHA3DumpLogs extends AbstractHA3JournalServerTestCase {

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
                "com.bigdata.journal.jini.ha.HAJournalServer.restorePolicy=new com.bigdata.journal.jini.ha.DefaultRestorePolicy(0L,1,0)",
                "com.bigdata.journal.jini.ha.HAJournalServer.snapshotPolicy=new com.bigdata.journal.jini.ha.DefaultSnapshotPolicy("+neverRun+",0)",
                "com.bigdata.journal.jini.ha.HAJournalServer.onlineDisasterRecovery=true"
        };
        
    }

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

    public TestHA3DumpLogs() {
		
	}
	
    public TestHA3DumpLogs(final String name) {
        super(name);
    }

    public void testSimpleDumpLogs() throws Exception {
        // only start 2 services to ensure logs are maintained
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        awaitMetQuorum();
        awaitHAStatus(serverA, HAStatusEnum.Leader);
        awaitHAStatus(serverB, HAStatusEnum.Follower);
        final UUID leaderId = quorum.getLeaderId();
        final HAGlue leader = quorum.getClient().getService(leaderId);
        awaitNSSAndHAReady(leader);
        awaitCommitCounter(1L, new HAGlue[] { serverA, serverB });
        
        // Run through a few transactions to generate some log files
        simpleTransaction();       
        simpleTransaction();        
        simpleTransaction();
        
        log.warn("After 3 met quorum transactions");
        showLogs();
        
        // now start third
        startC();
        awaitHAStatus(serverB, HAStatusEnum.Follower);
        
        awaitFullyMetQuorum();
        
        log.warn("After full quorum");
        showLogs();
        
        simpleTransaction();
        
        log.warn("After full quorum commit");
        showLogs();

    }
    
    public void testBatchDumpLogs() throws Exception {
        // only start 2 services to ensure logs are maintained
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        awaitMetQuorum();
    	
        final UUID leaderId = quorum.getLeaderId();
        final HAGlue leader = quorum.getClient().getService(leaderId);
        awaitNSSAndHAReady(leader);
        awaitCommitCounter(1L, new HAGlue[] { serverA, serverB });

        // Run through a few transactions to generate some log files
        // Ensure that services use multiple batches
        for (int t = 0; t < 200; t++) {
            simpleTransaction();
        }
        
        log.warn("After 200 met quorum transactions");
        
//        log.warn("Remove a couple of log files to force delta");        
//        Thread.sleep(20000);
        
        mainShowLogs(true/*summary*/);
        
        // now start third
        startC();
        
        awaitFullyMetQuorum();
        
        log.warn("After full quorum");
        mainShowLogs(true/*summary*/);
        
        simpleTransaction();
        
        log.warn("After full quorum commit");
        mainShowLogs(false/*summary*/);

    }
    
    /**
     * Enable simple testing of DumpLogDigests utility
     */
    private void showLogs() throws Exception {
        final DumpLogDigests dlds = new DumpLogDigests(
        	new String[] {
                	SRC_PATH + "dumpFile.config"
        	}	
        );
        try {
	        final StringWriter sw = new StringWriter();
	        final PrintWriter pw = new PrintWriter(sw);
	        final String logicalServiceId = getLogicalServiceId();
	        final Iterator<DumpLogDigests.ServiceLogs> serviceLogInfo = dlds.dump(logicalServiceId /*logicalServiceZPath*/, 20/*batchlogs*/, 5/*serviceThreads*/);
	        
	        while (serviceLogInfo.hasNext()) {
	        	DumpLogDigests.ServiceLogs info  = serviceLogInfo.next();
	        	pw.println(info);
	        }
	        
	        pw.flush(); 
	        
	        log.warn(sw.toString());
	       
        } finally {
        	dlds.shutdown();
        }
    }

    /**
     * Tests main entry for DumpLogDigests
     * @param summary 
     */
    private void mainShowLogs(boolean summary) throws Exception {
        final String logicalServiceId = getLogicalServiceId();
        DumpLogDigests.main(
        	new String[] {
                	SRC_PATH + "dumpFile.config",
                	logicalServiceId /*logicalServiceZPath*/,
                	(summary ? "summary" : "full")
        	}	
        );
    }


}

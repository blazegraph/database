/**

Copyright (C) SYSTAP, LLC 2006-2013.  All rights reserved.

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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;

public class TestHA3DumpLogs extends AbstractHA3JournalServerTestCase {

    public void testSimpleDumpLogs() throws Exception {
        // only start 2 services to ensure logs are maintained
    	startA();
    	startB();
    	
    	awaitMetQuorum();

        // Run through a few transactions to generate some log files
        simpleTransaction();       
        simpleTransaction();        
        simpleTransaction();
        
        log.warn("After 3 met quorum transactions");
        showLogs();
        
        // now start third
        startC();
        
        awaitFullyMetQuorum();
        
        log.warn("After full quorum");
        showLogs();
        
        simpleTransaction();
        
        log.warn("After full quorum commit");
        showLogs();

    }
    
    public void testBatchDumpLogs() throws Exception {
        // only start 2 services to ensure logs are maintained
    	startA();
    	startB();
    	
    	awaitMetQuorum();

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
	        
	        final Iterator<DumpLogDigests.ServiceLogs> serviceLogInfo = dlds.dump(logicalServiceZPath, 20/*batchlogs*/, 5/*serviceThreads*/);
	        
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
     * @param b 
     */
    private void mainShowLogs(boolean summary) throws Exception {
        DumpLogDigests.main(
        	new String[] {
                	SRC_PATH + "dumpFile.config",
                	logicalServiceZPath,
                	(summary ? "summary" : "full")
        	}	
        );
    }


}

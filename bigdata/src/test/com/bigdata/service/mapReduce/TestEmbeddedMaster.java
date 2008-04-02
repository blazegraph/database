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
/*
 * Created on Sep 26, 2007
 */

package com.bigdata.service.mapReduce;

import java.util.Properties;

import junit.framework.TestCase;

import com.bigdata.journal.BufferMode;
import com.bigdata.service.EmbeddedClient;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.EmbeddedClient.Options;
import com.bigdata.service.mapred.AbstractMaster;
import com.bigdata.service.mapred.EmbeddedMaster;
import com.bigdata.service.mapred.MapReduceJob;
import com.bigdata.service.mapred.jobs.CountKeywordJob;

/**
 * Test suite for {@link EmbeddedMaster}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestEmbeddedMaster extends TestCase {

    /**
     * 
     */
    public TestEmbeddedMaster() {
        super();
    }

    /**
     * @param arg0
     */
    public TestEmbeddedMaster(String arg0) {
        super(arg0);
    }

    IBigdataClient client;

    /**
     * Setup the client that will connect to (embedded) federation on which the
     * intermediate results will be written.
     */
    public void setUp() throws Exception {
        
        super.setUp();

        // inherit system properties.
        Properties properties = new Properties(System.getProperties());

        // Note: when using disk use temp files so that the reduce stores
        // do not survive restart.
        properties.setProperty(Options.CREATE_TEMP_FILE, Boolean.TRUE
                .toString());

        // Note: Option does not buffer data in RAM.
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());

        // Note: No disk at all, but consumes more RAM to buffer the data.
//        properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient.toString());

//        // Note: option iff using a disk-backed store
//        properties
//                .setProperty(Options.FORCE_ON_COMMIT, ForceEnum.No.toString());

        /*
         * #of data services to run in the federation.
         * 
         * Note: At most one data service will be used per reduce fan in (N).
         * Excess data services will not be used. There should not be much
         * overhead to unused data services since even the fully buffered modes
         * extend the buffer in response to use.
         */
        properties
                .setProperty(Options.NDATA_SERVICES, "1");

        // Create the federation.
        client = new EmbeddedClient(properties);

        client.connect();

    }

    /**
     * Disconnect from the federation.
     */
    public void tearDown() throws Exception {
        
        if (client != null)
            client.disconnect(true/* immediateShutdown */);
        
        super.tearDown();
        
    }
    
    /**
     * Run a map/reduce job using embedded services.
     */
    public void testCountKeywords() {

        /*
         * FIXME  Map processing is faster when N is smaller - why?
         * 
         * Given M=100
         * 
         * N = 1 : 12 seconds maptime; 2 seconds reduce time.
         * N = 2 : 16 seconds maptime; 2 seconds reduce time.
         * N = 3 : 23 seconds maptime; 3 seconds reduce time.
         * N = 4 : 29 seconds maptime; 3 seconds reduce time.
         * 
         * This is measured with 2 data services and bufferMode=Transient.
         * 
         * With bufferMode=Disk and forceOnCommit=No the results are:
         * 
         * N = 1 : 23 seconds maptime; 3 seconds reduce time.
         * N = 2 : 19 seconds maptime; 3 seconds reduce time.
         * N = 3 : 25 seconds maptime; 3 seconds reduce time.
         * N = 4 : 28 seconds maptime; 3 seconds reduce time.
         * 
         * which is even stranger.
         * 
         * I would expect the average write queue length on the data
         * services for the reduce partitions to be M/P, where P is
         * min(#of data services available,N) (or P=N if using group
         * commit).  So, if N = P = 1 and M = 100 then 100 tasks will
         * be trying to write on 1 data service concurrently.  However,
         * the #s above appear to go the other way.
         * 
         * Ok. The answers to the above observations are: (a) we write 
         * smaller batches on each partition as there are more partitions.
         * If we extract on the order of 1000 keywords per task and have
         * 10 partitions then a map task writes 100 keywords per partition.
         * Smaller writes are more expensive; (b) we are doing one commit
         * per map task per partition - more commits are more expensive;
         * (c) each commit makes the nodes and leaves of the btree immutable,
         * so we are doing more (de-)serialization of nodes and leaves.
         * 
         * Group commit will fix most of these issues - perhaps all.  However
         * there is going to be a practical limit on the #of reduce tasks to
         * run based on the expected #of tuples output per map task.  At some
         * point each map task will write zero or one tuples per reduce task.
         * Still, that might be necessary for very large map fan ins.  Consider
         * if you have billions of documents to process.  Each document is still
         * going to be very small, but the #of reduce partitions must be high
         * enough to make the intermediate files fit in local storage and to
         * make the reduce computation not too long.  The #of reduce tasks may
         * also be set by the #of machines that you are going to use to serve
         * the resulting data, e.g., a cluster of 100 machines serving a large
         * text index.
         * 
         * Note: With the new ConcurrentJournal I am able to run larger M and N
         * up to 10 without too much of an impact.  However, the overall performance
         * numbers are somewhat lower - I need to review this again after a
         * reboot.
         */
        MapReduceJob job = new CountKeywordJob(100/* m */, 10/* n */);
//      MapReduceJob job = new CountKeywordJob(1/* m */, 1/* n */);

        // non-zero to submit no more than this many map inputs.
//        job.setMaxMapTasks( 10 );

        // the timeout for a map task.
//        job.setMapTaskTimeout(2*1000/*millis*/);

        // the timeout for a reduce task.
//        job.setReduceTaskTimeout(2*1000/*millis*/);

        // the maximum #of times a map task will be retried (zero disables retry).
//        job.setMaxMapTaskRetry(3);

        // the maximum #of times a reduce task will be retried (zero disables retry).
//        job.setMaxReduceTaskRetry(3);

        /*
         * Run the map/reduce operation.
         * 
         * Note: This is using embedded services to test map/reduce.
         */
        
        AbstractMaster master = new EmbeddedMaster(job, client).run(.9d, .9d);
        
        System.out.println( master.status() );

        assertTrue("map problem?",master.getMapPercentSuccess()>0.9);

        assertTrue("reduce problem?", master.getReducePercentSuccess()>0.9);
        
    }

}

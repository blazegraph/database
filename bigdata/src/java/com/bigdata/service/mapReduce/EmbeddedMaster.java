package com.bigdata.service.mapReduce;

import java.util.Properties;
import java.util.UUID;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ForceEnum;
import com.bigdata.service.EmbeddedBigdataClient;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.EmbeddedBigdataFederation.Options;
import com.bigdata.service.mapReduce.MapService.EmbeddedMapService;
import com.bigdata.service.mapReduce.ReduceService.EmbeddedReduceService;

/**
 * A master running with embedded map and reduce services that may be used
 * for testing either the master and services or the execution of a specific
 * job.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EmbeddedMaster extends AbstractMaster {

    /**
     * 
     * @param job
     *            The map/reduce job to execute.
     * @param client
     *            The client used to read/write data stored in a federation.
     */
    public EmbeddedMaster(MapReduceJob job, IBigdataClient client) {

        super(job, client);

    }

    protected void setUp() {

        /**
         * Since we are running everything in process, this is the #of map
         * services to be start. The actual #of map tasks to run is based on
         * the #of input files and is discovered dynamically. The #of map
         * services to be started is based on an expectation that we will
         * distribute 100 concurrent map tasks to each map service. Since we
         * do not know the fan in (the #of input files) we are not able to
         * tell how many sets of map tasks will be run through the map
         * services before the input has been consumed.
         */
        final int numMapServicesToStart = 1; //Math.max(1, job.m / 100);

        /**
         * Since we are running everything in process, this is the #of
         * reduce services to start. The #of reduce tasks to be run is given
         * by job.n - this is a fixed input parameter from the user.
         */
        final int numReduceServicestoStart = 1;// Math.max(1, job.n/10);

        /**
         * The map services. Each services is capable of running map tasks
         * in parallel.
         */
        mapServices = new IJobAndTaskService[numMapServicesToStart];
        {

            // client properties.
            Properties properties = new Properties();

            for (int i = 0; i < numMapServicesToStart; i++) {

                mapServices[i] = new EmbeddedMapService(UUID.randomUUID(),
                        properties, client);

            }

        }

        /**
         * The reduce services. Each service is capable of running reduce
         * tasks in parallel.
         */
        reduceServices = new IJobAndTaskService[numReduceServicestoStart];
        {

            // client properties.
            Properties properties = new Properties();

            for (int i = 0; i < numReduceServicestoStart; i++) {

                reduceServices[i] = new EmbeddedReduceService(
                        UUID.randomUUID(), properties, client);

            }

        }

        super.setUp();

    }
    
    /**
     * Run a map/reduce task.
     * 
     * @param args
     */
    public static void main(String[] args) {

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
         */
        MapReduceJob job = new CountKeywordJob(100/* m */, 1/* n */);
//      MapReduceJob job = new CountKeywordJob(1/* m */, 1/* n */);

        // non-zero to submit no more than this many map inputs.
        job.setMaxMapTasks( 10 );

        // the timeout for a map task.
//        job.setMapTaskTimeout(2*1000/*millis*/);

        // the timeout for a reduce task.
//        job.setReduceTaskTimeout(2*1000/*millis*/);

        // the maximum #of times a map task will be retried (zero disables retry).
//        job.setMaxMapTaskRetry(3);

        // the maximum #of times a reduce task will be retried (zero disables retry).
//        job.setMaxReduceTaskRetry(3);

        /**
         * Setup the client that will connect to (embedded) federation on which
         * the intermediate results will be written.
         */
        final IBigdataClient client;
        {

            // inherit system properties.
            Properties properties = new Properties(System.getProperties());

            // Note: when using disk use temp files so that the reduce stores
            // do not survive restart.
            properties.setProperty(Options.CREATE_TEMP_FILE, Boolean.TRUE
                    .toString());

            // Note: Option does not buffer data in RAM.
            properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
                    .toString());

            // Note: No disk at all, but consumes more RAM to buffer the data.
            properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient
                    .toString());

            /*
             * #of data services to run in the federation.
             * 
             * Note: At most one data service will be used per reduce fan in
             * (N). Excess data services will not be used. There should not be
             * much overhead to unused data services since even the fully
             * buffered modes extend the buffer in response to use.
             */
            properties.setProperty(Options.NDATA_SERVICES, ""
                    + Math.min(10, job.n));

            // Note: Turn on if testing group commit performance.
            properties.setProperty(Options.FORCE_ON_COMMIT, ForceEnum.No
                    .toString());

            // Create the federation.
            client = new EmbeddedBigdataClient(properties);

            client.connect();

        }

        /*
         * Run the map/reduce operation.
         * 
         * Note: This is using embedded services to test map/reduce.
         */
        System.out.println(new EmbeddedMaster(job, client).run(.9d, .9d)
                .status());

        /*
         * Disconnect from the federation.
         */

        client.terminate();

    }

}

package com.bigdata.service.mapReduce;

import java.util.Properties;
import java.util.UUID;

import com.bigdata.service.IBigdataClient;
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

}

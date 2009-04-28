package com.bigdata.service.jini.master;

import java.rmi.Remote;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceRegistrar;

import com.bigdata.service.IRemoteExecutor;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.lookup.BigdataCachingServiceClient;
import com.bigdata.service.jini.master.TaskMaster.JobState;

/**
 * Class discovers and returns services matching a {@link ServicesTemplate}.
 * A number of instances of this class are submitted in parallel to verify
 * that the pre-conditions for the {@link JobState} are satisified and to
 * return the {@link ServiceItem}s for the {@link IRemoteExecutor}s to
 * which the client tasks will be distributed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DiscoverServices implements Callable<ServiceItem[]> {

	protected static final transient Logger log = Logger.getLogger(DiscoverServices.class);
	
    private final JiniFederation fed;

    private final ServicesTemplate servicesTemplate;

    private final long timeout;

    /**
     * 
     * @param fed
     *            The {@link JiniFederation}, which is already handling
     *            {@link ServiceRegistrar} discovery.
     * @param servicesTemplate
     *            The template to be matching, including the minimum #of
     *            matches to be made.
     * @param timeout
     *            The timeout in milliseconds to await the minimum #of
     *            matching services.
     */
    public DiscoverServices(final JiniFederation fed,
            final ServicesTemplate servicesTemplate, final long timeout) {

        if (fed == null)
            throw new IllegalArgumentException();

        if (servicesTemplate == null)
            throw new IllegalArgumentException();

        if (timeout <= 0)
            throw new IllegalArgumentException();

        this.fed = fed;

        this.servicesTemplate = servicesTemplate;

        this.timeout = timeout;

    }

    /**
     * Return all matching services, waiting no longer than the timeout
     * specified to the ctor.
     * 
     * @return All matching services. The caller can use as many as they
     *         desire and is responsible for handling the case when fewer
     *         services were matched than were requested.
     */
    public ServiceItem[] call() throws Exception {

        /*
         * Setup cached discovery for services matching the template and
         * filter.
         * 
         * Note: The cache miss timeout is ignored since we will not use
         * lookup by ServiceID here.
         */
        final BigdataCachingServiceClient<Remote> serviceClient = new BigdataCachingServiceClient<Remote>(
                fed, Remote.class/* serviceIfaceIsForLoggingOnly */,
                servicesTemplate.template, servicesTemplate.filter, 1000/* cacheMissTimeout */) {

        };

        final long begin = System.currentTimeMillis();

        int serviceCount;
        long elapsed;

        while ((serviceCount = serviceClient.getServiceCache()
				.getServiceCount()) < servicesTemplate.minMatches
				&& (elapsed = (System.currentTimeMillis() - begin)) < timeout) {

            final long remaining = timeout - elapsed;

            if (log.isDebugEnabled())
				log.debug("Discovered " + serviceCount + " : elapsed=" + elapsed
						+ ", template=" + servicesTemplate);
            
            // sleep a bit to await further service discovery.
            Thread.sleep(remaining < 100 ? remaining : 100);

        }

        /*
         * Return all discovered services which matched the template.
         */
        final ServiceItem[] a = serviceClient.getServiceCache()
				.getServiceItems(0/* maxCount */, null/* filter */);

        if (log.isInfoEnabled())
			log.info("Discovered " + serviceCount + " : template=" + servicesTemplate);

        return a;
        
    }

}

/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Sep 20, 2008
 */

package com.bigdata.service.jini;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceItem;
import net.jini.lookup.entry.Name;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.jini.lookup.entry.Hostname;
import com.bigdata.journal.DumpJournal;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.service.DataService;
import com.bigdata.service.IDataService;
import com.bigdata.service.ListIndicesTask;
import com.bigdata.service.MetadataService;
import com.bigdata.util.InnerCause;

/**
 * A client utility that connects to and dumps various interesting aspects of a
 * live federation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see DumpJournal
 * 
 * @todo dump by data service as well, showing the indices on the ds and the
 *       disk space allocated to the resource manager for the ds (depending on
 *       the release age there may be historical views preserved).
 * 
 * @todo modify to use the {@link Hostname} attribute in the {@link Entry}[]
 *       from the {@link ServiceItem}.
 */
public class DumpFederation {

    protected static final Logger log = Logger.getLogger(DumpFederation.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    /**
     * Dumps interesting things about the federation.
     * <p>
     * <strong>Jini MUST be running</strong>
     * <p>
     * <strong>You MUST specify a sufficiently lax security policy</strong>,
     * e.g., using <code>-Djava.security.policy=policy.all</code>, where
     * <code>policy.all</code> is the name of a policy file.
     * 
     * @param args
     *            The name of the configuration file for the jini client that
     *            will be used to connect to the federation.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws IOException
     * @throws TimeoutException
     *             if no {@link DataService} can be discovered.
     */
    static public void main(final String[] args) throws InterruptedException,
            ExecutionException, IOException, TimeoutException {

        if (args.length == 0) {

            System.err.println("usage: <client-config-file>");

            System.exit(1);

        }

        final JiniClient client;

        JiniServicesHelper helper = null;
        if (false) {// @todo command line option for this.

            /*
             * Use the services helper to (re-)start an embedded jini
             * federation.
             */
            helper = new JiniServicesHelper(new File(args[0]).getParent()
                    .toString()
                    + File.separator);

            helper.start();

            client = helper.client;

        } else {
            
            /*
             * Connect to an existing jini federation.
             */

            client = JiniClient.newInstance(args);

        }

        final JiniFederation fed = client.connect();

        try {

            /*
             * Wait until we have the metadata service.
             */
            fed.awaitServices(1/* minDataServices */, 10000L/* timeout(ms) */);

            final long lastCommitTime = fed.getLastCommitTime();
            
            if(lastCommitTime == 0L) {
                
                System.out.println("No committed data.");
                
                System.exit(0);
                
            }
            
            // a read-only transaction as of the last commit time.
            final long tx = fed.getTransactionService().newTx(lastCommitTime);
            
            try {

                final DumpFederation dumper = new DumpFederation(fed, tx);

                final String[] names = dumper.getIndexNames();

                System.out.println("Found " + names.length + " indices: "
                        + Arrays.toString(names));

                // @todo command line option.
                final boolean dumpIndexLocators = true;

                if (dumpIndexLocators) {

                    for (String name : names) {

                        // strip off the prefix to get the scale-out index name.
                        final String scaleOutIndexName = name
                                .substring(MetadataService.METADATA_INDEX_NAMESPACE
                                        .length());

                        dumper.dumpIndexLocators(scaleOutIndexName);

                    }

                }

            } finally {

                // discard read-only transaction.
                fed.getTransactionService().abort(tx);

            }

            System.out.println("Done.");

        } finally {

            if (helper != null) {

                helper.shutdown();

            } else {

                client.disconnect(false/* immediateShutdown */);

            }

        }
        
    }

    private final JiniFederation fed;

    private final long commitTime;
    
    public DumpFederation(final JiniFederation fed, final long commitTime) {
        
        this.fed = fed;
        
        this.commitTime = commitTime;
        
    }
    
    /**
     * The names of all registered scale-out indices.
     * 
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     * 
     */
    public String[] getIndexNames() throws InterruptedException,
            ExecutionException, IOException {

        return (String[]) fed.getMetadataService().submit(
                new ListIndicesTask(commitTime)).get();

    }

    /**
     * Dumps the {@link PartitionLocator}s for the named index.
     * 
     * @param name
     *            The name of a scale-out index.
     */
    public void dumpIndexLocators(final String name) {

        final long timestamp = TimestampUtility.asHistoricalRead(commitTime);

        final IMetadataIndex ndx;
        try {

            ndx = fed.getMetadataIndex(name, timestamp);

        } catch (Throwable t) {

            final Throwable t2 = InnerCause.getInnerCause(t,
                    ClassNotFoundException.class);

            if (t2 != null) {

                log.error("CODEBASE/CLASSPATH problem:", t2);

                return;

            }

            throw new RuntimeException(t);

        }

        dumpIndexLocators(name, ndx);

    }
    
    /**
     * Dumps the locators for a scale-out index.
     * 
     * @param name
     *            The name of the scale-out index.
     * @param metadataIndex
     *            The scale-out index.
     */
    protected void dumpIndexLocators(final String name,
            final IMetadataIndex metadataIndex) {

        System.out.println("\n\nname=" + name);

        final ITupleIterator<PartitionLocator> itr = metadataIndex.rangeIterator();

        PartitionLocator lastLocator = null;
        
        while (itr.hasNext()) {

            final PartitionLocator locator = itr.next().getObject();

            System.out.println("\t"+locator.toString());

            // logical data service UUID.
            final UUID uuid = locator.getDataServiceUUID();

            final ServiceMetadata smd = getServiceMetadata(uuid);
            
            /*
             * Verify some constraints on the index partition separator keys.
             */
            {
             
                if (lastLocator == null) {

                    if (locator.getLeftSeparatorKey() == null
                            || locator.getLeftSeparatorKey().length != 0) {

                        log
                                .error("name="
                                        + name
                                        + " : Left separator should be [] for 1st index partition: "
                                        + locator);

                    }

                } else {

                    /*
                     * The leftSeparator of each index partition after the first
                     * should be equal to the rightSeparator of the previous
                     * index partition.
                     */
                    final int cmp = BytesUtil.compareBytes(lastLocator
                            .getRightSeparatorKey(), locator
                            .getLeftSeparatorKey());

                    if (cmp < 0) {

                        /*
                         * The rightSeparator of the prior index partition is LT
                         * the leftSeparator of the current index partition.
                         * This means that there is a gap between these index
                         * partitions (e.g., there is no index partition that
                         * covers keys which would fall into that gap).
                         */

                        log
                                .error("name="
                                        + name
                                        + " : Gap between index partitions: lastLocator="
                                        + lastLocator + ", thisLocator="
                                        + locator);

                    } else if (cmp > 0) {

                        /*
                         * The rightSeparator of the prior index partition is GT
                         * the leftSeparator of the current index partition.
                         * This means that the two index partitions overlap for
                         * at least part of their key range.
                         */

                        log.error("name=" + name
                                + " : Index partitions overlap: lastLocator="
                                + lastLocator + ", thisLocator=" + locator);

                    }

                }
            
            }

            /*
             * Range count the index partition. This will verify that the
             * view can be materialized.
             */
            String rangeCount;
            try {

                final IIndex ndx = fed.getIndex(name, TimestampUtility
                        .asHistoricalRead(commitTime));

                rangeCount = Long
                        .toString(ndx.rangeCount(locator.getLeftSeparatorKey(),
                                locator.getRightSeparatorKey()));

            } catch (Throwable t) {

                log.error("name=" + name
                        + " : Could not range count index partition: "
                        + locator, t);

                rangeCount = t.getMessage();

            }

            System.out.println("\t\tDataService: label=" + smd.label
                    + ", uuid=" + uuid + ", rangeCount=" + rangeCount);

            lastLocator = locator;
            
        } // next index partition.

        /*
         * Verify a constraint on the last index partition.
         */
        if (lastLocator != null && lastLocator.getRightSeparatorKey() != null) {
            
            log.error("name="+name+" : Right separator of last index partition is not null: "
                            + lastLocator);
            
        }
        
    }

    /**
     * Service metadata extracted by {@link DumpFederation}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ServiceMetadata {

        private final UUID uuid;
        private final String hostname;
        private final String label;
        
        public ServiceMetadata(UUID uuid,String hostname,String label) {
            
            if (uuid == null)
                throw new IllegalArgumentException();
            
            if (hostname == null)
                throw new IllegalArgumentException();
            
            if (label == null)
                throw new IllegalArgumentException();

            this.uuid = uuid;
            
            this.hostname = hostname;
            
            this.label = label;

        }
        
        /**
         * The service {@link UUID}.
         */
        public UUID getUUID() {
            
            return uuid;
            
        }
        
        /**
         * The hostname of the machine on which the service is running.
         */
        public String getHostname() {
            
            return hostname;
            
        }
        
        /**
         * A label assigned to the service (easier to read than its {@link UUID}).
         */
        public String getLabel() {
         
            return label;
            
        }
        
    }
    
    /**
     * Return a label for the service that is easier for people to read than its
     * {@link UUID}. The assignment of the label to the service is stable for
     * the life of the {@link DumpFederation} instance.
     */
    synchronized ServiceMetadata getServiceMetadata(UUID uuid) {

        if (uuid == null)
            throw new IllegalArgumentException();

        final String hostname = getServiceHostName(uuid);

        final String label = getServiceName(uuid);

        return new ServiceMetadata(uuid, hostname, label);

    }

    /**
     * Return a name for the service.
     * <p>
     * Note: Normally this is a {@link Name} from the {@link ServiceItem}. When
     * no name is found a label is assembled from the hostname and a one up
     * counter.
     * 
     * @param uuid
     *            The service {@link UUID}.
     * 
     * @return A name associated with that service.
     */
    synchronized String getServiceName(UUID uuid) {

        String label = null;

        label = service2Label.get(uuid);
        
        if (label == null) {
            
            /*
             * Lookup the label on the attributes associated with the
             * ServiceItem for the service.
             */
            
            // lookup the service in the client's cache.
            final ServiceItem serviceItem = fed.dataServicesClient
                    .getServiceItem(uuid);

            if (serviceItem != null) {
         
                // scan the Entry[]
                final Entry[] attribs = serviceItem.attributeSets;

                for (Entry e : attribs) {

                    if (e instanceof Name) {

                        // found a name.
                        label = ((Name) e).name;

                    }

                }
                
            }
            
        }

        if (label == null) {

            /*
             * Generate a name since no label was discovered above.
             */
            
            final String hostname = getServiceHostName(uuid);

            label = hostname + "#" + serviceLabelCount++;

            service2Label.put(uuid, label);

        }

        return label;
        
    }
    private int serviceLabelCount = 0;
    private final Map<UUID, String> service2Label = new HashMap<UUID, String>();

    /**
     * Return the hostname of a service (caching).
     * 
     * @param uuid
     *            The service {@link UUID}.
     * 
     * @return The hostname.
     */
    synchronized String getServiceHostName(UUID uuid) {

        if (uuid == null)
            throw new IllegalArgumentException();

        String hostname = service2Hostname.get(uuid);

        if (hostname != null)
            return hostname;

        IDataService service = fed.getDataService(uuid);

        if (service == null) {

            return "<ServiceNotFound>";

        }
        
        try {

            hostname = service.getHostname();

        } catch (IOException ex) {

            return ex.getLocalizedMessage();

        }

        service2Hostname.put(uuid, hostname);

        return hostname;
        
    }
    private final Map<UUID, String> service2Hostname = new HashMap<UUID, String>();
    
}

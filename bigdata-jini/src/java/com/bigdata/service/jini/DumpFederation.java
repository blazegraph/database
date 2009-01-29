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
import java.io.Serializable;
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
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.jini.lookup.entry.Hostname;
import com.bigdata.journal.DumpJournal;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.StoreManager;
import com.bigdata.service.DataService;
import com.bigdata.service.IDataService;
import com.bigdata.service.IDataServiceAwareProcedure;
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

                dumper.dumpIndexLocators();

            } finally {

                // discard read-only transaction.
                fed.getTransactionService().abort(tx);

            }

            if(INFO)
                log.info("Done");

        } finally {

            if (helper != null) {

                helper.shutdown();

            } else {

                client.disconnect(false/* immediateShutdown */);

            }

        }
        
    }

    private final JiniFederation fed;

    /**
     * The read-historical transaction that will be used to dump the database.
     */
    private final long ts;
    
    /**
     * 
     * @param fed
     * @param tx
     *            The timestamp that will be used to dump the database. This
     *            SHOULD be a read-historical transaction since that will put a
     *            read-lock into place on the data during any operations by this
     *            class.
     */
    public DumpFederation(final JiniFederation fed, final long tx) {

        if (fed == null)
            throw new IllegalArgumentException();
        
        this.fed = fed;
        
        this.ts = tx;
        
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
                new ListIndicesTask(ts)).get();

    }

    /**
     * Dumps metadata for all named indices.
     * 
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     * 
     * @todo optional prefix or regex constraint on the scale-out index names or
     *       the key range of an index.
     */
    public void dumpIndexLocators() throws InterruptedException,
            ExecutionException, IOException {

        // write out table header.
        System.out
                .println("Timestamp"//
                        + "\tIndexName" //
                        + "\tPartitionId" //
                        + "\tServiceUUID" //
                        + "\tServiceName" //
                        + "\tHostname" //
                        + "\tServiceCode"//
                        //
                        + "\tSourceCount"//
                        + "\tRangeCount" //
                        + "\tRangeCountExact" //
                        + "\tSegmentBytes"// 
                        + "\tDataDirFreeSpace"// 
                        //
                        + "\tBytesUnderManagement"// 
                        + "\tJournalBytesUnderManagement"// 
                        + "\tIndexSegmentBytesUnderManagement"// 
                        + "\tManagedJournalCount"// 
                        + "\tManagedSegmentCount"//
                        + "\tOverflowCount"//
                        //
                        + "\tLeftSeparator"//
                        + "\tRightSeparator"//
                        + "\tHistory"//
                        );

        final String[] names = getIndexNames();

        if (INFO)
            log.info("Found " + names.length + " indices: "
                    + Arrays.toString(names));

        for (String name : names) {

            // strip off the prefix to get the scale-out index name.
            final String scaleOutIndexName = name
                    .substring(MetadataService.METADATA_INDEX_NAMESPACE
                            .length());

            dumpIndexLocators(scaleOutIndexName);

        }

    }
    
    /**
     * Dumps the {@link PartitionLocator}s for the named index.
     * 
     * @param indexName
     *            The name of a scale-out index.
     * 
     * @throws InterruptedException
     */
    public void dumpIndexLocators(final String indexName)
            throws InterruptedException {

        final IMetadataIndex ndx;
        try {

            ndx = fed.getMetadataIndex(indexName, ts);

        } catch (Throwable t) {

            final Throwable t2 = InnerCause.getInnerCause(t,
                    ClassNotFoundException.class);

            if (t2 != null) {

                log.error("CODEBASE/CLASSPATH problem:", t2);

                return;

            }

            throw new RuntimeException(t);

        }

        dumpIndexLocators(indexName, ndx);

    }
    
    /**
     * Container for a bunch of metadata extracted for an index partition
     * together with the methods required to extract that data.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class IndexPartitionRecord {

        public IndexPartitionRecord(final JiniFederation fed, final long ts,
                final String indexName, final PartitionLocator locator)
                throws InterruptedException {

            if (fed == null)
                throw new IllegalArgumentException();

            if (indexName == null)
                throw new IllegalArgumentException();

            if (locator == null)
                throw new IllegalArgumentException();

            this.indexName = indexName;

            this.locator = locator;
            
            smd = ServiceMetadata.getServiceMetadata(fed, locator
                    .getDataServiceUUID());
            
            final IDataService dataService = fed.getDataService(locator
                    .getDataServiceUUID());

            if (dataService == null) {

                /*
                 * There are lots of things that we can't do if we can't lookup
                 * the data service.
                 */
                throw new RuntimeException("Could not discover dataService: "
                        + dataService);

            }

            // fetch the LocalPartitionMetadata.
            LocalPartitionMetadata localPartitionMetadata = null;
            try {
                
                localPartitionMetadata = (LocalPartitionMetadata) dataService.submit(ts, DataService
                        .getIndexPartitionName(indexName, locator.getPartitionId()),
                        new FetchLocalPartitionMetadataTask());
                
            } catch (InterruptedException t) {
                
                throw t;
                
            } catch (Exception t) {
                
                log.warn("name=" + indexName + ", locator=" + locator, t);
                
            }
            this.localPartitionMetadata = localPartitionMetadata;

            // various byte counts of interest.
            IndexPartitionDetailRecord byteCountRec = null;
            try {

                byteCountRec = (IndexPartitionDetailRecord) dataService
                        .submit(ts, DataService.getIndexPartitionName(
                                indexName, locator.getPartitionId()),
                                new FetchIndexPartitionByteCountRecordTask()); 
                
            } catch(InterruptedException t) {
                
                throw t;
                
            } catch(Exception t) {

                log.warn("name=" + indexName, t);

            }
            this.detailRec = byteCountRec;
            
        }

        /**
         * The scale-out index name (from the ctor).
         */
        public final String indexName;

        /**
         * The index partition locator (from the ctor).
         */
        public final PartitionLocator locator;

        /**
         * Interesting metadata about the data service on which the index
         * partition is located.
         */
        public final ServiceMetadata smd;

        /**
         * The {@link LocalPartitionMetadata} for the index partition. This has
         * lots of interesting information.
         */
        public final LocalPartitionMetadata localPartitionMetadata;
        
        /**
         * The #of bytes across all {@link IndexSegment}s in the view.
         * <p>
         * Note: views generally have data on the live and possibly one (or
         * more) historical journals. However, there is no way to accurately
         * allocate the bytes on those journals to the indices on those
         * journals. The #of bytes under management for a {@link DataService}
         * may be examined using the performance counters reported for its
         * {@link StoreManager}.
         */
        public final IndexPartitionDetailRecord detailRec;

    }

    /**
     * Helper task returns the {@link LocalPartitionMetadata} for an index
     * partition.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class FetchLocalPartitionMetadataTask implements
            IIndexProcedure {

        /**
         * 
         */
        private static final long serialVersionUID = -482901101593128076L;

        public FetchLocalPartitionMetadataTask() {
        
        }

        public LocalPartitionMetadata apply(IIndex ndx) {

            return ndx.getIndexMetadata().getPartitionMetadata();
            
        }

        public boolean isReadOnly() {
            
            return true;
            
        }
        
    }
    
    /**
     * Encapsulates several different kinds of byte counts for the index
     * partition and the data service on which it resides.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class IndexPartitionDetailRecord implements Serializable {
       
        /**
         * 
         */
        private static final long serialVersionUID = 6275468354120307662L;

        /**
         * The #of resources in the view for the index partition.
         */
        public final int sourceCount;
        
        /**
         * The fast range count for the index partition.
         */
        public final long rangeCount;
        
        /**
         * The exact range count for the index partition (w/o deleted tuples).
         */
        public final long rangeCountExact;
        
        /**
         * The #of bytes across all {@link IndexSegment}s in the view.
         */
        public final long segmentByteCount;

        /**
         * The free space in bytes on the volume holding the data service's data
         * directory.
         */
        public final long dataDirFreeSpace;
        
        /**
         * The #of bytes being managed by the data service on which the index
         * partition resides.
         */
        public final long bytesUnderManagement;

        /**
         * The #of bytes found in journals managed by the data service on which
         * the index partition resides.
         */
        public final long journalBytesUnderManagement;

        /**
         * The #of bytes found in index segments managed by the data service on
         * which the index partition resides.
         */
        public final long segmentBytesUnderManagement;

        /**
         * The #of journals that are currently under management for the data
         * service on which the index partition resides.
         */
        public final int managedJournalCount;

        /**
         * The #of index segments that are currently under management for the
         * data service on which the index partition resides.
         */
        public final int managedSegmentCount;

        /**
         * The #of overflow events.
         */
        public final long overflowCount;
        
        public IndexPartitionDetailRecord(//
                final int sourceCount,//
                final long rangeCount,//
                final long rangeCountExact,//
                final long segmentByteCount,//
                final ResourceManager resourceManager//
        ) {

            this.rangeCount = rangeCount;
            
            this.rangeCountExact = rangeCountExact;
            
            this.sourceCount = sourceCount;
            
            this.segmentByteCount = segmentByteCount;
            
            this.dataDirFreeSpace = resourceManager.getDataDirFreeSpace();

            this.bytesUnderManagement = resourceManager
                    .getBytesUnderManagement();

            this.journalBytesUnderManagement = resourceManager
                    .getJournalBytesUnderManagement();

            this.segmentBytesUnderManagement = resourceManager
                    .getSegmentBytesUnderManagement();

            this.managedJournalCount = resourceManager.getManagedJournalCount();
            
            this.managedSegmentCount = resourceManager.getManagedSegmentCount();
            
            this.overflowCount = resourceManager.getOverflowCount();
            
        }

    }

    /**
     * Helper task returns various byte counts for an index partition and the
     * data service on which it resides.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class FetchIndexPartitionByteCountRecordTask implements
            IIndexProcedure, IDataServiceAwareProcedure {

        /**
         * 
         */
        private static final long serialVersionUID = 1656089893655069298L;

        public FetchIndexPartitionByteCountRecordTask() {

        }

        public IndexPartitionDetailRecord apply(final IIndex ndx) {

            if (dataService == null) {

                // data service was not set?
                throw new IllegalStateException();

            }

            System.err.println("ndx=" + ndx.getClass());

            final long rangeCount = ndx.rangeCount();

            final long rangeCountExact = ndx.rangeCountExact(
                    null/* fromKey */, null/* toKey */);
            
            final LocalPartitionMetadata pmd = ndx.getIndexMetadata()
                    .getPartitionMetadata();

            final ResourceManager resourceManager = dataService
                    .getResourceManager();

            long segmentByteCount = 0;

            int sourceCount = 0;
            
            if (pmd != null) {
                
                for (IResourceMetadata x : pmd.getResources()) {

                    sourceCount++;
                    
                    if (x.isJournal())
                        continue;

                    /*
                     * Note: This will force the (re-)open of the
                     * IndexSegmentStore. However, the store should already be
                     * open since we have an IIndex object and that should be a
                     * FusedView of its components.
                     * 
                     * @todo The IIndex is either a BTree or a FusedView so we
                     * can just enumerate its sources directly, which is much
                     * more straightforward.
                     */
                    final IndexSegmentStore segStore = (IndexSegmentStore) resourceManager
                            .openStore(x.getUUID());

                    /*
                     * Note: The size() of an IndexSegmentStore is always the
                     * length of the file. However, the #of bytes allocated by
                     * the OS to a file may differ depending on the block size
                     * for files on the volume.
                     */

                    segmentByteCount += segStore.size();

                }

            }
            
            return new IndexPartitionDetailRecord(sourceCount, rangeCount,
                    rangeCountExact, segmentByteCount, resourceManager);

        }

        public boolean isReadOnly() {
            
            return true;
            
        }

        public void setDataService(DataService dataService) {
            
            this.dataService = dataService;
            
        }
        private transient DataService dataService;
        
    }
        
    /**
     * Dumps useful information about the index partition in the context of the
     * data service on which it resides.
     * 
     * @param indexName
     *            The name of the scale-out index.
     * @param metadataIndex
     *            The scale-out index.
     */
    protected void dumpIndexLocators(final String indexName,
            final IMetadataIndex metadataIndex) throws InterruptedException {

        final ITupleIterator<PartitionLocator> itr = metadataIndex
                .rangeIterator();

        PartitionLocator lastLocator = null;

        while (itr.hasNext()) {

            final PartitionLocator locator = itr.next().getObject();

            final IndexPartitionRecord rec = new IndexPartitionRecord(fed, ts,
                    indexName, locator);

            /*
             * Verify some constraints on the index partition separator keys.
             */
            {
             
                if (lastLocator == null) {

                    if (locator.getLeftSeparatorKey() == null
                            || locator.getLeftSeparatorKey().length != 0) {

                        log
                                .error("name="
                                        + indexName
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
                                        + indexName
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

                        log.error("name=" + indexName
                                + " : Index partitions overlap: lastLocator="
                                + lastLocator + ", thisLocator=" + locator);

                    }

                }

            }

            lastLocator = locator;

            // format row for table.
            final StringBuilder sb = new StringBuilder();
            sb.append(ts);//new Date(ts));
            sb.append("\t" + indexName);
            sb.append("\t" + rec.locator.getPartitionId());
            sb.append("\t" + rec.locator.getDataServiceUUID());
            sb.append("\t" + rec.smd.getName());
            sb.append("\t" + rec.smd.getHostname());
            sb.append("\t" + "DS" + rec.smd.getCode());
            if (rec.detailRec != null) {
                sb.append("\t" + rec.detailRec.sourceCount);
                sb.append("\t" + rec.detailRec.rangeCount);
                sb.append("\t" + rec.detailRec.rangeCountExact);
                sb.append("\t" + rec.detailRec.segmentByteCount);
                sb.append("\t" + rec.detailRec.dataDirFreeSpace);
                
                sb.append("\t" + rec.detailRec.bytesUnderManagement);
                sb.append("\t" + rec.detailRec.journalBytesUnderManagement);
                sb.append("\t" + rec.detailRec.segmentBytesUnderManagement);
                sb.append("\t" + rec.detailRec.managedJournalCount);
                sb.append("\t" + rec.detailRec.managedSegmentCount);
                
                sb.append("\t" + rec.detailRec.overflowCount);
                
            } else {
                // error obtaining the data of interest.
                sb.append("\tN/A");
                sb.append("\tN/A");
                sb.append("\tN/A");
                sb.append("\tN/A");
                sb.append("\tN/A");

                sb.append("\tN/A");
                sb.append("\tN/A");
                sb.append("\tN/A");
                sb.append("\tN/A");
                sb.append("\tN/A");

                sb.append("\tN/A");
            }
            
            sb.append("\t"
                    + BytesUtil.toString(rec.locator.getLeftSeparatorKey()));
            sb.append("\t"
                    + BytesUtil.toString(rec.locator.getRightSeparatorKey()));
            
            if(rec.localPartitionMetadata!=null) {

                sb.append("\t\"" + rec.localPartitionMetadata.getHistory()+"\"");
                
            } else {
                
                sb.append("\tN/A");
            
            }
            
            System.out.println(sb.toString());

        } // next index partition.

        /*
         * Verify a constraint on the last index partition.
         */
        if (lastLocator != null && lastLocator.getRightSeparatorKey() != null) {

            log
                    .error("name="
                            + indexName
                            + " : Right separator of last index partition is not null: "
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
        private final String name;
        private final int code;
        
        /**
         * Extract some useful metadata for an {@link IDataService}.
         */
        static public ServiceMetadata getServiceMetadata(
                final JiniFederation fed, final UUID uuid) {

            if (fed == null)
                throw new IllegalArgumentException();

            if (uuid == null)
                throw new IllegalArgumentException();

            /*
             * @todo restricted to (meta)data services by use of type specific
             * cache!
             */
            final ServiceItem serviceItem = fed.getDataServicesClient()
                    .getServiceItem(uuid);

            if (serviceItem == null) {

                throw new RuntimeException("No such (Meta)DataService? uuid="
                        + uuid);

            }
            
            String hostname = null;
            String name = null;

            for (Entry e : serviceItem.attributeSets) {

                if (e instanceof Hostname && hostname == null) {

                    hostname = ((Hostname) e).hostname;

                } else if (e instanceof Name && name == null) {

                    name = ((Name) e).name;

                }

            }

            if (hostname == null) {

                log.warn("No hostname? : " + serviceItem);

                hostname = "Unknown(" + uuid + ")";

            }

            if (name == null) {

                log.warn("No name? : "+serviceItem);

                name = "Unknown(" + uuid + ")";

            }

            /*
             * Assign a one-up integer code to the service.
             */
            Integer code = null;

            synchronized (codes) {

                code = codes.get(uuid);

                if (code == null) {

                    code = new Integer(codes.size());

                    codes.put(uuid, code);

                }

            }

            return new ServiceMetadata(uuid, hostname, name, code);

        }

        /**
         * Map used to assign unique one-up codes to services which are shorter
         * than service names and easier to correlate than UUIDs.
         */
        static private Map<UUID,Integer> codes = new HashMap<UUID,Integer>();

        public ServiceMetadata(UUID uuid, String hostname, String name, int code) {

            if (uuid == null)
                throw new IllegalArgumentException();

            if (hostname == null)
                throw new IllegalArgumentException();

            if (name == null)
                throw new IllegalArgumentException();

            this.uuid = uuid;

            this.hostname = hostname;

            this.name = name;

            this.code = code;

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
         * The service name.
         */
        public String getName() {
         
            return name;
            
        }

        /**
         * A one-up code assigned to the service that is stable for the life of
         * the JVM. This may be used as a short label for the service that is
         * easy to correlate.
         */
        public int getCode() {
            
            return code;
            
        }
        
    }
        
}

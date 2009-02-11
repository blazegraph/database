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
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import net.jini.config.ConfigurationException;
import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceItem;
import net.jini.lookup.entry.Name;

import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentCheckpoint;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.config.Configuration;
import com.bigdata.jini.lookup.entry.Hostname;
import com.bigdata.journal.DumpJournal;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.StoreManager;
import com.bigdata.resources.StoreManager.ManagedJournal;
import com.bigdata.service.DataService;
import com.bigdata.service.IDataService;
import com.bigdata.service.IDataServiceAwareProcedure;
import com.bigdata.service.IMetadataService;
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
 * @todo debug logic to dump only within the namespace (its hacked in
 *       {@link ListIndicesTask}).
 */
public class DumpFederation {

    protected static final Logger log = Logger.getLogger(DumpFederation.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    /**
     * The component name for this class (for use with the
     * {@link ConfigurationOptions}).
     */
    public static final String COMPONENT = DumpFederation.class.getName(); 

    /**
     * {@link Configuration} options for this class.
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface ConfigurationOptions {

        /**
         * An optional namespace prefix. When given, only indices having this
         * prefix will be dumped.
         */
        String NAMESPACE = "namespace";

        /**
         * How long to wait for service discovery.
         */
        String DISCOVERY_DELAY = "discoveryDelay";
   
    }
    
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
     * @throws ConfigurationException 
     */
    static public void main(final String[] args) throws InterruptedException,
            ExecutionException, IOException, TimeoutException, ConfigurationException {

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

        final long discoveryDelay = (Long) fed.getClient().getConfiguration()
                .getEntry(COMPONENT, ConfigurationOptions.DISCOVERY_DELAY,
                        Long.TYPE, 5000L/* default */);

        final String namespace = (String) fed.getClient().getConfiguration()
                .getEntry(COMPONENT, ConfigurationOptions.NAMESPACE,
                        String.class, ""/* default */);

        try {

            /*
             * Wait until we have the metadata service
             */
            if (INFO)
                log.info("Waiting up to " + discoveryDelay
                        + "ms for metadata service discovery.");

            fed
                    .awaitServices(1/* minDataServices */, discoveryDelay/* timeout(ms) */);

            // a read-only transaction as of the last commit time.
            final long tx = fed.getTransactionService().newTx(
                    ITx.READ_COMMITTED);

            try {

                final FormatRecord formatter = new FormatTabTable(System.out);

                final DumpFederation dumper = new DumpFederation(fed, tx,
                        formatter);

                formatter.writeHeaders();
                
                dumper.dumpIndices( namespace );

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
     * Object used to format the output.
     */
    private final FormatRecord formatter;
    
    /**
     * Interface reponsible for formatting the output.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface FormatRecord {
       
        /**
         * Write out column headers for the dump records.
         */
        public void writeHeaders();
        
        /**
         * Write out the details for a record corresponding to a single index
         * partition.
         * 
         * @param rec
         *            The record.
         */
        public void writeRecord(IndexPartitionRecord rec);
        
    }
    
    /**
     * Tab-delimited tabular output.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class FormatTabTable implements FormatRecord {
        
        private final PrintWriter out;
        
        public FormatTabTable(final PrintWriter w) {

            if (w == null)
                throw new IllegalArgumentException();

            this.out = w;

        }

        public FormatTabTable(final Writer w) {

            this( new PrintWriter( w ));

        }

        public FormatTabTable(final PrintStream w) {

            this(new PrintWriter(w));
            
        }

        /**
         * @todo document the columns.
         */
        public void writeHeaders() {
            
            final String s = "Timestamp"//
                            + "\tIndexName" //
                            + "\tIndexPartitionName"//
                            + "\tPartitionId" //
                            + "\tServiceUUID" //
                            + "\tServiceName" //
                            + "\tHostname" //
                            + "\tServiceCode"//
                            /*
                             * Basic metadata about the index partition.
                             */
                            + "\tSourceCount"//
                            + "\tSourceJournalCount"
                            + "\tSourceSegmentCount"
                            + "\tSumEntryCounts" //
                            + "\tSumSegmentBytes"// 
                            + "\tSumSegmentNodeBytes" //
                            + "\tSumSegmentLeafBytes"// 
                            /*
                             * Note: These values are aggregates for the data
                             * service on which the index partition resides.
                             */
                            + "\tDataDirFreeSpace"// 
                            + "\tBytesUnderManagement"// 
                            + "\tJournalBytesUnderManagement"// 
                            + "\tIndexSegmentBytesUnderManagement"// 
                            + "\tManagedJournalCount"// 
                            + "\tManagedSegmentCount"//
                            + "\tOverflowCount"//
                            /*
                             * Extended metadata about the index partition.
                             */
                            + "\tLeftSeparator"//
                            + "\tRightSeparator"//
                            + "\tView"
                            + "\tHistory"//
                            ;

            out.println(s);
            
        }

        /** format row for table. */
        public void writeRecord(final IndexPartitionRecord rec) {
            
            final StringBuilder sb = new StringBuilder();
            sb.append(rec.ts);//new Date(ts));
            sb.append("\t" + rec.indexName);
            sb.append("\t" + DataService.getIndexPartitionName(rec.indexName,rec.locator.getPartitionId()));
            sb.append("\t" + rec.locator.getPartitionId());
            sb.append("\t" + rec.locator.getDataServiceUUID());
            sb.append("\t" + rec.smd.getName());
            sb.append("\t" + rec.smd.getHostname());
            sb.append("\t" + "DS" + rec.smd.getCode());
            
            if (rec.detailRec != null) {
                
                // aggregate across all sources in the view.
                final SourceDetailRecord sourceDetailRec = new SourceDetailRecord(
                        rec.detailRec.sources);
                
                // core view stats.
                sb.append("\t" + rec.detailRec.sourceCount);
                sb.append("\t" + rec.detailRec.journalSourceCount);
                sb.append("\t" + rec.detailRec.segmentSourceCount);
                
                // per source stats (aggregated across sources in the view).
                sb.append("\t" + sourceDetailRec.entryCount);
                sb.append("\t" + sourceDetailRec.segmentByteCount);
                sb.append("\t" + sourceDetailRec.segmentNodeByteCount);
                sb.append("\t" + sourceDetailRec.segmentLeafByteCount);

                // stats for the entire data service
                sb.append("\t" + rec.detailRec.dataDirFreeSpace);
                sb.append("\t" + rec.detailRec.bytesUnderManagement);
                sb.append("\t" + rec.detailRec.journalBytesUnderManagement);
                sb.append("\t" + rec.detailRec.segmentBytesUnderManagement);
                sb.append("\t" + rec.detailRec.managedJournalCount);
                sb.append("\t" + rec.detailRec.managedSegmentCount);
                sb.append("\t" + rec.detailRec.overflowCount);
                
            } else {
                
                /*
                 * Error obtaining the data of interest.
                 */
                
                // core view stats.
                sb.append("\tN/A");
                sb.append("\tN/A");
                sb.append("\tN/A");
                
                // aggregated per-source in view stats.
                sb.append("\tN/A");
                sb.append("\tN/A");
                sb.append("\tN/A");
                sb.append("\tN/A");

                // data service stats.
                sb.append("\tN/A");
                sb.append("\tN/A");
                sb.append("\tN/A");
                sb.append("\tN/A");
                sb.append("\tN/A");
                sb.append("\tN/A");
                sb.append("\tN/A");
                
            }

            // extended view stats.
            sb.append("\t"
                    + BytesUtil.toString(rec.locator.getLeftSeparatorKey()));
            sb.append("\t"
                    + BytesUtil.toString(rec.locator.getRightSeparatorKey()));

            if (rec.detailRec != null && rec.detailRec.pmd != null) {

                // current view definition.
                sb.append("\t\""
                        + Arrays.toString(rec.detailRec.pmd.getResources())
                        + "\"");

                // history
                sb.append("\t\"" + rec.detailRec.pmd.getHistory() + "\"");

            } else {

                sb.append("\tN/A");
                sb.append("\tN/A");

            }
            
            out.println(sb.toString());
            
        }

    }
    
    /**
     * 
     * @param fed
     *            The federation whose indices will be dump.
     * @param tx
     *            The timestamp that will be used to dump the database. This
     *            SHOULD be a read-historical transaction since that will put a
     *            read-lock into place on the data during any operations by this
     *            class.
     * @param formatter
     *            Object used to format the output.
     */
    public DumpFederation(final JiniFederation fed, final long tx,
            final FormatRecord formatter) {

        if (fed == null)
            throw new IllegalArgumentException();
        
        if (formatter == null)
            throw new IllegalArgumentException();
        
        this.fed = fed;
        
        this.ts = tx;
        
        this.formatter = formatter;
        
    }
    
    /**
     * The names of all registered scale-out indices having the specified
     * namespace prefix.
     * 
     * @param namespace
     *            The namespace prefix.
     * 
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     * 
     */
    public String[] getIndexNames(String namespace)
            throws InterruptedException, ExecutionException, IOException {

        if (namespace.length() != 0) {
            
            /*
             * Note: Add the prefix that is used by the indices in the metadata
             * service.
             */
            namespace = MetadataService.METADATA_INDEX_NAMESPACE + namespace;
            
        }
        
        final IMetadataService mds = fed.getMetadataService();
        
        if(mds == null) {
            
            throw new RuntimeException("Could not discover the metadata service");
            
        }
        
        return (String[]) mds.submit(new ListIndicesTask(ts, namespace)).get();

    }

    /**
     * Generates the dump record for all scale-out indices having the specified
     * namespace prefix.
     * 
     * @param namespace
     *            The namespace prefix (may be an empty string to dump all
     *            indices).
     * 
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void dumpIndices(final String namespace)
            throws InterruptedException, ExecutionException, IOException {

        final String[] names = getIndexNames(namespace);

        if (INFO)
            log.info("Found " + names.length + " indices: "
                    + Arrays.toString(names));

        for (String name : names) {

            // strip off the prefix to get the scale-out index name.
            final String scaleOutIndexName = name
                    .substring(MetadataService.METADATA_INDEX_NAMESPACE
                            .length());

            dumpIndex(scaleOutIndexName);

        }

    }
    
    /**
     * Generates the dump record for the specified scale-out index.
     * 
     * @param indexName
     *            The name of a scale-out index.
     * 
     * @throws InterruptedException
     */
    public void dumpIndex(final String indexName) throws InterruptedException {

        final IMetadataIndex metadataIndex;
        try {

            metadataIndex = fed.getMetadataIndex(indexName, ts);

        } catch (Throwable t) {

            final Throwable t2 = InnerCause.getInnerCause(t,
                    ClassNotFoundException.class);

            if (t2 != null) {

                log.error("CODEBASE/CLASSPATH problem:", t2);

                return;

            }

            throw new RuntimeException(t);

        }

        dumpIndexLocators(indexName, metadataIndex);

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

            this.ts = ts;
            
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

            // all things of interest.
            IndexPartitionDetailRecord detailRec = null;
            try {

                detailRec = (IndexPartitionDetailRecord) dataService
                        .submit(new FetchIndexPartitionByteCountRecordTask(ts,
                                DataService.getIndexPartitionName(indexName,
                                        locator.getPartitionId()))).get(); 
                
            } catch(InterruptedException t) {
                
                throw t;
                
            } catch(Exception t) {

                log.warn("name=" + indexName, t);

            }
            this.detailRec = detailRec;
            
        }

        /**
         * The timestamp used to obtain the view.
         */
        public final long ts;
        
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
     * A record detailing various things counted on a per-source basis.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class SourceDetailRecord implements Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = -2064727234836478585L;

        /**
         * The sum of the entry count for each {@link AbstractBTree} in the
         * index partition view.
         * <p>
         * Note: This is computed from the {@link IndexSegmentCheckpoint}
         * without requiring us to open the {@link IndexSegment} itself, so it
         * can be significantly faster if the {@link IndexSegment} is trying to
         * fully buffer the nodes region of the file and will also impose less
         * of a memory burden since those node buffers will not be allocated in
         * response to this operation. The only drawback of the sum of the entry
         * counts is that it will overestimate the #of tuples in an index
         * partition after a split until the next compacting merge because
         * historical {@link IndexSegment}s in the old view will be reused by
         * each of the view generated by the split until the next compacting
         * merge, move, or join.
         * <p>
         * Note: The exact range count is MUCH too expensive as it requires
         * materializing every tuple in the index partition view!
         */
        public final long entryCount;
        
        /**
         * The #of bytes across all {@link IndexSegment}s in the view.
         */
        public final long segmentByteCount;
        
        /**
         * The #of bytes in the node region of the {@link IndexSegment}s in
         * the view.
         */
        public final long segmentNodeByteCount;

        /**
         * The #of bytes in the leaf region of the {@link IndexSegment}s in
         * the view.
         */
        public final long segmentLeafByteCount;

        /**
         * 
         * @param entryCount
         * @param segmentByteCount
         * @param segmentNodeByteCount
         * @param segmentLeafByteCount
         */
        public SourceDetailRecord(//
                final long entryCount,//
                final long segmentByteCount,//
                final long segmentNodeByteCount,//
                final long segmentLeafByteCount//
                ) {
            
            this.entryCount = entryCount;
            
            this.segmentByteCount = segmentByteCount;
            
            this.segmentNodeByteCount = segmentNodeByteCount;
            
            this.segmentLeafByteCount = segmentLeafByteCount;
            
        }

        /**
         * Ctor returns a record that contains the sum across the given array of
         * record.
         * 
         * @param a
         *            An array of records to be summed.
         */
        public SourceDetailRecord(final SourceDetailRecord[] a) {

            if (a == null)
                throw new IllegalArgumentException();
            
            long entryCount = 0;
            long segmentByteCount = 0;
            long nodeByteCount = 0;
            long leafByteCount = 0;
            
            for (SourceDetailRecord t : a) {

                entryCount += t.entryCount;

                segmentByteCount += t.segmentByteCount;

                nodeByteCount += t.segmentNodeByteCount;

                leafByteCount += t.segmentLeafByteCount;

            }

            this.entryCount = entryCount;

            this.segmentByteCount = segmentByteCount;

            this.segmentNodeByteCount = nodeByteCount;

            this.segmentLeafByteCount = leafByteCount;

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
         * The name of the index partition.
         */
        public final String indexPartitionName;
        
        /**
         * The detailed description of the index view.
         */
        public final LocalPartitionMetadata pmd;
        
        /**
         * Details on each source in the view.  The order is the order
         * of the sources in the view.
         */
        public SourceDetailRecord[] sources;
        
        /**
         * The #of resources in the view for the index partition.
         */
        public final int sourceCount;
        
        /**
         * The #of resources in the view for the index partition which are
         * {@link ManagedJournal}s.
         */
        public final int journalSourceCount;
        
        /**
         * The #of resources in the view for the index partition which are
         * {@link IndexSegment}s.
         */
        public final int segmentSourceCount;
        
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

        /**
         * 
         * @param btree
         * @param resourceManager
         */
        public IndexPartitionDetailRecord(
                final ResourceManager resourceManager, final long timestamp,
                final String name) {
            
            if (resourceManager == null)
                throw new IllegalArgumentException();

            if (name == null)
                throw new IllegalArgumentException();

            this.indexPartitionName = name;
            
            // the mutable BTree for the view.
            final BTree btree;
            {

                /*
                 * Obtain the read-only {@link BTree} for the historical
                 * timestamp for which the dump is being generated. This is
                 * strongly typed as a {@link BTree} since we DO NOT want to
                 * force the materialization of the index partition view in case
                 * it is not already open. Materializing the view will force the
                 * index segments in the view to be materialized, and that means
                 * buffering their nodes in memory which is a moderately
                 * expensive IO.
                 */
                
                final IRawStore store = resourceManager.getJournal(timestamp);

                if (store == null) {

                    throw new RuntimeException("No journal? : timestamp="
                            + timestamp);

                }

                btree = (BTree) resourceManager.getIndexOnStore(
                        name, timestamp, store);

                if (btree == null) {

                    throw new RuntimeException(
                            "No index partition on journal? : timestamp="
                                    + timestamp + ", name=" + name);

                }

                pmd = btree.getIndexMetadata().getPartitionMetadata();
                
            }

            if (pmd == null) {

                sources = new SourceDetailRecord[] { //
                // the btree on the live journal.
                new SourceDetailRecord(btree.getEntryCount(), 0L, 0L, 0L) //
                };

                this.sourceCount = 1;

                this.journalSourceCount = 1;

                this.segmentSourceCount = 0;
                
            } else {
                
                int sourceCount = 0;
                
                int journalSourceCount = 0;
                
                int segmentSourceCount = 0;
                
                final IResourceMetadata[] resources = pmd.getResources();
                
                sources = new SourceDetailRecord[resources.length];

                for (int i = 0; i < resources.length; i++) {

                    final IResourceMetadata x = resources[i];

                    sourceCount++;

                    /*
                     * Note: This will force the (re-)open of the
                     * IndexSegmentStore, but not of the IndexSegment on that
                     * store!
                     */
                    final IRawStore store = resourceManager.openStore(x
                            .getUUID());

                    if (store == null) {
                        
                        throw new RuntimeException(
                                "Store not found? : " + x);
                        
                    }
                    
                    if (x.isJournal()) {

                        journalSourceCount++;

//                        if (i == 0) {
//
//                            sources[i] = new SourceDetailRecord(btree
//                                    .getEntryCount(), 0L, 0L, 0L);
//
//                        } else {

                            final BTree tmp = (BTree) resourceManager
                                    .getIndexOnStore(name, timestamp, store);

                            if (tmp == null) {

                                throw new RuntimeException(
                                        "No index partition on journal? : timestamp="
                                                + timestamp + ", name=" + name);

                            }

                            sources[i] = new SourceDetailRecord(tmp
                                    .getEntryCount(), 0L, 0L, 0L);

//                        }

                        continue;

                    } else {

                        segmentSourceCount++;

                        final IndexSegmentStore segStore = (IndexSegmentStore) store;

                        // #of tuples in this index segment.
                        final long entryCount = segStore.getCheckpoint().nentries;

                        sources[i] = new SourceDetailRecord(//
                                // #of tuples
                                entryCount,//
                                // #of bytes in the index segment.
                                segStore.size(),//
                                // #of bytes in the nodes extent of the seg.
                                segStore.getCheckpoint().extentNodes,
                                // #of bytes in the leaves extent of the seg.
                                segStore.getCheckpoint().extentLeaves);

                    }
                    
                }

                this.sourceCount = sourceCount;

                this.journalSourceCount = journalSourceCount;
                
                this.segmentSourceCount = segmentSourceCount;
                
            }
            
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
            Callable<IndexPartitionDetailRecord>, IDataServiceAwareProcedure {

        /**
         * 
         */
        private static final long serialVersionUID = 1656089893655069298L;

        private final long timestamp;
        
        private final String name;
        
        /**
         * @param name
         *            The name of the index partition.
         * @param timestamp
         *            The timestamp of the read-historical transaction that is
         *            being used to generate the dump. This is used here to open
         *            the journal on which the {@link BTree} is found for that
         *            timestamp for the named index partition.
         */
        public FetchIndexPartitionByteCountRecordTask(final long timestamp,
                final String name) {

            if (name == null)
                throw new IllegalArgumentException();
            
            this.timestamp = timestamp;
            
            this.name = name;
            
        }

        public IndexPartitionDetailRecord call() throws Exception {

            if (dataService == null) {

                // data service was not set?
                throw new IllegalStateException();

            }

            final ResourceManager resourceManager = dataService
                    .getResourceManager();

            return new IndexPartitionDetailRecord(resourceManager, timestamp,
                    name);

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
     * data service on which it resides. The information is collected in
     * parallel in order to minimize the total latency. This is especially
     * important when there are a large #of index partitions.
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

        final List<Callable<IndexPartitionRecord>> tasks = new LinkedList<Callable<IndexPartitionRecord>>();
        
        while (itr.hasNext()) {

            final PartitionLocator locator = itr.next().getObject();

            tasks.add(new Callable<IndexPartitionRecord>(){
                public IndexPartitionRecord call() throws Exception {

                    return new IndexPartitionRecord(fed, ts, indexName, locator);

                }
            });
            
        }
        
        // execute all requests in parallel.
        final List<Future<IndexPartitionRecord>> futures = fed.getExecutorService().invokeAll(tasks);
        
        final List<IndexPartitionRecord> results = new LinkedList<IndexPartitionRecord>();
        
        for(Future<IndexPartitionRecord> f : futures) {
            
            try {
                results.add(f.get());
            } catch(ExecutionException ex) {
                log.error(indexName, ex);
                continue;
            }
            
        }
        
        PartitionLocator lastLocator = null;

        for(IndexPartitionRecord rec : results) {

            /*
             * Verify some constraints on the index partition separator keys.
             */
            final PartitionLocator locator = rec.locator;
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

            formatter.writeRecord(rec);
            
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

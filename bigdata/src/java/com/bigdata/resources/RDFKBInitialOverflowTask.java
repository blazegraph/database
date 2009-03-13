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
 * Created on Feb 18, 2009
 */

package com.bigdata.resources;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import com.bigdata.btree.BTree;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.Event;
import com.bigdata.service.EventResource;
import com.bigdata.service.IDataService;
import com.bigdata.service.IDataServiceAwareProcedure;
import com.bigdata.service.ResourceService;
import com.bigdata.service.Split;
import com.bigdata.util.concurrent.ExecutionExceptions;

/**
 * A task designed specifically for the RDF DB. The DB MUST reside on a single
 * data service, preferrably one with nothing else on it though that is not
 * required. This overflow task takes advantage of the "schema" for the RDF DB
 * to distribute index partitions across a federation in something which
 * approaches an optimal manner. This is basically a "scatter split" for the
 * TERM2ID and POS indices (they are split into N+1 chunks and one chunk will be
 * placed onto each of the N target data service with one chunk being left on
 * this data service) and a "tailSplit+move" for the ID2TERM, SPO, and OSP
 * indices, except that an empty tail is placed onto each of the N target data
 * services.
 * <p>
 * The RDF DB is comprised of 5 core indices. There are two for the lexicon
 * (TERM2ID and ID2TERM), and three statement indices: SPO, POS, and OSP. These
 * are briefly described in {@link IndexEnum}.
 * <p>
 * TERM2ID and POS are split into N more or less equal partitions and scattered
 * across the federation.
 * <p>
 * ID2TERM, SPO, OSP are split by a special operation which takes advantage of
 * the fact that each new TERM2ID index partition will generate term identifiers
 * that are larger than any term identifier we have already seen. Therefore we
 * create a new index partition for each of these indices which holds all of the
 * data in the initial partition and also create one new index partition for
 * each of the N index partitions created for TERM2ID. These N index partitions
 * will be empty since no terms have yet been assigned term identifiers which
 * could enter into those index partitions. These empty index partitions are
 * created in place on data services across the federation. As soon as the
 * TERM2ID index is split tuples will begin to be written into these index
 * partitions.
 * <p>
 * We handle TERM2ID atomically with the ID2TERM, SPO and OSP indices since
 * there is a mutual dependency (the new TERM2ID index partitions will populate
 * the new ID2TERM, SPO, and OSP index partitions). The POS and any other
 * indices are handled next.
 * <p>
 * Note: This task gains an exclusive write lock on the indices used by the RDF
 * DB and will therefore block application writes on that RDF DB until the
 * operation is complete. This deemed acceptable for a one-time specialized
 * overflow handler. Subsequent overflows will be handled by the normal
 * mechanisms which are designed to minimize the time which the application will
 * be blocked for write access to any given index partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Integrate w/ synchronous overflow in {@link OverflowManager#overflow()}.
 * <p>
 * The integration must recognize the existence of TERM2ID#0 and put an "equal
 * parts scatter split" on a list of actions to be executed "first" during
 * asynchronous overflow. Without resorting to global synchronous locks, we also
 * recognize the presence of SPO#0, ID2TERM#0, POS#0, and OSP#0 and assign a
 * task to each. POS#0 will be an "equal parts scatter split". The other indices
 * will be "equal partitions scatter splits".
 * <p>
 * The "equal parts scatter split" divides an index into N+1 equal splits,
 * leaves one split on the source data service, and puts the rest of the splits
 * onto N other data services (move targets) identified during synchronous
 * overflow (rather than during asynchronous overflow since we want to scatter
 * and not just load balance).
 * <p>
 * The "equal partitions scatter splits" divides an index (such as ID2TERM, SPO,
 * or OSP) into the same number of index partitions as some index on which it
 * has a dependency (these are all dependent on TERM2ID). The separator keys for
 * the new index partitions are defined in an index specific manner in terms of
 * the index partition identifiers for the dependency. Thus if we do an "equal
 * parts scatter split" for TERM2ID#0 and produce index partitions TERM2ID#1 ...
 * TERM2ID#10, then we will also generate 10 index partitions for each of
 * ID2TERM, SPO, and OSP. Since this is a scatter split, we will leave one index
 * partition in place and scatter the rest onto the available data services.
 * 
 * FIXME Put the targetService[] onto the {@link SplitResult} so we can isolate
 * the logic for how to create the new index partitions on both the local and
 * any remote data services within {@link SplitUtility} or
 * {@link SplitIndexPartitionTask}. Right now, it is assuming that split leaves
 * the data local and allows a single partition to be moved as an after action.
 * The old and new locators should also be encapsulated on the
 * {@link SplitResult} since we need them for the MDS update.
 * <P>
 * Maybe just write another task that does the SPLIT as an unisolated operation
 * and handles all of this stuff in the new manner.
 * <p>
 * Another MOVE operation could be written along the same lines.
 * 
 * FIXME Must either constrain the initial index allocation to a single data
 * service -or- constrain the SPO, OSP, and ID2TERM actions to NOT permit Split
 * until there are at least N TERM2ID index partitions.
 * 
 * @todo full text, justifications index must be handled by normal asynchronous
 *       overflow.
 *       <p>
 *       Since we are not treating them specially we need to register the
 *       indices that we are handling using
 *       {@link OverflowMetadata#setAction(String, OverflowActionEnum)} and then
 *       make sure that {@link PostProcessOldJournalTask} does not attempt to
 *       handle those indices as well.
 * 
 * @todo unit tests. this will have to be in the bigdata-rdf module.
 * 
 * @todo If building and sending index segments is efficient enough then maybe
 *       {@link SplitIndexPartitionTask} and {@link MoveIndexPartitionTask}
 *       should be reworked as UNISOLATED tasks which first generate index
 *       segments containing the total view of the index partition(s) and then,
 *       blast the index partition to the target data service, and then do the
 *       atomic update protocol.
 *       <p>
 *       Join could also be reworked in this manner but that is a bit trickier.
 *       Joins may well require the use of distributed synchronous locks, in
 *       which case we may have to use them for all overflow tasks.
 * 
 * @deprecated This class was never functional. After reflection, I have decided
 *             to handle this in the short-term by a pre-parse of a subset of
 *             the RDF data collecting all terms in a "terms" index and
 *             predicates in a "preds" index. Based on the pre-parse I can
 *             choose the TERM2ID splits from the "terms" index and then
 *             allocate the index partitions on the cluster. I can allocate the
 *             ID2TERM, SPO, and OSP index partitions based on the TERM2ID
 *             splits. I can allocate the POS index partitions based on the
 *             "preds" index.
 *             <p>
 *             The stuff outlined in this class will eventually be reabsorbed in
 *             an UNISOLATED SPLIT operation that first does a compacting merge,
 *             is capable of placing each split onto an arbitrary data service,
 *             and uses the {@link ResourceService} to get the
 *             {@link IndexSegment} onto the target data service (when the
 *             target is a different data service).
 *             <p>
 *             A UNISOLATED MOVE operation will also be defined along the same
 *             lines.
 */
public class RDFKBInitialOverflowTask extends AbstractTask {

    enum IndexEnum {
        /**
         * Index assigns unique 64-bit identifiers to each "term". A term is a
         * URI, literal, blank node or statement identifier. Each partition of
         * this index uses its partitionId in the high word of the long term
         * identifier in order to "partition" the term identifier value space
         * and ensure that unique term identifiers can be generated based solely
         * on information local to that index partition.
         */
        TERM2ID,
        /**
         * Index maps a 64-bit identifier back to the corresponding term.
         */
        ID2TERM,
        /**
         * Key is {s,p,o}, where s, p, and o are term identifiers.
         */
        SPO,
        /**
         * Key is {o,s,p}, where s, p, and o are term identifiers.
         */
        OSP,
        /**
         * Key is {p,o,s}, where s, p, and o are term identifiers.
         */
        POS;

        /**
         * Return the fully qualified name of partition #0 of the index.
         * 
         * @param namespace
         *            The namespace of the RDF DB.
         * 
         * @return The fully qualified index name.
         */
        public String getFQN(final String namespace) {

            final String relation;
            switch (this) {
            case TERM2ID:
            case ID2TERM:
                relation = "lex";
                break;
            case SPO:
            case OSP:
            case POS:
                relation = "spo";
                break;
            default:
                throw new AssertionError(this);
            }

            return namespace + "." + relation + "." + this + "#0";

        }
        
    }
    
    /**
     * Create an instance of the overflow task.
     * 
     * @param overflowMetadata
     * @param namespace
     *            The namespace of the RDF DB.
     * @param minDataServices
     *            The minimum #of data services across which the RDF DB will be
     *            scattered by this operation. When ZERO (0), there is no
     *            minimum.
     * @param maxDataServices
     *            The maximum #of data services across which the RDF DB will be
     *            scattered by this operation. When ZERO (0), all discovered
     *            data services will be used.
     * 
     * @return The overflow task.
     */
    static public RDFKBInitialOverflowTask create(
            final OverflowMetadata overflowMetadata, final String namespace,
            final int minDataServices, final int maxDataServices) {
    
        /*
         * We will need to operate on partition zero for each of these indices
         * with the namespace of the KB.
         */
        final String[] resources = new String[] {
              
                IndexEnum.TERM2ID.getFQN(namespace),
                IndexEnum.ID2TERM.getFQN(namespace),

                IndexEnum.SPO.getFQN(namespace),
                IndexEnum.OSP.getFQN(namespace),
                IndexEnum.POS.getFQN(namespace),

        };

        return new RDFKBInitialOverflowTask(overflowMetadata, namespace,
                resources, minDataServices, maxDataServices);

    }

    private final OverflowMetadata overflowMetadata;
    
    private final ResourceManager resourceManager;

    private final String namespace;
   
    /**
     * The maximum #of data services across which the RDF DB will be scattered
     * by this operation. When ZERO (0), all discovered data services will be
     * used.
     */
    private final int maxDataServices;
    private final int minDataServices;
    
    private final AbstractScaleOutFederation fed;
    
    /**
     * @param overflowMetadata
     * @param resource
     *            Names each index in the RDF DB. Those indices MUST exist on
     *            this data service.
     * @param maxDataServices
     *            The maximum #of data services across which the RDF DB will be
     *            scattered by this operation. When ZERO (0), all discovered
     *            data services will be used.
     */
    protected RDFKBInitialOverflowTask(final OverflowMetadata overflowMetadata,
            final String namespace, final String[] resource,
            final int minDataServices, final int maxDataServices
            ) {

        super(overflowMetadata.resourceManager.getConcurrencyManager(), ITx.UNISOLATED, resource);

        this.overflowMetadata = overflowMetadata;
        
        this.resourceManager = overflowMetadata.resourceManager;

        this.namespace = namespace;
        
        this.minDataServices = minDataServices;

        this.maxDataServices = maxDataServices;

        this.fed = (AbstractScaleOutFederation) resourceManager.getFederation();

    }

    @Override
    protected Object doTask() throws Exception {

        final Event e = new Event(fed, new EventResource(), getClass()
                .getName(), namespace).start();

        try {

            // verify partition#0 for each index exists
            for (String s : getResource()) {

                if (getIndex(s) == null) {

                    throw new Exception(
                            "Precondition failure: index not found: " + s);

                }

            }

            /*
             * This is the last commit time on the old journal. We use this as
             * the commitTime that gets written into the checkpoint records for
             * the generated index segments.
             */
            final long commitTime = getJournal().getLastCommitTime();
            
            // the target data services.
            final UUID[] moveTargets = getTargetDataServices();

            // resolve each UUIDs to a proxy for that data service.
            final IDataService[] moveServices = fed.getDataServices(moveTargets);
            
            /*
             * Generate tasks to build all of the necessary index segments.
             * There will be one index segment per split for the TERM2ID and the
             * POS indices. There will be one index segment for each of the
             * ID2TERM, SPO, and OSP indices.
             */
            final LinkedList<Callable<BuildResult>> buildTasks = new LinkedList<Callable<BuildResult>>();

            /*
             * Identify the TERM2ID splits and create tasks that will generate
             * one index segment per TERM2ID Split. The goal is to have one
             * TERM2ID index partition per moveTarget. One of these index
             * segments will be left on this data service. The rest will be
             * moved to other data services.
             */
            buildTasks.addAll(getBuildTasks(
                    IndexEnum.TERM2ID.getFQN(namespace),
                    getIndex(IndexEnum.TERM2ID.getFQN(namespace)), commitTime,
                    getSplits(getIndex(IndexEnum.TERM2ID
                            .getFQN(namespace)),
                            moveTargets.length + 1), e));

            /*
             * Identify the POS splits and create tasks that will generate one
             * index segment per POS Split. The goal is to have one POS index
             * partition per moveTarget. One of these index segments will be
             * left on this data service. The rest will be moved to other data
             * services.
             */
            buildTasks.addAll(getBuildTasks(IndexEnum.POS.getFQN(namespace),
                    getIndex(IndexEnum.POS.getFQN(namespace)), commitTime,
                    getSplits(getIndex(IndexEnum.POS.getFQN(namespace)),
                            moveTargets.length + 1), e));

            // build task for the ID2TERM index.
            buildTasks.add(new Callable<BuildResult>() {
                public BuildResult call() throws Exception {
                    return resourceManager.buildIndexSegment(IndexEnum.ID2TERM
                            .getFQN(namespace), getIndex(IndexEnum.ID2TERM
                            .getFQN(namespace)), true/* compactingMerge */,
                            commitTime, null/* fromKey */, null/*toKey*/, e);
                }
            });

            // build task for the SPO index.
            buildTasks.add(new Callable<BuildResult>() {
                public BuildResult call() throws Exception {
                    return resourceManager.buildIndexSegment(IndexEnum.SPO
                            .getFQN(namespace), getIndex(IndexEnum.SPO
                            .getFQN(namespace)), true/* compactingMerge */,
                            commitTime, null/* fromKey */, null/*toKey*/, e);
                }
            });

            // build task for the OSP index.
            buildTasks.add(new Callable<BuildResult>() {
                public BuildResult call() throws Exception {
                    return resourceManager.buildIndexSegment(IndexEnum.OSP
                            .getFQN(namespace), getIndex(IndexEnum.OSP
                            .getFQN(namespace)), true/* compactingMerge */,
                            commitTime, null/* fromKey */, null/*toKey*/, e);
                }
            });

            /*
             * Build all the necessary index segments in parallel (blocks until
             * done).
             */
            final Map<String/* sourceIndexPartitionName */, List<BuildResult>> buildResults = buildSplits(buildTasks);

            /*
             * This executes in parallel the atomic update for each of the index
             * partitions which we need to register on a remote data service and
             * also handles the copy (by the remote data service) of the index
             * segment when we need to send it same data. We have already
             * generated the index segments for each of the indices.
             * 
             * For the TERM2ID and POS indices, we submit tasks to the target
             * data services to which we want to move each index partition. The
             * task will copy the index partition from this data service and
             * register the new index partition. The view for the index
             * partition includes the newly registered index on the live journal
             * for that data service and the index segment that we just sent to
             * that data service.
             * 
             * For the ID2TERM, SPO, and OSP indices we just create register an
             * empty index partition on the target data service. The view for
             * the index partition includes just the mutable BTree on the live
             * journal for that data service. [If this task fails, then we
             * create the empty index partition on this data service rather than
             * dying.] We also create a new index partition on this data service
             * that will have all the data written to date on each of those
             * indices.
             * 
             * Note: While the new index partitions will exist after this code
             * block, they have not been registered in the MDS and clients are
             * not being given StaleLocatorExceptions so they are not getting
             * used.
             */
            // The TERM2ID index segment that stays here.
            final BuildResult localTerm2Id;
            // The POS index segment that stays here.
            final BuildResult localPOS;
            // The ID2TERM index segment that stays here.
            final BuildResult localID2TERM;
            // The SPO index segment that stays here.
            final BuildResult localSPO;
            // The OSP index segment that stays here.
            final BuildResult localOSP;
            // The results (and the futures for the tasks doing the work for each generated index partition).
            final Map<MoveResult, Future<? extends Object>> scatterSplitFutures = new LinkedHashMap<MoveResult, Future<? extends Object>>();
            final Event sendSegment = e.newSubEvent(
                    OverflowSubtaskEnum.SendIndexSegment, "").start();
            try {

                { // TERM2ID

                    final String indexPartitionName = IndexEnum.TERM2ID
                            .getFQN(namespace);

                    final List<BuildResult> lst = buildResults
                            .get(indexPartitionName);

                    if (lst == null)
                        throw new AssertionError("No results? "
                                + indexPartitionName);

                    final BuildResult[] a = lst.toArray(new BuildResult[] {});

                    {
                        /*
                         * Handle the index partition which is left behind on
                         * this data service. We have already done a "build" and
                         * now we need to register the index partition on this
                         * data service. This is handled just like a move which
                         * normalizes the atomic updates so that they all just
                         * do MDS updates.
                         * 
                         * FIXME Should all be handled as "splits" since the
                         * scatter split is a split in which we place all but
                         * one of the created index partitions onto other data
                         * services? [Either way we still need to know which one
                         * is staying behind and run different logic for that
                         * one.]
                         */
                        final BuildResult buildResult = a[0];

                        // this one stays here.
                        localTerm2Id = buildResult;

                        final IndexMetadata indexMetadata = getIndex(
                                indexPartitionName).getIndexMetadata();

                        final LocalPartitionMetadata pmd = indexMetadata
                                .getPartitionMetadata();

                        final int newPartitionId = resourceManager
                                .nextPartitionId(indexMetadata.getName());

                        final PartitionLocator oldLocator = new PartitionLocator(//
                                pmd.getPartitionId(),//
                                resourceManager.getDataServiceUUID(),//
                                pmd.getLeftSeparatorKey(),//
                                pmd.getRightSeparatorKey()//
                        );

                        // Note: This "move" is to the same data service!
                        final UUID targetDataServiceUUID = resourceManager
                                .getDataServiceUUID();

                        final PartitionLocator newLocator = new PartitionLocator(
                                newPartitionId,//
                                targetDataServiceUUID,//
                                pmd.getLeftSeparatorKey(),//
                                pmd.getRightSeparatorKey()//
                        );

                        final MoveResult moveResult = new MoveResult(
                                indexPartitionName, indexMetadata,
                                targetDataServiceUUID, newPartitionId,
                                oldLocator, newLocator);

                        final Callable<Void> task = new Callable<Void>() {

                            public Void call() throws Exception {

                                getJournal().registerIndex(moveResult.name,
                                        // @todo should be cloned?
                                        moveResult.indexMetadata);

                                return null;
                                
                            }

                        };
                        
                        scatterSplitFutures.put(moveResult, fed
                                .getExecutorService().submit(task));

                    }

                    // note: leave the first result in place here.
                    for (int i = 1; i < a.length; i++) {

                        final BuildResult buildResult = a[i];
                        
                        final int index = (i - 1) % moveServices.length;

                        final UUID targetDataServiceUUID = moveTargets[index];

                        final IDataService ds = moveServices[index];

                        final IndexMetadata indexMetadata = getIndex(
                                indexPartitionName).getIndexMetadata();

                        final LocalPartitionMetadata pmd = indexMetadata
                                .getPartitionMetadata();

                        final int newPartitionId = resourceManager
                                .nextPartitionId(indexMetadata.getName());

                        final PartitionLocator oldLocator = new PartitionLocator(//
                                pmd.getPartitionId(),//
                                resourceManager.getDataServiceUUID(),//
                                pmd.getLeftSeparatorKey(),//
                                pmd.getRightSeparatorKey()//
                        );

                        final PartitionLocator newLocator = new PartitionLocator(
                                newPartitionId,//
                                targetDataServiceUUID,//
                                pmd.getLeftSeparatorKey(),//
                                pmd.getRightSeparatorKey()//
                        );

                        final MoveResult moveResult = new MoveResult(
                                indexPartitionName, indexMetadata,
                                targetDataServiceUUID, newPartitionId,
                                oldLocator, newLocator);
                        
                        final Callable<Void> task = new CopyIndexSegmentAndCreateIndexPartition(
                                resourceManager, indexPartitionName,
                                newPartitionId, indexMetadata,
                                targetDataServiceUUID,
                                buildResult.segmentMetadata);

                        scatterSplitFutures.put(moveResult, ds.submit(task));

                    }

                }
                { // POS

                    final String indexPartitionName = IndexEnum.POS
                            .getFQN(namespace);

                    final List<BuildResult> lst = buildResults
                            .get(indexPartitionName);

                    if (lst == null)
                        throw new AssertionError("No results? "
                                + indexPartitionName);

                    final BuildResult[] a = lst.toArray(new BuildResult[] {});

                    // this one stays here.
                    localPOS = a[0];

                    // note: leave the first result in place here.
                    for (int i = 1; i < a.length; i++) {

                        final BuildResult buildResult = a[i];
                        
                        final int index = (i - 1) % moveServices.length;

                        final UUID targetDataServiceUUID = moveTargets[index];

                        final IDataService ds = moveServices[index];

                        final IndexMetadata indexMetadata = getIndex(
                                indexPartitionName).getIndexMetadata();

                        final LocalPartitionMetadata pmd = indexMetadata
                                .getPartitionMetadata();

                        final int newPartitionId = resourceManager
                                .nextPartitionId(indexMetadata.getName());

                        final PartitionLocator oldLocator = new PartitionLocator(//
                                pmd.getPartitionId(),//
                                resourceManager.getDataServiceUUID(),//
                                pmd.getLeftSeparatorKey(),//
                                pmd.getRightSeparatorKey()//
                        );

                        final PartitionLocator newLocator = new PartitionLocator(
                                newPartitionId,//
                                targetDataServiceUUID,//
                                pmd.getLeftSeparatorKey(),//
                                pmd.getRightSeparatorKey()//
                        );

                        final MoveResult moveResult = new MoveResult(
                                indexPartitionName, indexMetadata,
                                targetDataServiceUUID, newPartitionId,
                                oldLocator, newLocator);
                        
                        final Callable<Void> task = new CopyIndexSegmentAndCreateIndexPartition(
                                resourceManager, indexPartitionName,
                                newPartitionId, indexMetadata,
                                targetDataServiceUUID,
                                buildResult.segmentMetadata);

                        scatterSplitFutures.put(moveResult, ds.submit(task));

                    }

                }
                { // await outcomes.

                    for (Map.Entry<MoveResult, Future<? extends Object>> entry : scatterSplitFutures
                            .entrySet()) {

                        final AbstractResult result = entry.getKey();

                        final Future<? extends Object> future = entry
                                .getValue();

                        try {

                            /*
                             * @todo try/catch and register the index partition
                             * here instead. But only if the MDS state is
                             * knowable. And only if the failed task was a
                             * submitted to a remote data service.
                             */

                            future.get();

                        } catch (ExecutionException ex) {

                            throw new Exception("Move failed: " + result.name,
                                    ex);

                        }

                    }

                }
                
            } finally {
                
                sendSegment.end();
                
            }

            /*
             * FIXME SPO, OSP, ID2TERM : MUST REGISTER INDEX ON TARGET DS!
             * 
             * This needs to compute the new separator keys and then register
             * the index partitions. This should be done in parallel on the
             * different data services.
             * 
             * The SPO and OSP splits can use the same separator keys for the
             * new index partitions. The fromKeys will be formed as
             * {newPartitionId,0L,0L}, where newPartitionId is each new index
             * partition identifier for the ID2TERM index in turn.
             * 
             * The ID2TERM splits use separatorKeys which are formed from the
             * partitionId (the key for that index is just the term identifier
             * and the partitionId is the high word of the term identifier).
             */
            {
                
                for (int i = 0; i < moveTargets.length; i++) {
                    
                    final IDataService ds = moveServices[i];
                    
//                    ds.registerIndex(name, metadata);
                    if (true)
                        throw new UnsupportedOperationException();

                }
                
            }

            /*
             * Atomic update.
             * 
             * Note: The TERM2ID and POS indices are similar to a normal move.
             * We need to register an index partition on the target data
             * service. [Note: If the atomic update for any of the TERM2ID or
             * POS splits fails, then we register the index partition here
             * instead rather than dying.] We also create the index partition on
             * this data service for the TERM2ID and POS splits that will be
             * staying here rather than moving to another data service.
             * 
             * After the atomic update all clients will be redirected. Any write
             * tasks for this RDF DB which are already queued up on this data
             * service will fail with a StaleLocatorException. The client will
             * see that exception and automatically re-split if necessary and
             * redirect the tasks to the appropriate data service(s).
             * 
             * @todo Make sure that we delete the output index partitions after
             * the operation since they are no longer required by the source
             * data service. We can delete them in a finally clause here. If the
             * segment was copied to another data service and then this task
             * dies, then we also need to delete the index segment on the other
             * data service (@todo unless that will happen automatically once it
             * is no longer part of any view and in that case we need to make
             * sure that it is strongly held until we are done with this
             * operation!)
             * 
             * @todo Since these are not index segments for an partition that
             * will live on this data service they really should not be entered
             * into the set of managed resources, except that we need to do that
             * in order for them to be "sendable". Again, they MUST be protected
             * against purgeResources() until the move is over. Maybe generate
             * them in the temporary directory under an assumed name and copy
             * them into the temporary directory under an assumed name and then
             * move them into place during the atomic update? Note that this
             * will involve a file copy unless we set aside a directory on the
             * data service explicitly for incoming files (outgoing files can be
             * placed into the real tmp dir). We will also have to explicitly
             * allow the outgoing files to be sent with a hook for the
             * ResourceService.
             */
            final Event atomicUpdate = e.newSubEvent(
                    OverflowSubtaskEnum.AtomicUpdate, "").start();
            try {
               
                /*
                 * FIXME atomic update for the leave behind partitions for
                 * ID2TERM, SPO, and OSP. All of these need to be bundled as a
                 * single "SPLIT" notice for the MDS even though some are
                 * "moves" we still report them as a 1:N split. This means that
                 * we must already have the new "leave behind" index partitions
                 * in place and must assemble the old/new locators here from the
                 * MoveResults and the BuildResult for the leave behind index
                 * partition.
                 */
                for (MoveResult moveResult : scatterSplitFutures.keySet()) {

                    // atomic update on the metadata server.
                    resourceManager.getFederation().getMetadataService()
                            .moveIndexPartition(
                                    moveResult.indexMetadata.getName(),
                                    moveResult.oldLocator,
                                    moveResult.newLocator);

                    /*
                     * Set flag indicating that clients will now see the new index
                     * partition.
                     * 
                     * FIXME This flag is not being processed and there is no error
                     * handling. This is because the error handling would have to
                     * run as an error action on the outer AbstractTask. Perhaps it
                     * is time to add that feature? Also add an after action for a
                     * successfull task.
                     */
                    moveResult.registeredInMDS.set(true);

                    // will notify tasks that index partition has moved.
                    resourceManager.setIndexPartitionGone(getOnlyResource(),
                            StaleLocatorReason.Move);

                    // notify successful index partition move.
                    resourceManager.indexPartitionMoveCounter.incrementAndGet();

                    /*
                     * Note: The new index partition will not be usable until (and
                     * unless) this task commits.
                     */
                    
                }

                /*
                 * FIXME atomic update for scattered empty tails for the
                 * ID2TERM, SPO, and OSP. This is just an MDS update since these
                 * index partitions already exist in all the right locations.
                 */
                
                if (true)
                    throw new UnsupportedOperationException();

                /*
                 * FIXME atomic update for the leave behind partitions for
                 * TERM2ID and POS. We have to create the appropriate views on
                 * the live journal (unless that is done above in the prepare
                 * phase).  Then update the MDS.
                 */
                if (true)
                    throw new UnsupportedOperationException();
                
                /*
                 * Drop the old index partitions. This action will be rolled
                 * back automatically if this task fails so the source index
                 * partition will remain available if the move fails.
                 */
                for (String r : getResource()) {

                    getJournal().dropIndex(r);

                }

            } finally {
                
                atomicUpdate.end();
                
            }
            
            // Done.
            return null;

        } finally {

            e.end();

        }
        
    }

    /**
     * Determine the split points for the TERM2ID or POS index. Both of these
     * indices are split into N equal parts so we can use the same logic for
     * both.
     * <p>
     * Note: nsplits should be the #of target data services PLUS ONE (1) so that
     * you have one left over that will stay on this data service.
     * 
     * @param nsplits
     *            The #of splits to generate.
     * 
     * @return The split points.
     */
    protected Split[] getSplits(final ILocalBTreeView ndx, final int nsplits) {

        if (ndx.getSourceCount() != 2) {

            throw new IllegalStateException(
                    "Expecting 2 sources: the old journal: #sources="
                            + ndx.getSourceCount());

        }

        final long rangeCount = ndx.rangeCount();

        if (rangeCount > Integer.MAX_VALUE) {

            throw new RuntimeException("Range count exceeds 32bits.");

        }

        final int entryCount = (int) rangeCount; 

        if (entryCount < nsplits) {

            throw new RuntimeException("Can not split " + entryCount
                    + " tuples into " + nsplits + " index partitions.");
            
        }
  
        final int minEntryCount = 1;
        final int entryCountPerSplit = entryCount / nsplits;
        final double overCapacityMultiplier = 1d;
        final double underCapacityMultiplier = 1d;
        final int sampleRate = 20;

        final DefaultSplitHandler splitHandler = new DefaultSplitHandler(
                minEntryCount, entryCountPerSplit, overCapacityMultiplier,
                underCapacityMultiplier, sampleRate);

        final Split[] splits = splitHandler.getSplits(resourceManager, ndx);
        
        if (splits == null) {

            throw new RuntimeException("Did not split index?");

        }

        log.warn("Will create " + splits.length + " splits for "
                + ndx.getIndexMetadata().getName() + " : "
                + Arrays.toString(splits));

        return splits;

    }

    /**
     * Returns a list of tasks which will build the index partitions described
     * for each {@link Split} of the specified index.
     * 
     * @param indexPartitionName
     * @param src
     * @param commitTime
     * @param splits
     * @param parentEvent
     * @return
     */
    protected List<Callable<BuildResult>> getBuildTasks(
            final String indexPartitionName, final ILocalBTreeView src,
            final long commitTime, final Split[] splits, final Event parentEvent) {

        // validate the splits before processing them.
        SplitUtility.validateSplits(src, splits);

        final List<Callable<BuildResult>> tasks = new LinkedList<Callable<BuildResult>>();
        
        for (Split split : splits) {

            final LocalPartitionMetadata pmd = (LocalPartitionMetadata) split.pmd;

            final byte[] fromKey = pmd.getLeftSeparatorKey();

            final byte[] toKey = pmd.getRightSeparatorKey();

            if (fromKey == null && toKey == null) {

                /*
                 * Note: This is not legal because it implies that we are
                 * building the index segment from the entire source key range -
                 * hence not a split at all!
                 */

                throw new RuntimeException("Not a key-range?");

            }

            // build the index segment from the key range.
            tasks.add(new Callable<BuildResult>() {
                public BuildResult call() throws Exception {

                    return resourceManager.buildIndexSegment(
                            indexPartitionName, src,
                            true/* compactingMerge */, commitTime, fromKey,
                            toKey, parentEvent);

                }
            });

        }

        return tasks;

    }
    
    /**
     * Returns a set of distinct {@link UUID}s for data services (NOT including
     * the data service on which we are running). These data services will be
     * the targets for the move operations.
     * 
     * @return
     * 
     * @throws IOException
     * @throws TimeoutException
     * @throws InterruptedException
     */
    protected UUID[] getTargetDataServices() throws IOException,
            TimeoutException, InterruptedException {

        /*
         * @todo useLBS should be figured out based on whether or not it has
         * been discovered and how "good" its host and service load scores are.
         * How good is basically a question of how long it has been running (and
         * how good it is at handling a variety of edge cases :-0)
         */
        final boolean useLBS = false;
        final UUID a[];
        if (useLBS) {
            /*
             * Note: This uses the LBS to find the least utilized data services.
             * This makes sense in a pre-existing federation with a lot of data
             * and decent host and service scores.
             */
            a = fed.getLoadBalancerService().getUnderUtilizedDataServices(
                    minDataServices, maxDataServices,
                    resourceManager.getDataServiceUUID()/* exclude */);
        } else {
            /*
             * Note: This removes the dependecy on the LBS by just using up to
             * maxDataServices discovered data services.
             */
            a = fed.getDataServiceUUIDs(maxDataServices);
        }

        final HashSet<UUID> distinct = new HashSet<UUID>(a.length);

        for (UUID t : a) {

            if (distinct.add(t)) {

                if (resourceManager.getDataServiceUUID().equals(t)) {

                    throw new AssertionError("Move not allowed to self");

                }
                
            }
            
        }
        
        return distinct.toArray(new UUID[0]);
        
    }
    
    /**
     * Build all index segments in parallel.
     * 
     * @param tasks
     *            The task to build each index segment.
     * 
     * @return A {@link Map} associating each source index partition name with
     *         the {@link BuildResult} for each {@link IndexSegment} generated
     *         for that index partition. There will be N such
     *         {@link BuildResult}s for the TERM2ID and POS indices. There will
     *         be ONE (1) such {@link BuildResult} for each of the ID2TERM, SPO,
     *         and OSP indices.
     * 
     * @throws InterruptedException
     * @throws ExecutionExceptions
     */
    protected Map<String, List<BuildResult>> buildSplits(
            final List<Callable<BuildResult>> tasks)
            throws InterruptedException, ExecutionExceptions {

        if (tasks == null)
            throw new IllegalArgumentException();

        // submit and await completion.
        final List<Future<BuildResult>> futures = fed.getExecutorService()
                .invokeAll(tasks);

        int nbuilt = 0;
        final Map<String, List<BuildResult>> results = new LinkedHashMap<String, List<BuildResult>>();
        final List<Throwable> causes = new LinkedList<Throwable>();
        for (Future<BuildResult> f : futures) {

            try {

                // get the outcome of the task.
                final BuildResult result = f.get();

                // associate the outcome with the appropriate index in the
                // map.
                List<BuildResult> lst = results.get(result.name);

                if (lst == null) {

                    lst = new LinkedList<BuildResult>();

                    results.put(result.name, lst);

                }

                lst.add(result);
                nbuilt++;

            } catch (Throwable t) {

                causes.add(t);

                log.error(t.getLocalizedMessage());

            }

        }

        if (!causes.isEmpty()) {

            /*
             * Error handling - remove all generated files.
             */

            for (List<BuildResult> list : results.values()) {

                if (list == null)
                    continue;

                for (BuildResult result : list) {

                    resourceManager.deleteResource(result.segmentMetadata
                            .getUUID(), false/* isJournal */);

                }

            }

            // throw wrapped set of exceptions.
            throw new ExecutionExceptions(causes);

        }

        if (INFO)
            log.info("Generated " + nbuilt + " index segments");

        return results;

    }
    
    /**
     * Task receives copies an {@link IndexSegment} from the sender's
     * {@link DataService} and creates a new index partition on the receiver's
     * {@link DataService} whose view is comprised of a newly registered
     * {@link BTree} on the live journal and the copied {@link IndexSegment}.
     * <p>
     * Preconditions:
     * <ul>
     * <li>The task must be submitted to the target data service while the
     * caller is holding an exclusive write lock on the source index partition.</li>
     * <li>The {@link IndexSegment} must contain ALL data for the new index
     * partition.</li>
     * </ul>
     * Note: Clients WILL NOT be directed to the new index partition until the
     * MDS has been updated. The caller is responsible for atomically updating
     * the MDS, dropping the old index partition, and deleting the copied
     * {@link IndexSegment} since it is now living on the target
     * {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class CopyIndexSegmentAndCreateIndexPartition implements Callable<Void>,
            Serializable, IDataServiceAwareProcedure {
        
        /**
         * 
         */
        private static final long serialVersionUID = 2865961936767512748L;

        /**
         * The {@link IndexMetadata} that will be used to register the new index
         * partition on the target data service.
         */
        final IndexMetadata indexMetadata;

        /**
         * Name of the scale-out index.
         */
        final String scaleOutIndexName;
        /**
         * Name of the source index partition.
         */
        final String sourceIndexName;
        /**
         * Name of the target index partition.
         */
        final String targetIndexName;
        /**
         * The partition identifier for the source index partition.
         */
        final int oldPartitionId;
        /**
         * The partition identifier for the target index partition.
         */
        final int newPartitionId;
        
        /**
         * The description of the resource that will be read.
         */
        final IResourceMetadata resourceMetadata;

        /**
         * The address of the {@link ResourceService} which can be used to read
         * the resource.
         */
        final InetAddress addr;

        /**
         * The port used to connect to the {@link ResourceService}.
         */
        final int port;

        /**
         * 
         * @param resourceMetadata
         *            Metadata identifies the resource to be received.
         * 
         * @throws UnknownHostException
         *             if the {@link InetAddress} for this host can not be
         *             obtained.
         * 
         * @todo javadoc and check args.
         */
        public CopyIndexSegmentAndCreateIndexPartition(
                final ResourceManager resourceManager,
                final String sourceIndexName,
                final int newPartitionId,
                final IndexMetadata indexMetadata,
                final UUID targetDataServiceUUID,
                final IResourceMetadata resourceMetadata)
                throws UnknownHostException {

            if (resourceManager == null)
                throw new IllegalArgumentException();

            if (indexMetadata == null)
                throw new IllegalArgumentException();

            if (resourceMetadata == null)
                throw new IllegalArgumentException();

            // Note: clone the index metadata since we are going to modify it.
            this.indexMetadata = indexMetadata.clone();
            
            this.addr = InetAddress.getLocalHost();

            this.port = resourceManager.getResourceServicePort();

            this.resourceMetadata = resourceMetadata;

            this.scaleOutIndexName = indexMetadata.getName();

            this.sourceIndexName = sourceIndexName;

            this.targetIndexName = DataService.getIndexPartitionName(
                    indexMetadata.getName(), newPartitionId);

            this.oldPartitionId = indexMetadata.getPartitionMetadata()
                    .getPartitionId();

            this.newPartitionId = newPartitionId;

            final String summary = OverflowActionEnum.Move + "("
                    + sourceIndexName + "->" + targetIndexName + ")";

            // the partition metadata for the source index partition.
            final LocalPartitionMetadata oldpmd = indexMetadata
                    .getPartitionMetadata();

            indexMetadata.setPartitionMetadata(new LocalPartitionMetadata(//
                    newPartitionId,//
                    // The source partition identifier.
                    oldpmd.getPartitionId(), oldpmd.getLeftSeparatorKey(),//
                    oldpmd.getRightSeparatorKey(),//
                    /*
                     * Note: This is [null] to indicate that the resource
                     * metadata needs to be filled in by the target data service
                     * when the new index partition is registered. It will be
                     * populated with the resource metadata description for the
                     * live journal on that data service.
                     */
                    null,
                    // history line.
                    oldpmd.getHistory() + summary + " "));

            if (INFO)
                log.info(summary);
            
        }

        /**
         * Runs on a remote {@link DataService} and transfers the specified
         * resource into its local data directory and registers it with the
         * local {@link ResourceManager}.
         */
        public Void call() throws Exception {
            
            if (dataService == null) {

                // this happens if you do not submit it to a data service.
                throw new IllegalStateException();
                
            }

            /*
             * FIXME Since the file (in this case an index segment) is being
             * transferred from one data service to another the dataDir will be
             * different and we need to make sure that the file winds up in the
             * correct directory for the scale-out index to which it belongs.
             */
            final File file = new File(dataService.getResourceManager()
                    .getDataDir(), resourceMetadata.getFile());

            file.getParentFile().mkdirs();
            
            new ResourceService.ReadResourceTask(addr, port, resourceMetadata
                    .getUUID(), file);
            
            dataService.getResourceManager()
                    .addResource(resourceMetadata, file);

            /*
             * Register new index partition on the target data service.
             */
            dataService.registerIndex(targetIndexName, indexMetadata);

            if (INFO)
                log
                        .info("Registered new index partition on target data service: targetIndexName="
                                + targetIndexName);

            return null;
            
        }

        public void setDataService(DataService dataService) {
            
            this.dataService = dataService;

        }

        private transient DataService dataService;

    }

//    protected void doAtomicLeaveBehindUpdate(final SplitResult splitResult,final int leaveBehindIndex) {
//        
//        final ILocalBTreeView src = getIndex(splitResult.name);
//        
//            // new metadata record (cloned).
//            final IndexMetadata md = src.getIndexMetadata().clone();
//
//            final LocalPartitionMetadata pmd = (LocalPartitionMetadata) splitResult.splits[leaveBehindIndex].pmd;
//
//            // the new partition identifier.
//            final int partitionId = pmd.getPartitionId();
//
//            // name of the new index partition.
//            final String name2 = DataService.getIndexPartitionName(
//                    md.getName(), partitionId);
//
//            /*
//             * form locator for the new index partition for this split..
//             */
//            final PartitionLocator locator = new PartitionLocator(pmd
//                    .getPartitionId(),//
//                    resourceManager.getDataServiceUUID(),//
//                    pmd.getLeftSeparatorKey(),//
//                    pmd.getRightSeparatorKey()//
//            );
//
//            final String summary = OverflowActionEnum.Split+ "(" + splitResult.name + "->" + name2
//                    + ")";
//
//            /*
//             * Update the view definition.
//             */
//            md
//                    .setPartitionMetadata(new LocalPartitionMetadata(
//                            pmd.getPartitionId(),//
//                            -1, // Note: Split not allowed during move.
//                            pmd.getLeftSeparatorKey(),//
//                            pmd.getRightSeparatorKey(),//
//                            new IResourceMetadata[] {//
//                                    /*
//                                     * Resources are (a) the new btree; and (b)
//                                     * the new index segment.
//                                     */
//                                    resourceManager.getLiveJournal()
//                                            .getResourceMetadata(),
//                                    splitResult.buildResults[leaveBehindIndex].segmentMetadata },
//                            /*
//                             * Note: history is record of the split.
//                             */
//                            pmd.getHistory() + summary + " ")//
//                    );
//
//            /*
//             * create new btree.
//             * 
//             * Note: the lower 32-bits of the counter will be zero. The
//             * high 32-bits will be the partition identifier assigned to
//             * the new index partition.
//             */
//            final BTree btree = BTree.create(resourceManager
//                    .getLiveJournal(), md);
//
//            // make sure the partition identifier was asserted.
//            assert partitionId == btree.getIndexMetadata()
//                    .getPartitionMetadata().getPartitionId();
//
//            final long newCounter = btree.getCounter().get();
//
//            // lower bound (inclusive)
//            final byte[] fromKey = pmd.getLeftSeparatorKey();
//
//            // upper bound (exclusive)
//            final byte[] toKey = pmd.getRightSeparatorKey();
//
//            // register it on the live journal
//
//            if (INFO)
//                log.info("Registering index: " + name2);
//
//            getJournal().registerIndex(name2, btree);
//
//            // clone the current metadata record for the live index.
//            final IndexMetadata indexMetadata = btree.getIndexMetadata()
//                    .clone();
//
//            /*
//             * This is the index partition definition on the live index -
//             * the one that will be replaced with a new view as the result
//             * of this atomic update.
//             */
//            final LocalPartitionMetadata currentpmd = indexMetadata
//                    .getPartitionMetadata();
//
//            /*
//             * Notify the metadata service that the index partition has been
//             * split.
//             */
//            final LocalPartitionMetadata oldpmd = btree.getIndexMetadata().getPartitionMetadata();
//            resourceManager.getFederation().getMetadataService()
//                    .splitIndexPartition(src.getIndexMetadata().getName(),//
//                            new PartitionLocator(//
//                                    oldpmd.getPartitionId(), //
//                                    resourceManager.getDataServiceUUID(), //
//                                    oldpmd.getLeftSeparatorKey(),//
//                                    oldpmd.getRightSeparatorKey()//
//                            ), locators);
//
//            // will notify tasks that index partition was split.
//            resourceManager.setIndexPartitionGone(splitResult.name,
//                    StaleLocatorReason.Split);
//
//    }
    
}

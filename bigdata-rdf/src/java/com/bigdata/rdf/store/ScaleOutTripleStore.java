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
 * Created on May 19, 2007
 */

package com.bigdata.rdf.store;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IProcedure;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.OptimizedValueFactory.TermIdComparator;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.model.OptimizedValueFactory._ValueSortKeyComparator;
import com.bigdata.rdf.util.RdfKeyBuilder;
import com.bigdata.service.AutoSplitProcedure;
import com.bigdata.service.BigdataFederation;
import com.bigdata.service.ClientIndexView;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.ClientIndexView.Split;

/**
 * Implementation of an {@link ITripleStore} as a client of a
 * {@link BigdataFederation}. The implementation supports a scale-out
 * architecture in which each index may have one or more partitions. Index
 * partitions are multiplexed onto {@link IDataService}s.
 * <p>
 * The client uses unisolated writes against the lexicon (terms and ids indices)
 * and the statement indices. The index writes are automatically broken down
 * into one split per index partition. While each unisolated write on an index
 * partition is ACID, the indices are fully consistent iff the total operation
 * is successfull. For the lexicon, this means that the write on the terms and
 * the ids index must both succeed. For the statement indices, this means that
 * the write on each access path must succeed. If a client fails while adding
 * terms, then it is possible for the ids index to be incomplete with respect to
 * the terms index (i.e., terms are mapped into the lexicon and term identifiers
 * are assigned but the reverse lookup by term identifier will not discover the
 * term). Likewise, if a client fails while adding statements, then it is
 * possible for some of the access paths to be incomplete with respect to the
 * other access paths (i.e., some statements are not present in some access
 * paths).
 * <p>
 * Two additional mechanisms are used in order to guarentee reads from only
 * fully consistent data. First, clients providing query answering should read
 * from a database state that is known to be consistent (by using a read-only
 * transaction whose start time is the globally agreed upon commit time for that
 * database state). Second, if a client operation fails then it must be retried.
 * Such fail-safe retry semantics are available when data load operations are
 * executed as part of a map-reduce job.
 * <p>
 * 
 * FIXME refactor to run against either the concurrent journal API or the data
 * service API and verify that load, inference, and query work correctly and
 * that the memory cap on inference has been removed. Examine implementation for
 * hidden performance costs (there should be no new performance hits when
 * compared to the current scale-out version) and examine ways to reduce costs
 * and increase parallelism -- if this works out nicely then we can converge the
 * scale-out and local implementations! Test 1st on an embedded federation. Note
 * that we will want to turn off auto-commit and index check points for index
 * operations in order to be competitive with the bulk scale-out of the local
 * implementation.
 * 
 * @todo test with indices split into more than one partition and with parallel
 *       processing of batch operation and procedure splits using a thread pool.
 * 
 * @todo test with concurrent threads running load of many small files, such as
 *       LUBM.
 * 
 * @todo provide a mechanism to make document loading robust to client failure.
 *       When loads are unisolated, a client failure can result in the
 *       statements being loaded into only a subset of the statement indices.
 *       robust load would require a means for undo or redo of failed loads. a
 *       loaded based on map/reduce would naturally provide a robust mechanism
 *       using a redo model.
 * 
 * @todo run various tests against all implementations and tune up the network
 *       protocol. Examine dictinary and hamming codes and parallelization of
 *       operations. Write a distributed join.
 * 
 * @todo write a stress test with concurrent threads inserting terms and having
 *       occasional failures between the insertion into terms and the insertion
 *       into ids and verify that the resulting mapping is always fully
 *       consistent because the client verifies that the term is in the ids
 *       mapping on each call to addTerm().
 * 
 * @todo write a performance test with (a) a single client thread; and (b)
 *       concurrent clients/threads loaded terms into the KB and use it to tune
 *       the batch load operations (there is little point to tuning the single
 *       term load operations).
 * 
 * @todo Each unisolated write results in a commit. This means that a single
 *       client will run more slowly since it must perform more commits when
 *       loading the data. However, if we support group commit on the data
 *       service then that will have a big impact on concurrent load rates since
 *       multiple clients can write on the same data service before the next
 *       commit. (This does not work for a single client since the write must
 *       commit before control returns to the client.)
 * 
 * @todo test consistent concurrent load.
 * 
 * @todo test query (LUBM).
 * 
 * @todo provide read against historical state and periodically notify clients
 *       when there is a new historical state that is complete (data are loaded
 *       and closure exists). this will prevent partial reads of data during
 *       data load.
 * 
 * @todo Very large bulk data load.
 * 
 * @todo write utility class to create and pre-partition a federated store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ScaleOutTripleStore extends AbstractTripleStore {

    private final IBigdataFederation fed;
    
    /**
     * 
     */
    public ScaleOutTripleStore(IBigdataFederation fed, Properties properties) {

        super( properties );

        if (fed == null)
            throw new IllegalArgumentException();

        // @todo throw ex if client not connected to the federation.
        
        this.fed = fed;

        /*
         * Conditionally register the necessary indices.
         * 
         * @todo right now they are created when the federation is created.
         */
//        registerIndices();
        
    }

    /**
     * Register the indices.
     * 
     * @todo default allocation of the terms, statements, and justifications
     *       index (the latter iff justifications are configured). The default
     *       allocation scheme should be based on expectations of data volume
     *       written or read, the benefits of locality for the indices, and the
     *       concurrency of read or write operations on those indices.
     * 
     * @todo handle exception if the index already exists.
     * 
     * @todo you should not be able to turn off the lexicon for the scale-out
     *       triple store (or for the local triple store). That option only
     *       makes sense for the {@link TempTripleStore}.
     * 
     * @todo registration of indices should be atomic. this can be achieved
     *       using a procedure that runs once it has a lock on the resource
     *       corresponding to each required scale-out index. At that point the
     *       indices either will or will not exist and we can create them or
     *       accept their pre-existence atomically.
     */
    final public void registerIndices() {

        final IBigdataClient client = fed.getClient();
        
        // all known data service UUIDs.
        final UUID[] uuids = client.getDataServiceUUIDs(0);
    
        if (false && uuids.length == 2 && lexicon && !oneAccessPath) {

            /*
             * Special case for (2) data services attempts to balance the write
             * volume and concurrent write load for the indices.
             * 
             * dataService0: terms, spo
             * 
             * dataService1: ids, pos, osp, just (if used)
             * 
             * @todo This appears to slow things down slightly when loading
             * Thesaurus.owl. Try again with a concurrent load scenario and see
             * what interaction may be occurring with group commit. Also look at
             * the effect of check pointing indices (rather than doing a commit)
             * and of either check pointing nor committing groups (a special
             * procedure could be used to do a commit) in order to simulate the
             * best case scenario for continuous index load. (Both check point
             * and commit may help to keep down GC since they will limit the
             * life span of a mutable btree node, but they will mean more IO
             * unless we retain nodes on a read retention queue for the index
             * and in any case it will mean more conversion of immutable nodes
             * back to mutable nodes.)
             */

            log.warn("Special case allocation for two data services");
            
            fed.registerIndex(name_termId, new byte[][] { new byte[] {} },
                    new UUID[] { uuids[0] });
            
            fed.registerIndex(name_idTerm, new byte[][] { new byte[] {} },
                    new UUID[] { uuids[1] });
            
            if(justify) {
                /*
                 * @todo review this decision when tuning the scale-out store
                 * for inference.  also, consider the use of bloom filters for
                 * inference since there appears to be a large number of queries
                 * resulting in small result sets (0 to 5 statements).
                 */
                fed.registerIndex(name_just, new byte[][] { new byte[] {} },
                        new UUID[] { uuids[1] });
            }
            
            /*
             * @todo could pre-partition based on the expected #of statements
             * for the store. If we want on the order of 20M statements per
             * partition and we expect at least 2B statements then we can
             * compute the #of required partitions. Since this is static
             * partitioning it will not be exact. This means that you can have
             * more statements in some partitions than in others - and this will
             * vary across the different access paths. It also means that the
             * last partition will absorb all statements beyond the expected
             * maximum.
             * 
             * The separator keys would be formed from the term identifiers that
             * would be assigned as [id:NULL:NULL]. We can use the same
             * separator keys for each of the statement indices.
             * 
             * Note: The term identifiers will be strictly incremental up to ~30
             * bits per index partition for the term:ids index (the index that
             * assigns the term identifiers). If there are multiple partitions
             * of the terms:ids index then the index partition identifier will
             * be non-zero after the first terms:ids index partition and the
             * logic to compute the ids for forming the statement index
             * separator keys would have to be changed.
             */
            
            fed.registerIndex(name_spo,new byte[][] { new byte[] {} },
                    new UUID[] { uuids[0] });
            
            fed.registerIndex(name_pos, new byte[][] { new byte[] {} },
                    new UUID[] { uuids[1] });
            
            fed.registerIndex(name_osp, new byte[][] { new byte[] {} },
                    new UUID[] { uuids[1] });
            
            return;
            
        }
        
        /*
         * Allocation of index partitions to data services is governed by the
         * metadata service.
         */
        
        if(lexicon) {

            fed.registerIndex(name_termId);
        
            fed.registerIndex(name_idTerm);

        }

        if (oneAccessPath) {

            fed.registerIndex(name_spo);
            
        } else {
            
            fed.registerIndex(name_spo);
            
            fed.registerIndex(name_pos);
            
            fed.registerIndex(name_osp);
            
        }

        if(justify) {

            fed.registerIndex(name_just);
            
        }

    }
    
    /**
     * @todo this must be an atomic drop/add or concurrent clients will not have
     *       a coherent view of the database during a {@link #clear()}. That
     *       could be achieved using a procedure that runs on the metadata
     *       service and which handles the drop/add while holding a lock on the
     *       resources corresponding to the indices to be dropped/added.
     */
    final public void clear() {

        if(true) {
            
            /*
             * FIXME we need to drop the indices from the federation!
             * 
             * Right now the logic has not been implemented to drop the mdi and
             * partitions for a scale-out index!
             */

            log.warn("request ignored!");
            
            return;
        
        }

        if (lexicon) {
         
            fed.dropIndex(name_idTerm); ids = null;
            
            fed.dropIndex(name_termId); terms = null;
        
        }
        
        if(oneAccessPath) {
            
            fed.dropIndex(name_spo); spo = null;
            
        } else {
            
            fed.dropIndex(name_spo); spo = null;
            
            fed.dropIndex(name_pos); pos = null;
            
            fed.dropIndex(name_osp); osp = null;
            
        }
    
        if(justify) {

            fed.dropIndex(name_just); just = null;
            
        }
        
        registerIndices();
        
    }
    
    /**
     * The terms index.
     */
    private ClientIndexView terms;

    /**
     * The ids index.
     */
    private ClientIndexView ids;

    /**
     * The statement indices for a triple store.
     */
    private ClientIndexView spo, pos, osp;

    private ClientIndexView just;

    final public IIndex getTermIdIndex() {

        if (terms == null) {

            terms = (ClientIndexView) fed.getIndex(
                    IBigdataFederation.UNISOLATED, name_termId);

        }

        return terms;

    }

    final public IIndex getIdTermIndex() {

        if (ids == null) {

            ids = (ClientIndexView) fed.getIndex(IBigdataFederation.UNISOLATED,
                    name_idTerm);

        }

        return ids;

    }

    final public IIndex getSPOIndex() {

        if (spo == null) {

            spo = (ClientIndexView) fed.getIndex(IBigdataFederation.UNISOLATED,
                    name_spo);

        }

        return spo;

    }

    final public IIndex getPOSIndex() {

        if (pos == null) {

            pos = (ClientIndexView) fed.getIndex(IBigdataFederation.UNISOLATED,
                    name_pos);

        }

        return pos;

    }

    final public IIndex getOSPIndex() {

        if (osp == null) {

            osp = (ClientIndexView) fed.getIndex(IBigdataFederation.UNISOLATED,
                    name_osp);

        }

        return osp;

    }

    final public IIndex getJustificationIndex() {

        if (just == null) {

            just = (ClientIndexView) fed.getIndex(
                    IBigdataFederation.UNISOLATED, name_just);

        }

        return just;
        
    }

    /*
     * terms index.
     */
    
// final public _Value getTerm(long id) {
//
// byte[] data = (byte[])getIdTermIndex().lookup(keyBuilder.id2key(id));
//
//        if (data == null)
//            return null;
//
//        return _Value.deserialize(data);
//
//    }
//
//    final public long getTermId(Value value) {
//
//        if(value==null) return IRawTripleStore.NULL;
//        
//        _Value val = (_Value) OptimizedValueFactory.INSTANCE
//                .toNativeValue(value);
//        
//        if( val.termId != IRawTripleStore.NULL ) return val.termId; 
//
//        Object tmp = getTermIdIndex().lookup(keyBuilder.value2Key(value));
//        
//        if (tmp == null)
//            return IRawTripleStore.NULL;
//
//        try {
//
//            val.termId = new DataInputBuffer((byte[]) tmp).unpackLong();
//
//        } catch (IOException ex) {
//
//            throw new RuntimeException(ex);
//
//        }
//
//        return val.termId;
//
//    }
    
    /**
     * This implementation is designed to use unisolated batch writes on the
     * terms and ids index that guarentee consistency.
     * 
     * FIXME parallelize processing against index partitions in
     * {@link ClientIndexView} and {@link AutoSplitProcedure}
     * 
     * FIXME make writing justifications a batch process.
     * 
     * @todo if the auto-split of a procedure can be made efficient enough then
     *       we can use the same code for the embedded and scale-out versions.
     * 
     * @todo "known" terms should be filtered out of this operation.
     *       <p>
     *       A term is marked as "known" within a client if it was successfully
     *       asserted against both the terms and ids index (or if it was
     *       discovered on lookup against the ids index).
     *       <p>
     *       We should also check the {@link AbstractTripleStore#termCache} for
     *       known terms.
     */
    public void addTerms(RdfKeyBuilder keyBuilder,final _Value[] terms, final int numTerms) {
        
        if (numTerms == 0)
            return;

        long begin = System.currentTimeMillis();
        long keyGenTime = 0; // time to convert unicode terms to byte[] sort keys.
        long sortTime = 0; // time to sort terms by assigned byte[] keys.
        long insertTime = 0; // time to insert terms into the forward and reverse index.
        
        {

            /*
             * First make sure that each term has an assigned sort key.
             */
            {

                long _begin = System.currentTimeMillis();
                
                generateSortKeys(keyBuilder, terms, numTerms);
                
                keyGenTime = System.currentTimeMillis() - _begin;

            }
            
            /*
             * Sort terms by their assigned sort key. This places them into the
             * natural order for the term:id index.
             */
            {
            
                long _begin = System.currentTimeMillis();
                
                Arrays.sort(terms, 0, numTerms, _ValueSortKeyComparator.INSTANCE);

                sortTime += System.currentTimeMillis() - _begin;
                
            }

            /*
             * For each term that does not have a pre-assigned term identifier,
             * execute a remote unisolated batch operation that assigns the term
             * identifier.
             */
            {
                
                final long _begin = System.currentTimeMillis();

                final ClientIndexView termIdIndex = (ClientIndexView)getTermIdIndex();

                /*
                 * Create a key buffer holding the sort keys. This does not
                 * allocate new storage for the sort keys, but rather aligns the
                 * data structures for the call to splitKeys().
                 */
                final byte[][] keys = new byte[numTerms][];
                {
                    
                    for(int i=0; i<numTerms; i++) {
                        
                        keys[i] = terms[i].key;
                        
                    }
                    
                }
                
                final AutoSplitProcedure proc = new AutoSplitProcedure<AddTerms.Result>(
                        numTerms, keys, null/* vals */) {

                    private static final long serialVersionUID = -3159579576628729679L;

                    protected IProcedure newProc(Split split) {
                        
                        return new AddTerms(split.ntuples,split.fromIndex,keys);
                        
                    }
                    
                    /**
                     * Copy the assigned/discovered term identifiers onto the
                     * corresponding elements of the terms[].
                     */
                    protected void aggregate(AddTerms.Result result, Split split) {
                        
                        for(int i=split.fromIndex, j=0; i<split.toIndex; i++, j++) {
                            
                            terms[i].termId = result.ids[j];
                            
                        }

                    }
                    
                };

                // run the procedure on the partitioned index.

                proc.apply(termIdIndex);
                
                insertTime += System.currentTimeMillis() - _begin;
                
            }
            
        }
        
        {
            
            /*
             * Sort terms based on their assigned termId (when interpreted as
             * unsigned long integers).
             */

            long _begin = System.currentTimeMillis();

            Arrays.sort(terms, 0, numTerms, TermIdComparator.INSTANCE);

            sortTime += System.currentTimeMillis() - _begin;

        }
        
        {
        
            /*
             * Add terms to the reverse index, which is the index that we use to
             * lookup the RDF value by its termId to serialize some data as
             * RDF/XML or the like.
             * 
             * Note: Every term asserted against the forward mapping [terms]
             * MUST be asserted against the reverse mapping [ids] EVERY time.
             * This is required in order to assure that the reverse index
             * remains complete and consistent. Otherwise a client that writes
             * on the terms index and fails before writing on the ids index
             * would cause those terms to remain undefined in the reverse index.
             */
            
            final long _begin = System.currentTimeMillis();
            
            final ClientIndexView idTermIndex = (ClientIndexView)getIdTermIndex();

            /*
             * Create a key buffer to hold the keys generated from the term
             * identifers and then generate those keys. The terms are already in
             * sorted order by their term identifiers from the previous step.
             */
            final byte[][] keys = new byte[numTerms][]; // @todo use long[] instead
            final byte[][] vals = new byte[numTerms][];
            
            {

                // Private key builder removes single-threaded constraint.
                final KeyBuilder tmp = new KeyBuilder(Bytes.SIZEOF_LONG); 

                // buffer is reused for each serialized term.
                final DataOutputBuffer out = new DataOutputBuffer();
                
                for(int i=0; i<numTerms; i++) {
                    
                    keys[i] = tmp.reset().append(terms[i].termId).getKey();
                    
                    // Serialize the term.
                    vals[i] = terms[i].serialize(out.reset());                    

                }
                
            }

            final AutoSplitProcedure proc = new AutoSplitProcedure(numTerms,
                    keys, vals) {

                private static final long serialVersionUID = -2484002926793236065L;

                protected IProcedure newProc(Split split) {

                    return new AddIds(split.ntuples, split.fromIndex, keys, vals);

                }
                
                /**
                 * Since the unisolated write succeeded the client knows that
                 * the term is now in both the forward and reverse indices. We
                 * codify that knowledge by setting the [known] flag on the
                 * term.
                 */
                protected void aggregate(Object result, Split split) {
                    
                    for(int i=split.fromIndex, j=0; i<split.toIndex; i++, j++) {
                        
                        terms[i].known = true;

                    }
                    
                }
                
            };
            
            // run the procedure on the partitioned index.
            
            proc.apply(idTermIndex);
            
            insertTime += System.currentTimeMillis() - _begin;

        }

        long elapsed = System.currentTimeMillis() - begin;
        
        if (numTerms > 1000 || elapsed > 3000) {

            log.info("Wrote " + numTerms + " in " + elapsed + "ms; keygen="
                    + keyGenTime + "ms, sort=" + sortTime + "ms, insert="
                    + insertTime + "ms");
            
        }
        
    }

    /**
     * FIXME Implement for the scale-out index (might be an identical
     * implementation).
     */
    protected void indexTermText(_Value[] terms, int numTerms) {

        throw new UnsupportedOperationException();

    }

    /** TODO Auto-generated method stub */
    public IIndex getFullTextIndex() {
        throw new UnsupportedOperationException();
    }

    /**
     * Adds reporting by data service to the usage summary.
     */
    public String usage(){
        
        StringBuilder sb = new StringBuilder( super.usage() );
        
        sb.append("\nsummary by dataService::\n");
        
        IBigdataClient client = fed.getClient();
        
        UUID[] dataServiceIds = client.getDataServiceUUIDs(0);
        
        for(int i=0; i<dataServiceIds.length; i++) {
            
            UUID serviceId = dataServiceIds[ i ];
            
            IDataService dataService = client.getDataService(serviceId);
            
            sb.append("\n");
            
            try {
            
                sb.append( dataService.getStatistics() );
                
            } catch (IOException e) {
                
                sb.append( "Could not get statistics for data service: uuid="+serviceId);
                
            }
            
        }
        
        return sb.toString();
        
    }

    /**
     * NOP since the client uses unisolated writes which auto-commit.
     */
    final public void commit() {
        
        if(INFO) log.info(usage());
        
    }
    
    /**
     * NOP since the client uses unisolated writes which auto-commit.
     * 
     * @todo verify that no write state is maintained by the
     *       {@link ClientIndexView} and that we therefore do NOT need to
     *       discard those views on abort.
     */
    final public void abort() {
        
    }
    
    /**
     * The federation is considered stable regardless of whether the federation
     * is on stable storage since clients only disconnect when they use
     * {@link #close()}.
     */
    final public boolean isStable() {
        
        return true;
        
    }
    
    final public boolean isReadOnly() {
        
        return false;
        
    }
    
    /**
     * Disconnects from the {@link IBigdataFederation}.
     */
    final public void close() {
        
        fed.disconnect();
        
        super.close();
        
    }

    /**
     * Drops the indices used by the {@link ScaleOutTripleStore} and disconnects
     * from the {@link IBigdataFederation}.
     */
    final public void closeAndDelete() {
        
        clear();
        
        fed.disconnect();
        
        super.closeAndDelete();
        
    }

    /**
     * @todo this is temporarily overriden in order to experiment with buffer
     *       capacity vs data transfer size for batch operations vs data
     *       compaction techniques for client-service RPC vs breaking down
     *       within index partition operations to no more than n megabytes per
     *       operation.
     */
    protected int getDataLoadBufferCapacity() {
        
        return 100000;
        
    }

}

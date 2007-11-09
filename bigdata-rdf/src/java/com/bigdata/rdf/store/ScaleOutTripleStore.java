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
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.openrdf.model.Value;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.MutableKeyBuffer;
import com.bigdata.btree.MutableValueBuffer;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.OptimizedValueFactory;
import com.bigdata.rdf.model.OptimizedValueFactory.TermIdComparator;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.model.OptimizedValueFactory._ValueSortKeyComparator;
import com.bigdata.rdf.rio.BulkRioLoader;
import com.bigdata.rdf.util.AddIds;
import com.bigdata.rdf.util.AddTerms;
import com.bigdata.service.BigdataFederation;
import com.bigdata.service.ClientIndexView;
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
 * @todo Very large bulk data load (using the {@link BulkRioLoader} to load
 *       {@link IndexSegment} directly). Reconcile as of the commit time of the
 *       bulk insert and you get to do that using efficient compacting
 *       sort-merges of "perfect" bulk index segments.
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
    public ScaleOutTripleStore(IBigdataFederation fed) {

        // @todo pass in the Unicode configuration.
        super(new Properties());

        if (fed == null)
            throw new IllegalArgumentException();

        this.fed = fed;

        // @todo create indices iff necessary.
        //
        // createIndices();
        
    }

    /**
     * Register the indices.
     * 
     * @todo this should be atomic and iff they do not exist.
     */
    final private void createIndices() {
        
         fed.registerIndex(name_termId);
         fed.registerIndex(name_idTerm);

         fed.registerIndex(name_spo);
         fed.registerIndex(name_pos);
         fed.registerIndex(name_osp);
         
         fed.registerIndex(name_just);

    }
    
    /** @todo this should be an atomic drop/add. */
    final public void clear() {

        fed.dropIndex(name_idTerm); ids = null;
        fed.dropIndex(name_termId); terms = null;
        
        fed.dropIndex(name_spo); spo = null;
        fed.dropIndex(name_pos); pos = null;
        fed.dropIndex(name_osp); osp = null;
    
        fed.dropIndex(name_just); just = null;
        
        createIndices();
        
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
        
        if(terms!=null) {
        
            terms = (ClientIndexView) fed.getIndex(IBigdataFederation.UNISOLATED,
                    name_termId);

        }
        
        return terms;
        
    }

    final public IIndex getIdTermIndex() {

        if(ids!=null) {
            
            ids = (ClientIndexView) fed.getIndex(IBigdataFederation.UNISOLATED,
                    name_idTerm);

        }
        
        return ids;
        
    }
    
    final public IIndex getSPOIndex() {
        
        if(spo!=null) {
            
            spo = (ClientIndexView) fed.getIndex(IBigdataFederation.UNISOLATED,
                    name_spo);

        }
        
        return spo;
        
    }
    
    final public IIndex getPOSIndex() {
        
        if(pos!=null) {
            
            pos = (ClientIndexView) fed.getIndex(IBigdataFederation.UNISOLATED,
                    name_pos);

        }
        
        return pos;
        
    }

    final public IIndex getOSPIndex() {

        if (osp != null) {

            osp = (ClientIndexView) fed.getIndex(IBigdataFederation.UNISOLATED,
                    name_osp);

        }

        return osp;

    }

    final public IIndex getJustificationIndex() {

        if (just != null) {

            just = (ClientIndexView) fed.getIndex(
                    IBigdataFederation.UNISOLATED, name_just);

        }
        
        return just;
        
    }

    /**
     * Add a term into the term:id index and the id:term index, returning the
     * assigned term identifier (non-batch API).
     * 
     * @param value
     *            The term.
     * 
     * @return The assigned term identifier.
     */
    public long addTerm(Value value) {

        final _Value[] terms = new _Value[]{(_Value)value};
        
        insertTerms(terms, 1, false, false);
        
        return terms[0].termId;
        
    }

    final public _Value getTerm(long id) {

        byte[] data = (byte[])getIdTermIndex().lookup(keyBuilder.id2key(id));

        if (data == null)
            return null;

        return _Value.deserialize(data);

    }

    final public long getTermId(Value value) {

        if(value==null) return IRawTripleStore.NULL;
        
        _Value val = (_Value) OptimizedValueFactory.INSTANCE
                .toNativeValue(value);
        
        if( val.termId != IRawTripleStore.NULL ) return val.termId; 

        Object tmp = getTermIdIndex().lookup(keyBuilder.value2Key(value));
        
        if (tmp == null)
            return IRawTripleStore.NULL;

        try {

            val.termId = new DataInputBuffer((byte[]) tmp).unpackLong();

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        return val.termId;

    }

    /**
     * This implementation is designed to use unisolated batch writes on the
     * terms and ids index that guarentee consistency.
     * 
     * @todo can we refactor the split support out of this method so that the
     *       code can be reused for the scale-up as well as scale-out indices?
     */
    public void insertTerms(_Value[] terms, int numTerms, boolean haveKeys,
            boolean sorted) {

        if (numTerms == 0)
            return;

        if (!haveKeys && sorted)
            throw new IllegalArgumentException("sorted requires keys");
        
        long begin = System.currentTimeMillis();
        long keyGenTime = 0; // time to convert unicode terms to byte[] sort keys.
        long sortTime = 0; // time to sort terms by assigned byte[] keys.
        long insertTime = 0; // time to insert terms into the forward and reverse index.
        
        System.err.print("Writing "+numTerms+" terms ("+terms.getClass().getSimpleName()+")...");

        {

            /*
             * First make sure that each term has an assigned sort key.
             */
            if(!haveKeys) {

                long _begin = System.currentTimeMillis();
                
                generateSortKeys(keyBuilder, terms, numTerms);
                
                keyGenTime = System.currentTimeMillis() - _begin;

            }
            
            /*
             * Sort terms by their assigned sort key. This places them into the
             * natural order for the term:id index.
             * 
             * @todo "known" terms should be filtered out of this operation. A
             * term is marked as "known" within a client if it was successfully
             * asserted against both the terms and ids index (or if it was
             * discovered on lookup against the ids index).
             */

            if (!sorted ) {
            
                long _begin = System.currentTimeMillis();
                
                Arrays.sort(terms, 0, numTerms, _ValueSortKeyComparator.INSTANCE);

                sortTime += System.currentTimeMillis() - _begin;
                
            }

            /*
             * For each term that does not have a pre-assigned term identifier,
             * execute a remote unisolated batch operation that assigns the term
             * identifier.
             * 
             * @todo parallelize operations across index partitions?
             */
            {
                
                long _begin = System.currentTimeMillis();

                ClientIndexView termId = (ClientIndexView)getTermIdIndex();

                /*
                 * Create a key buffer holding the sort keys. This does not
                 * allocate new storage for the sort keys, but rather aligns the
                 * data structures for the call to splitKeys().
                 */
                byte[][] termKeys = new byte[numTerms][];
                
                {
                    
                    for(int i=0; i<numTerms; i++) {
                        
                        termKeys[i] = terms[i].key;
                        
                    }
                    
                }

                // split up the keys by the index partitions.
                List<Split> splits = termId.splitKeys(numTerms, termKeys);

                // for each split.
                Iterator<Split> itr = splits.iterator();
                
                while(itr.hasNext()) {

                    Split split = itr.next();
                    
                    byte[][] keys = new byte[split.ntuples][];
                    
                    for(int i=split.fromIndex, j=0; i<split.toIndex; i++, j++) {
                        
                        keys[j] = terms[i].key;
                        
                    }
                    
                    // create batch operation for this partition.
                    AddTerms op = new AddTerms(new MutableKeyBuffer(
                            split.ntuples, keys));

                    // submit batch operation.
                    AddTerms.Result result = (AddTerms.Result) termId.submit(
                            split.pmd, op);
                    
                    // copy the assigned/discovered term identifiers.
                    for(int i=split.fromIndex, j=0; i<split.toIndex; i++, j++) {
                        
                        terms[i].termId = result.ids[j];
                        
                    }
                    
                }
                
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
            
            long _begin = System.currentTimeMillis();
            
            ClientIndexView idTerm = (ClientIndexView)getIdTermIndex();

            /*
             * Create a key buffer to hold the keys generated from the term
             * identifers and then generate those keys. The terms are already in
             * sorted order by their term identifiers from the previous step.
             */
            byte[][] idKeys = new byte[numTerms][];
            
            {

                // Private key builder removes single-threaded constraint.
                KeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG); 
                
                for(int i=0; i<numTerms; i++) {
                    
                    idKeys[i] = keyBuilder.reset().append(terms[i].termId).getKey();
                    
                }
                
            }

            // split up the keys by the index partitions.
            List<Split> splits = idTerm.splitKeys(numTerms, idKeys);

            // for each split.
            Iterator<Split> itr = splits.iterator();

            // StatementBuffer is reused for each serialized term.
            DataOutputBuffer out = new DataOutputBuffer();
            
            while(itr.hasNext()) {

                Split split = itr.next();
                
                byte[][] keys = new byte[split.ntuples][];
                
                byte[][] vals = new byte[split.ntuples][];
                
                for(int i=split.fromIndex, j=0; i<split.toIndex; i++, j++) {
                    
                    // Encode the term identifier as an unsigned long integer.
                    keys[j] = idKeys[i];
                    
                    // Serialize the term.
                    vals[j] = terms[i].serialize(out.reset());
                    
                }
                
                // create batch operation for this partition.
                AddIds op = new AddIds(
                        new MutableKeyBuffer(split.ntuples, keys),
                        new MutableValueBuffer(split.ntuples, vals));

                // submit batch operation (result is "null").
                idTerm.submit(split.pmd, op);
                
                /*
                 * Iff the unisolated write succeeds then this client knows that
                 * the term is now in both the forward and reverse indices. We
                 * codify that knowledge by setting the [known] flag on the
                 * term.
                 */
                for(int i=split.fromIndex, j=0; i<split.toIndex; i++, j++) {
                    
                    terms[i].known = true;

                }
                
            }
            
            insertTime += System.currentTimeMillis() - _begin;

        }

        long elapsed = System.currentTimeMillis() - begin;
        
        System.err.println("in " + elapsed + "ms; keygen=" + keyGenTime
                + "ms, sort=" + sortTime + "ms, insert=" + insertTime + "ms");
        
    }

    /**
     * NOP since the client uses unisolated writes which auto-commit.
     */
    final public void commit() {
        
        if(INFO) usage();
        
    }
    
    /**
     * The federation is considered stable regardless of whether the federation
     * is on stable storage since clients only disconnect when they use
     * {@link #close()}.
     */
    final public boolean isStable() {
        
        return true;
        
    }
    
    /**
     * Disconnects from the {@link IBigdataFederation}.
     */
    final public void close() {
        
        fed.disconnect();
        
    }

    /**
     * Drops the indices used by the {@link ScaleOutTripleStore} and disconnects
     * from the {@link IBigdataFederation}.
     */
    final public void closeAndDelete() {
        
        clear();
        
        fed.disconnect();
        
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

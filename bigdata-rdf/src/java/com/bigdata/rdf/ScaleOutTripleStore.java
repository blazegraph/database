/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on May 19, 2007
 */

package com.bigdata.rdf;

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
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
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

        // // Register indices.
        // fed.registerIndex(name_terms);
        // fed.registerIndex(name_ids);
        // fed.registerIndex(name_spo);
        // fed.registerIndex(name_pos);
        // fed.registerIndex(name_osp);

        // Obtain unisolated views on those indices.
        terms = (ClientIndexView) fed.getIndex(IBigdataFederation.UNISOLATED,
                name_termId);
        ids = (ClientIndexView) fed.getIndex(IBigdataFederation.UNISOLATED,
                name_idTerm);
        spo = (ClientIndexView) fed.getIndex(IBigdataFederation.UNISOLATED,
                name_spo);
        pos = (ClientIndexView) fed.getIndex(IBigdataFederation.UNISOLATED,
                name_pos);
        osp = (ClientIndexView) fed.getIndex(IBigdataFederation.UNISOLATED,
                name_osp);

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
    
    final public IIndex getTermIdIndex() {
        
        return terms;
        
    }

    final public IIndex getIdTermIndex() {
        
        return ids;
        
    }
    
    final public IIndex getSPOIndex() {
        
        return spo;
        
    }
    
    final public IIndex getPOSIndex() {
        
        return pos;
        
    }
    
    final public IIndex getOSPIndex() {
        
        return osp;
        
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
             * FIXME "known" terms should be filtered out of this operation. A
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

            // Buffer is reused for each serialized term.
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
     * Disconnects from the {@link IBigdataFederation}.
     */
    final public void close() {
        
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

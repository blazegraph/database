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
 * Created on May 21, 2007
 */
package com.bigdata.rdf.lexicon;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.ConcurrentHashMap;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;
import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.ICounter;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.compression.IDataSerializer;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure;
import com.bigdata.btree.proc.IParallelizableIndexProcedure;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.relation.IMutableRelationIndexWriteProcedure;

/**
 * This unisolated operation inserts terms into the <em>term:id</em> index,
 * assigning identifiers to terms as a side-effect. The use of this operation
 * MUST be followed by the the use of {@link Id2TermWriteProc} to ensure that
 * the reverse mapping from id to term is defined before any statements are
 * inserted using the assigned term identifiers. The client MUST NOT make
 * assertions using the assigned term identifiers until the corresponding
 * {@link Id2TermWriteProc} operation has suceeded.
 * <p>
 * In order for the lexicon to remain consistent if the client fails for any
 * reason after the forward mapping has been made restart-safe and before the
 * reverse mapping has been made restart-safe clients MUST always use a
 * successful {@link Term2IdWriteProc} followed by a successful
 * {@link Id2TermWriteProc} before inserting statements using term identifiers
 * into the statement indices. In particular, a client MUST NOT treat lookup
 * against the terms index as satisfactory evidence that the term also exists
 * in the reverse mapping.
 * <p>
 * Note that it is perfectly possible that a concurrent client will overlap in
 * the terms being inserted. The results will always be fully consistent if the
 * rules of the road are observed since (a) unisolated operations are
 * single-threaded; (b) term identifiers are assigned in an unisolated atomic
 * operation by {@link Term2IdWriteProc}; and (c) the reverse mapping is made
 * consistent with the assignments made/discovered by the forward mapping.
 * <p>
 * Note: The {@link Term2IdWriteProc} and {@link Id2TermWriteProc} operations
 * may be analyzed as a batch variant of the following pseudocode.
 * 
 * <pre>
 *  
 *  for each term:
 *  
 *  termId = null;
 *  
 *  synchronized (ndx) {
 *    
 *    counter = ndx.getCounter();
 *  
 *    termId = ndx.lookup(term.key);
 *    
 *    if(termId == null) {
 * 
 *       termId = counter.inc();
 *       
 *       ndx.insert(term.key,termId);
 *       
 *       }
 *  
 *  }
 *  
 * </pre>
 * 
 * In addition, the actual operations against scale-out indices are performed on
 * index partitions rather than on the whole index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Term2IdWriteProc extends AbstractKeyArrayIndexProcedure implements
        IParallelizableIndexProcedure, IMutableRelationIndexWriteProcedure {
    
    protected static final Logger log = Logger.getLogger(Term2IdWriteProc.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static protected boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static protected boolean DEBUG = log.isDebugEnabled();

//    static {
//        if(DEBUG) {
//         
//            log.removeAllAppenders();
//            
//            try {
//                log.addAppender(new FileAppender(new SimpleLayout(),"Term2IdWriteProc.log"));
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            
//        }
//    }

    /**
     * Flag enables optional ground truth verification. It is only enabled at
     * the DEBUG level IFF this flag is ALSO set.
     * <p>
     * <strong>WARNING: This IS NOT scaleable! </strong>
     * <p>
     * <strong>WARNING: This option IS NOT safe when using more than one triple
     * store either concurrently or in sequence! For example, you can use it to
     * examine a single unit test for inconsistences, but not a sequence of unit
     * tests since the data will be kept within the same global map and hence
     * confound the test!</strong>
     */
    private static boolean enableGroundTruth = false;
    private static ConcurrentHashMap<Long,byte[]> groundTruthId2Term;
    private static ConcurrentHashMap<byte[],Long> groundTruthTerm2Id;
    static {
        
        if (DEBUG && enableGroundTruth) {
        
            log.warn("Will track ground truth assignments");
            
            // note: use a large initial capacity. default concurrency level is 16.
            
            groundTruthId2Term = new ConcurrentHashMap<Long,byte[]>(500000);

            groundTruthTerm2Id = new ConcurrentHashMap<byte[],Long>(500000);
            
        }
        
    }
    
    /**
     * 
     */
    private static final long serialVersionUID = -4736465754523655679L;

    /**
     * Serialized as extended metadata. When <code>true</code> unknown terms
     * are NOT added to the database.
     */
    private boolean readOnly;
    
    public final boolean isReadOnly() {
        
        return readOnly;
        
    }
    
    /**
     * Serialized as extended metadata. When <code>true</code> blank nodes
     * are stored in the lexicon's forward index.
     */
    private boolean storeBlankNodes;
    
    public final boolean isStoreBlankNodes() {
        
        return storeBlankNodes;
        
    }

    private int scaleOutTermIdBitsToReverse;

    /**
     * De-serialization constructor.
     */
    public Term2IdWriteProc() {
        
    }
    
    protected Term2IdWriteProc(IDataSerializer keySer, int fromIndex,
            int toIndex, byte[][] keys, boolean readOnly,
            boolean storeBlankNodes, int scaleOutTermIdBitsToReverse) {

        super(keySer, null, fromIndex, toIndex, keys, null /* vals */);

        this.readOnly = readOnly;

        this.storeBlankNodes = storeBlankNodes;

        this.scaleOutTermIdBitsToReverse = scaleOutTermIdBitsToReverse;

    }

    public static class Term2IdWriteProcConstructor extends
            AbstractKeyArrayIndexProcedureConstructor<Term2IdWriteProc> {

        private final boolean readOnly;
        private final boolean storeBlankNodes;
        private final int scaleOutTermIdBitsToReverse;

        /**
         * Values ARE NOT sent.
         */
        public final boolean sendValues() {
            
            return false;
            
        }

        public Term2IdWriteProcConstructor(boolean readOnly,
                boolean storeBlankNodes, int scaleOutTermIdBitsToReverse) {

            this.readOnly = readOnly;

            this.storeBlankNodes = storeBlankNodes;

            this.scaleOutTermIdBitsToReverse = scaleOutTermIdBitsToReverse;

        }

        public Term2IdWriteProc newInstance(IDataSerializer keySer,
                IDataSerializer valSer, int fromIndex, int toIndex,
                byte[][] keys, byte[][] vals) {

            assert vals == null;

            return new Term2IdWriteProc(keySer, fromIndex, toIndex, keys,
                    readOnly, storeBlankNodes, scaleOutTermIdBitsToReverse);

        }

    }

    /**
     * For each term whose serialized key is mapped to the current index
     * partition, lookup the term in the <em>terms</em> index. If it is there
     * then note its assigned termId. Otherwise, use the partition local counter
     * to assign the term identifier, note the term identifer so that it can be
     * communicated back to the client, and insert the {term,termId} entry into
     * the <em>terms</em> index.
     * 
     * @param ndx
     *            The terms index.
     * 
     * @return The {@link Result}, which contains the discovered / assigned
     *         term identifiers.
     * 
     * @todo no point sending bnodes when readOnly.
     */
    public Object apply(final IIndex ndx) {
        
        final int numTerms = getKeyCount();
        
        assert numTerms > 0 : "numTerms="+numTerms;
        
        // used to store the discovered / assigned term identifiers.
        final long[] ids = new long[numTerms];
        
        // used to assign term identifiers.
        final ICounter counter = ndx.getCounter();

//        // true iff this is an unpartitioned index.
//        final boolean scaleOut = counter instanceof BTree.PartitionedCounter;
        
        // used to serialize term identifers.
        final DataOutputBuffer idbuf = new DataOutputBuffer(Bytes.SIZEOF_LONG);
        
        final TermIdEncoder encoder = new TermIdEncoder(//scaleOutTermIds,
                scaleOutTermIdBitsToReverse);
        
        // #of new terms (#of writes on the index).
        int nnew = 0;
        for (int i = 0; i < numTerms; i++) {

            final byte[] key = getKey(i);

            // this byte encodes the kind of term (URI, Literal, BNode, etc.)
            final byte code = KeyBuilder.decodeByte(key[0]);
            
            final long termId;

            if (!storeBlankNodes && code == ITermIndexCodes.TERM_CODE_BND) {

                /*
                 * Do not enter blank nodes into the forward index.
                 * 
                 * For this case, we just assign a term identifier and leave it
                 * at that. If two different documents by some chance happen to
                 * specify the same blank node ID they will still be assigned
                 * distinct term identifiers. The only way that you can get the
                 * same term identifier for a blank node is to have the blank
                 * node ID matched in a canonicalizing map of blank nodes by the
                 * client. That map, of course, should be scoped to the document
                 * in which the blank node IDs appear.
                 */
                
                if (readOnly) {
                
                    // blank nodes can not be resolved by the index.
                    termId = 0L;

                } else {
                    
                    // assign a term identifier.
                    termId = TermIdEncoder.setFlags(encoder.encode(counter
                            .incrementAndGet()), code);
                }
                
            } else {

                // Lookup in the forward index (URIs, Literals, and SIDs)
                //
                // Also BNodes iff storeBlankNodes is true
                final byte[] tmp = ndx.lookup(key);
    
                if (tmp == null) {

                    // not found.
                    
                    if(readOnly) {
                        
                        // not found - will not be assigned.
                        termId = 0L;

                    } else {

                        // assign a term identifier.
                        termId = TermIdEncoder.setFlags(encoder.encode(counter
                                .incrementAndGet()), code);

                        if (DEBUG && enableGroundTruth) {

                            groundTruthTest(key, termId, ndx, counter);

                        }

                        // format as packed long integer.
                        try {

//                            idbuf.reset().packLong(termId);
                            idbuf.reset().writeLong(termId); // may be negative.

                        } catch (IOException ex) {

                            throw new RuntimeException(ex);

                        }

                        // insert into index.
                        if (ndx.insert(key, idbuf.toByteArray()) != null) {

                            throw new AssertionError();

                        }

                        nnew++;
                    
                    }
                    
                } else { // found.
    
                    try {

                        // unpack the existing term identifier.
//                        termId = new DataInputBuffer(tmp).unpackLong();
                        termId = new DataInputBuffer(tmp).readLong();
                        
                    } catch (IOException ex) {
                        
                        throw new RuntimeException(ex);
                        
                    }
                       
                }
    
            }
            
            ids[i] = termId;

        }

        /*
         * Note: this is for debugging. It does not rely on ground truth, but
         * only logs information. It was originally used to track down a lost
         * update problem.
         */
//        if (enableGroundTruth && ndx.getIndexMetadata().getPartitionMetadata() != null) {
//
//            final long v = counter.get();
//            final int pid = (int) v >>> 32;
//            final int ctr = (int) v;
//
//            // note: the mutable btree - accessed here for debugging only.
//            final BTree btree;
//            if (ndx instanceof AbstractBTree) {
//                btree = (BTree) ndx;
//            } else {
//                btree = (BTree) ((FusedView) ndx).getSources()[0];
//            }
//            
//            log.warn("after task"+
//            ": nnew="+nnew+//
//            ", partitionId="+ndx.getIndexMetadata().getPartitionMetadata().getPartitionId()+//
//            ", pid="+pid+//
//            ", ctr="+ctr+//
//            ", counter="+counter.getClass().getName()+//
//            ", sourceCheckpoint="+btree.getCheckpoint()// btree was loaded from here.
//            );
//            
//        }
        
        return new Result(ids);

    }
    
    private void groundTruthTest(byte[] key, long termId, IIndex ndx,
            ICounter counter) {
        
        if(groundTruthId2Term.isEmpty()) {
            
            log.warn("Ground truth testing enabled.");
            
        }

        /*
         * Note: add to map if not present. returns the value
         * already stored in the map (and null if there was no value
         * in the map).
         */
        
        // remember the termId assigned to that key.
        final Long oldId = groundTruthTerm2Id.putIfAbsent(key, termId);
        
        if( oldId != null && oldId.longValue() != termId ) {

            /*
             * The assignment of the term identifier to the key is
             * not stable.
             */
            
            throw new AssertionError("different termId assigned"+//
                    ": oldId=" + oldId + //
                    ", newId=" + termId + //
                    ", key=" + BytesUtil.toString(key)+//
                    ", pmd="+ndx.getIndexMetadata().getPartitionMetadata());

        }

        // remember the key to which we assigned that termId.
        final byte[] oldKey = groundTruthId2Term.putIfAbsent(termId, key);
        
        if (oldKey != null && !BytesUtil.bytesEqual(oldKey, key)) {

            /*
             * The assignment of the term identifier to the key is
             * not unique.
             */
            
            // the partition identifier (assuming index is partitioned).
//                        final long pid = id0 >> 32;
//                        final long mask = 0xffffffffL;
//                        final int ctr = (int) (id0 & mask);
            
            throw new AssertionError("assignment not unique"+//
                    ": termId=" + termId +//
                    ", oldKey=" + BytesUtil.toString(oldKey) + //
                    ", newKey=" + BytesUtil.toString(key)+//
                    ", pmd="+ndx.getIndexMetadata().getPartitionMetadata()+//
//                                ", pid="+pid+", ctr="+ctr+//
                    ", counter="+counter+//
                    ", counter="+counter.getClass().getName());
            
        }
        
    }
    
    protected void readMetadata(ObjectInput in) throws IOException, ClassNotFoundException {
        
        super.readMetadata(in);
        
        readOnly = in.readBoolean();
     
//        scaleOutTermIds = in.readBoolean();

        scaleOutTermIdBitsToReverse = (int) in.readByte();

    }

    /**
     * Writes metadata (not the keys or values, but just other metadata used by
     * the procedure).
     * <p>
     * The default implementation writes <code>toIndex - fromIndex</code>,
     * which is the #of keys.
     * 
     * @param out
     * 
     * @throws IOException
     */
    protected void writeMetadata(ObjectOutput out) throws IOException {

        super.writeMetadata(out);

        out.writeBoolean(readOnly);

//        out.writeBoolean(scaleOutTermIds);

        out.writeByte((byte) scaleOutTermIdBitsToReverse);

    }
    
    /**
     * Object encapsulates the discovered / assigned term identifiers and
     * provides efficient serialization for communication of those data to the
     * client.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Result implements Externalizable {

        public long[] ids;
        
        /**
         * 
         */
        private static final long serialVersionUID = -8307927320589290348L;

        /**
         * De-serialization constructor.
         */
        public Result() {
            
        }
        
        public Result(long[] ids) {

            assert ids != null;
            
            assert ids.length > 0;
            
            this.ids = ids;
            
        }

        private final static transient short VERSION0 = 0x0;
        
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

            final short version = ShortPacker.unpackShort(in);
            
            if(version!=VERSION0) {
                
                throw new IOException("Unknown version: "+version);
                
            }
            
            final int n = (int) LongPacker.unpackLong(in);
            
            ids = new long[n];
            
            for (int i = 0; i < n; i++) {
                
//                ids[i] = LongPacker.unpackLong(in);
                ids[i] = in.readLong();
                
            }
            
        }

        public void writeExternal(ObjectOutput out) throws IOException {

            final int n = ids.length;
            
            ShortPacker.packShort(out, VERSION0);
            
            LongPacker.packLong(out,n);

            for (int i = 0; i < n; i++) {
                
//                LongPacker.packLong(out, ids[i]);
                out.writeLong(ids[i]);
                
            }
            
        }
        
    }
    
}

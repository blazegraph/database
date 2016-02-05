/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import org.apache.log4j.Logger;

import com.bigdata.btree.ICounter;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.AbstractLocalSplitResultAggregator;
import com.bigdata.btree.proc.IParallelizableIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.btree.proc.SplitValuePair;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.io.LongPacker;
import com.bigdata.io.ShortPacker;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.lexicon.Term2IdWriteProc.Result;
import com.bigdata.relation.IMutableRelationIndexWriteProcedure;
import com.bigdata.service.Split;
import com.bigdata.util.BytesUtil;

/**
 * This unisolated operation inserts terms into the <em>term:id</em> index,
 * assigning identifiers to terms as a side-effect. The use of this operation
 * MUST be followed by the the use of {@link Id2TermWriteProc} to ensure that
 * the reverse mapping from id to term is defined before any statements are
 * inserted using the assigned term identifiers. The client MUST NOT make
 * assertions using the assigned term identifiers until the corresponding
 * {@link Id2TermWriteProc} operation has succeeded.
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
 * may be analyzed as a batch variant of the following pseudo code.
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
 */
public class Term2IdWriteProc extends AbstractKeyArrayIndexProcedure<Result> implements
        IParallelizableIndexProcedure<Result>, IMutableRelationIndexWriteProcedure<Result> {
    
    private static final Logger log = Logger.getLogger(Term2IdWriteProc.class);
    
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
     * <strong>WARNING: This IS NOT scalable! </strong>
     * <p>
     * <strong>WARNING: This option IS NOT safe when using more than one triple
     * store either concurrently or in sequence! For example, you can use it to
     * examine a single unit test for inconsistencies, but not a sequence of unit
     * tests since the data will be kept within the same global map and hence
     * confound the test!</strong>
     */
    private static boolean enableGroundTruth = false;
    private static ConcurrentHashMap<Long,byte[]> groundTruthId2Term;
    private static ConcurrentHashMap<byte[],Long> groundTruthTerm2Id;
    static {
        
        if (log.isDebugEnabled() && enableGroundTruth) {
        
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

    @Override
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
    
    protected Term2IdWriteProc(IRabaCoder keySer, int fromIndex,
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
        @Override
        public final boolean sendValues() {
            
            return false;
            
        }

        public Term2IdWriteProcConstructor(final boolean readOnly,
                final boolean storeBlankNodes,
                final int scaleOutTermIdBitsToReverse) {

            this.readOnly = readOnly;

            this.storeBlankNodes = storeBlankNodes;

            this.scaleOutTermIdBitsToReverse = scaleOutTermIdBitsToReverse;
            
        }

        @Override
        public Term2IdWriteProc newInstance(final IRabaCoder keySer,
                final IRabaCoder valSer, final int fromIndex,
                final int toIndex, final byte[][] keys, final byte[][] vals) {

            assert vals == null;

            if(log.isInfoEnabled())
                log.info("TERM2ID Proc Ctor: ntuples=" + (toIndex-fromIndex));
            
            return new Term2IdWriteProc(keySer, fromIndex, toIndex, keys,
                    readOnly, storeBlankNodes, scaleOutTermIdBitsToReverse);

        }

    }

    /**
     * For each term whose serialized key is mapped to the current index
     * partition, lookup the term in the <em>terms</em> index. If it is there
     * then note its assigned termId. Otherwise, use the partition local counter
     * to assign the term identifier, note the term identifier so that it can be
     * communicated back to the client, and insert the {term,termId} entry into
     * the <em>terms</em> index.
     * 
     * @param ndx
     *            The terms index.
     * 
     * @return The {@link Result}, which contains the discovered / assigned
     *         term identifiers.
     * 
     * TODO no point sending bnodes when readOnly.
     */
    @Override
    public Result applyOnce(final IIndex ndx, final IRaba keys, final IRaba vals) {

		final boolean DEBUG = log.isDebugEnabled();

		final int numTerms = keys.size();

        assert numTerms > 0 : "numTerms="+numTerms;
        
		// used to store the discovered / assigned term identifiers.
		@SuppressWarnings("rawtypes")
		final IV[] ivs = new IV[numTerms];
        
        // used to assign term identifiers.
        final ICounter counter = ndx.getCounter();

//        // true iff this is an unpartitioned index.
//        final boolean scaleOut = counter instanceof BTree.PartitionedCounter;
        
        // used to serialize term identifiers.
        @SuppressWarnings("resource")
      final DataOutputBuffer idbuf = new DataOutputBuffer();
        
        final TermIdEncoder encoder = readOnly ? null
                : scaleOutTermIdBitsToReverse == 0 ? null : new TermIdEncoder(
                        scaleOutTermIdBitsToReverse);
        
//        final DataOutputBuffer kbuf = new DataOutputBuffer(128);

        // #of new terms (#of writes on the index).
        int nnew = 0;
        for (int i = 0; i < numTerms; i++) {

            // Note: Copying the key into a buffer does not help since we need
            // it in its own byte[] to do lookup against the index.
//          getKeys().copy(i, kbuf.reset());
            final byte[] key = keys.get(i);

            // this byte encodes the kind of term (URI, Literal, BNode, etc.)
            final byte code = key[0];//KeyBuilder.decodeByte(key[0]);
            
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
                    ivs[i] = null;

                } else {
                    
                    /*
                     * Assign a term identifier.
                     * 
                     * Note: The TermIdEncoder is ONLY used in scale-out.
                     */
                    
                    final long ctr = counter.incrementAndGet();
                    
                    final long termId = encoder == null ? ctr : encoder
                            .encode(ctr);
                    
                    ivs[i] = new TermId(VTE(code), termId);
                    
                }
                
            } else {

                /*
                 * Lookup in the forward index (URIs, Literals, and SIDs)
                 * 
                 * Note: Also handles BNodes iff storeBlankNodes is true
                 * 
                 * @todo reuse Tuple for lookups to reduce allocation (will
                 * reuse an internal buffer).
                 */
                final byte[] tmp = ndx.lookup(key);
    
                if (tmp == null) {

                    // not found.
                    
                    if(readOnly) {
                        
                        // not found - will not be assigned.
                        ivs[i] = null;

                    } else {

                        /*
                         * Assign a term identifier.
                         * 
                         * Note: The TermIdEncoder is ONLY used in scale-out.
                         */
                        
                        final long ctr = counter.incrementAndGet();
                        
                        final long termId = encoder == null ? ctr : encoder
                                .encode(ctr);

                        @SuppressWarnings("rawtypes")
                        final TermId<?> iv = new TermId(VTE(code), termId);
                        
                        if (DEBUG && enableGroundTruth) {

                            groundTruthTest(key, termId, ndx, counter);

                        }

                        final byte[] bytes = iv
                                .encode(KeyBuilder.newInstance()).getKey();

                        idbuf.reset().write(bytes);

                        // insert into index.
                        if (ndx.insert(key, idbuf.toByteArray()) != null) {

                            throw new AssertionError();

                        }

                        nnew++;
                        
                        ivs[i] = iv;
                    
                    }
                    
                } else { // found.
    
                    ivs[i] = IVUtility.decode(tmp);
                        
                }
    
            }
            
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
        
        return new Result(ivs);

    }
    
    private void groundTruthTest(final byte[] key, final long termId, final IIndex ndx,
            final ICounter counter) {
        
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
    
    @Override
    protected void readMetadata(final ObjectInput in) throws IOException, ClassNotFoundException {
        
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
    @Override
    protected void writeMetadata(final ObjectOutput out) throws IOException {

        super.writeMetadata(out);

        out.writeBoolean(readOnly);

//        out.writeBoolean(scaleOutTermIds);

        out.writeByte((byte) scaleOutTermIdBitsToReverse);

    }
    
    final public static VTE VTE(final byte code) {
        
        switch(code) {
        case ITermIndexCodes.TERM_CODE_URI:
            return VTE.URI;
        case ITermIndexCodes.TERM_CODE_BND:
            return VTE.BNODE;
//        case ITermIndexCodes.TERM_CODE_STMT:
//            return VTE.STATEMENT;
        case ITermIndexCodes.TERM_CODE_DTL:
//        case ITermIndexCodes.TERM_CODE_DTL2:
        case ITermIndexCodes.TERM_CODE_LCL:
        case ITermIndexCodes.TERM_CODE_LIT:
            return VTE.LITERAL;
        default:
            throw new IllegalArgumentException("code=" + code);
        }
        
    }

    
    /**
     * Object encapsulates the discovered / assigned term identifiers and
     * provides efficient serialization for communication of those data to the
     * client.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public static class Result implements Externalizable {

        public IV[] ivs;
        
        private static final long serialVersionUID = -8307927320589290348L;

        /**
         * De-serialization constructor.
         */
        public Result() {
            
        }
        
        public Result(final IV[] ivs) {

            assert ivs != null;
            
            assert ivs.length > 0;
            
            this.ivs = ivs;
            
        }

        private final static transient short VERSION0 = 0x0;

        @Override
        public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {

            final short version = ShortPacker.unpackShort(in);
            
            if(version!=VERSION0) {
                
                throw new IOException("Unknown version: "+version);
                
            }
            
            final int n = (int) LongPacker.unpackLong(in);
            
            ivs = new IV[n];
            
            for (int i = 0; i < n; i++) {
                
//                ids[i] = LongPacker.unpackLong(in);
                ivs[i] = (IV) in.readObject();
                
            }
            
        }

        @Override
        public void writeExternal(final ObjectOutput out) throws IOException {

            final int n = ivs.length;
            
            ShortPacker.packShort(out, VERSION0);
            
            LongPacker.packLong(out,n);

            for (int i = 0; i < n; i++) {
                                
//                LongPacker.packLong(out, ids[i]);
                out.writeObject(ivs[i]);
                
            }
            
        }
        
    }
    
    /**
	 * {@link Split}-wise aggregation followed by combining the results across
	 * those splits in order to return an aggregated result whose iv[] is 1:1
	 * with the original keys[][].
	 */
	@Override
	protected IResultHandler<Result, Result> newAggregator() {
	
		return new TermResultAggregator(getKeys().size());

	}
	
	/**
	 * Aggregator collects the individual results in an internal ordered map and
	 * assembles the final result when it is requested from the individual
	 * results. With this approach there is no overhead or contention when the
	 * results are being produced in parallel and they can be combined
	 * efficiently within a single thread in {@link #getResult()}.
	 *
	 * @author bryan
	 */
	private class TermResultAggregator extends AbstractLocalSplitResultAggregator<Result> {

		/**
		 * 
		 * @param size
		 *            The #of elements in the request (which is the same as the
		 *            cardinality of the aggregated result).
		 */
		public TermResultAggregator(final int size) {
			
			super(size);
			
		}

		@Override
		protected Result newResult(final int size, final SplitValuePair<Split, Result>[] a) {

			@SuppressWarnings("rawtypes")
			final IV[] ivs = new IV[size];

			for (int i = 0; i < a.length; i++) {

				final Split split = a[i].key;

				final Result tmp = a[i].val;

				System.arraycopy(tmp.ivs/* src */, 0/* srcPos */, ivs/* dest */, split.fromIndex/* destPos */,
						split.ntuples/* length */);

			}

			/*
			 * Return the aggregated result.
			 */
			final Result r = new Result(ivs);

			return r;

		}

	} // TermResultHandler

}

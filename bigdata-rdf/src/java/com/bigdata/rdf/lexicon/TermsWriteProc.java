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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.openrdf.model.Value;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IParallelizableIndexProcedure;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.io.LongPacker;
import com.bigdata.io.ShortPacker;
import com.bigdata.rdf.internal.AbstractIV;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.relation.IMutableRelationIndexWriteProcedure;

/**
 * This unisolated operation inserts {@link Value}s into the
 * <em>{termCode,hash(Value),counter}:Value</em> index, assigning {@link IV}s to
 * {@link Value}s as a side-effect.
 * <p>
 * Note that it is perfectly possible that a concurrent client will overlap in
 * the terms being inserted. The results will always be fully consistent if the
 * rules of the road are observed since (a) unisolated operations are
 * single-threaded; and (b) {@link IV}s are assigned in an unisolated atomic
 * operation by {@link TermsWriteProc}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: Term2IdWriteProc.java 4582 2011-05-31 19:12:53Z thompsonbry $
 */
public class TermsWriteProc extends AbstractKeyArrayIndexProcedure implements
        IParallelizableIndexProcedure, IMutableRelationIndexWriteProcedure {
    
    private static final Logger log = Logger.getLogger(TermsWriteProc.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

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
    private boolean toldBNodes;
    
    public final boolean isToldBNodes() {
        
        return toldBNodes;
        
    }

    /**
     * De-serialization constructor.
     */
    public TermsWriteProc() {
        
    }
    
	protected TermsWriteProc(final IRabaCoder keySer, final IRabaCoder valSer,
			final int fromIndex, final int toIndex, final byte[][] keys,
			final byte[][] vals, final boolean readOnly,
			final boolean storeBlankNodes) {

		super(keySer, valSer, fromIndex, toIndex, keys, vals);

		this.readOnly = readOnly;

		this.toldBNodes = storeBlankNodes;

	}

	public static class TermsWriteProcConstructor extends
			AbstractKeyArrayIndexProcedureConstructor<TermsWriteProc> {

		private final boolean readOnly;
		private final boolean toldBNodes;

		public final boolean sendValues() {

			return true;
            
        }

        public TermsWriteProcConstructor(final boolean readOnly,
                final boolean toldBNodes) {

            this.readOnly = readOnly;

            this.toldBNodes = toldBNodes;

        }

        public TermsWriteProc newInstance(final IRabaCoder keySer,
                final IRabaCoder valSer, final int fromIndex,
                final int toIndex, final byte[][] keys, final byte[][] vals) {

            if(log.isInfoEnabled())
				log.info("ntuples=" + (toIndex - fromIndex));

			return new TermsWriteProc(keySer, valSer, fromIndex, toIndex, keys,
					vals, readOnly, toldBNodes);

        }

    } // class TermsWriteProcConstructor

	/**
	 * For each term whose serialized key is mapped to the current index
	 * partition, lookup the term in the <em>terms</em> index. If it is there
	 * then wrap its key as its {@link IV}. Otherwise, note the of terms in the
	 * collision bucket, and insert {hash(term),counter},term} entry into the
	 * <em>terms</em> index.
	 * 
	 * @param ndx
	 *            The terms index.
	 * 
	 * @return The {@link Result}, which contains the discovered / assigned term
	 *         identifiers.
	 * 
	 *         TODO There is no point sending bnodes when readOnly and NOT in
	 *         told bnodes mode because the caller is unable to unify a blank
	 *         node with an entry in the index.
	 */
    public Object apply(final IIndex ndx) {
        
        final int numTerms = getKeyCount();
        
        assert numTerms > 0 : "numTerms="+numTerms;

		// Helper class for index operations.
		final TermsIndexHelper helper = new TermsIndexHelper();
		
		// used to format the keys for the TERMS index.
		final IKeyBuilder keyBuilder = helper.newKeyBuilder();

        // used to store the discovered / assigned term identifiers.
        final IV[] ivs = new IV[numTerms];
        
        /*
         * Note: The baseKey should be one byte shorter than the full key (since
         * it does not include the one byte counter).
         */
        final byte[] baseKey = new byte[keyBuilder.capacity() - 1];

        // Used to report the size of each collision bucket.
        final AtomicInteger bucketSize = new AtomicInteger(0);
        
        // Incremented by the size of each collision bucket probed.
        long totalBucketSize = 0L;
        
        // The size of the largest collision bucket.
        int maxBucketSize = 0;
        
        for (int i = 0; i < numTerms; i++) {

            // Copy key into reused buffer to reduce allocation.
//            final byte[] baseKey = getKey(i);
			getKeys().copy(i, new DataOutputBuffer(0, baseKey));

			// decode the VTE from the flags.
			final VTE vte = AbstractIV.getVTE(baseKey[0]);
            
			if (!toldBNodes && vte == VTE.BNODE) {

                /*
                 * Do not enter blank nodes into the TERMS index.
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

                    /*
                     * FIXME Use this to track down people who pass in a blank
                     * node on a read-only request when we are not using told
                     * bnodes. Under these conditions we can not unify the blank
                     * node with the TERMS index so the node should not have
                     * been passed in at all (for efficiency reasons).
                     */
//                    throw new UnsupportedOperationException();

                } else {

                    /*
                     * We are not in a told bnode mode and this is not a
                     * read-only request.
                     */

                    final byte[] key = helper.addBNode(ndx, keyBuilder,
                            baseKey, getValue(i));

                    ivs[i] = new TermId<BigdataValue>(key);
                    
					// The size of the collision bucket.
					final int tmp = ((TermId<?>) ivs[i]).counter();
					if (maxBucketSize < tmp) {
    					maxBucketSize += tmp;
    				}
    				totalBucketSize += tmp;

//                    // assign a term identifier.
//					final long termId = counter.incrementAndGet();
//
//                    ivs[i] = new TermId(VTE(code), termId);
                    
//                	final BNode bnode = (BNode)BigdataValueFactoryImpl.getInstance("test").getValueSerializer().deserialize(getValue(i));
//                	throw new UnsupportedOperationException("BNode: ID="+bnode.getID());
                    
                }
                
            } else {

				/*
				 * The serialized BigdataValue object.
				 * 
				 * TODO Avoid materialization of this, preferring to operate on
				 * streams in the source IRaba. We will need to compare it with
				 * the value[] on any other tuples in the collision bucket. We
				 * also need to decode the byte which represents the termCode
				 * can can be used to derive the VTE of the Value.
				 */
            	final byte[] val = getValue(i);
            	
				final byte[] key = helper.resolveOrAddValue(ndx, readOnly,
						keyBuilder, baseKey, val, bucketSize);

				final int tmp = bucketSize.get();
				if (maxBucketSize < tmp) {
					maxBucketSize += tmp;
				}
				totalBucketSize += tmp;
				
				if (key != null) {

					// Note: The first byte of the key is the IV's flags.
					ivs[i] = new TermId<BigdataValue>(key);
					
				} else {
					
					// Not found (and read-only).
					
				}
    
            }
            
        }

		return new Result(totalBucketSize, maxBucketSize, ivs);

    } // apply(ndx)

	protected void readMetadata(final ObjectInput in) throws IOException,
			ClassNotFoundException {

        super.readMetadata(in);
        
        readOnly = in.readBoolean();

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
    protected void writeMetadata(final ObjectOutput out) throws IOException {

        super.writeMetadata(out);

        out.writeBoolean(readOnly);

    }
    
//    final private static VTE VTE(final byte code) {
//        
//        switch(code) {
//        case ITermIndexCodes.TERM_CODE_URI:
//            return VTE.URI;
//        case ITermIndexCodes.TERM_CODE_BND:
//            return VTE.BNODE;
//        case ITermIndexCodes.TERM_CODE_STMT:
//            return VTE.STATEMENT;
//        case ITermIndexCodes.TERM_CODE_DTL:
////        case ITermIndexCodes.TERM_CODE_DTL2:
//        case ITermIndexCodes.TERM_CODE_LCL:
//        case ITermIndexCodes.TERM_CODE_LIT:
//            return VTE.LITERAL;
//        default:
//            throw new IllegalArgumentException();
//        }
//        
//    }

	/**
	 * Object encapsulates the discovered / assigned {@link IV}s and provides
	 * efficient serialization for communication of those data to the client.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 */
    public static class Result implements Externalizable {

		/**
		 * The total size of the hash collision buckets examined across all
		 * {@link Value}s in the request. Each time a {@link Value} is resolved
		 * to a hash collision bucket, the size of that bucket is incremented
		 * against this field. Thus it will double count a bucket if the same
		 * bucket is visited more than once for the request.
		 */
		public long totalBucketSize;
		
		/**
		 * The size of the largest hash collision bucket examined across all the
		 * {@link Value}s in the request.
		 */
		public int maxBucketSize;

		/**
		 * The {@link IV}s assigned to each {@link Value} in the request. The
		 * indicates of this array are correlated with the indices of the array
		 * provided to the request.
		 */
		public IV[] ivs;
        
        private static final long serialVersionUID = 1L;

        /**
         * De-serialization constructor.
         */
        public Result() {
            
        }

		/**
		 * 
		 * @param totalBucketSize
		 *            The total bucket size across all buckets examined.
		 * @param maxBucketSize
		 *            The size of the largest collision bucket examined.
		 * @param ivs
		 *            The assigned/resolved {@link IV}s.
		 */
		public Result(final long totalBucketSize, final int maxBucketSize,
				final IV[] ivs) {

            assert ivs != null;
            
            assert ivs.length > 0;

            this.totalBucketSize = totalBucketSize;
            
            this.maxBucketSize = maxBucketSize;
            
            this.ivs = ivs;
            
        }

        private final static transient short VERSION0 = 0x0;
        
		public void readExternal(final ObjectInput in) throws IOException,
				ClassNotFoundException {

			final short version = ShortPacker.unpackShort(in);

			if (version != VERSION0) {

				throw new IOException("Unknown version: " + version);

			}

			final int n = (int) LongPacker.unpackLong(in);

			ivs = new IV[n];

            for (int i = 0; i < n; i++) {
                
                ivs[i] = (IV) in.readObject();
                
            }
            
        }

        public void writeExternal(final ObjectOutput out) throws IOException {

            final int n = ivs.length;
            
            ShortPacker.packShort(out, VERSION0);

            // The #of results.
			LongPacker.packLong(out, n);

			// The total bucket size across all buckets examined.
			LongPacker.packLong(out, totalBucketSize);
			
			// The size of the largest collision bucket examined.
			LongPacker.packLong(out, maxBucketSize);
			
            for (int i = 0; i < n; i++) {
                                
                out.writeObject(ivs[i]);
                
            }
            
        }
        
    } // class Result

}

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

import org.apache.log4j.Logger;
import org.openrdf.model.Value;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.keys.IKeyBuilder;
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
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.AbstractIV;
import com.bigdata.rdf.internal.impl.BlobIV;
import com.bigdata.rdf.lexicon.BlobsWriteProc.Result;
import com.bigdata.relation.IMutableRelationIndexWriteProcedure;
import com.bigdata.service.Split;

/**
 * This unisolated operation inserts {@link Value}s into the
 * <em>{termCode,hash(Value),counter}:Value</em> index, assigning {@link IV}s to
 * {@link Value}s as a side-effect.
 * <p>
 * Note that it is perfectly possible that a concurrent client will overlap in
 * the terms being inserted. The results will always be fully consistent if the
 * rules of the road are observed since (a) unisolated operations are
 * single-threaded; and (b) {@link IV}s are assigned in an unisolated atomic
 * operation by {@link BlobsWriteProc}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class BlobsWriteProc extends AbstractKeyArrayIndexProcedure<Result> implements
        IParallelizableIndexProcedure<Result>, IMutableRelationIndexWriteProcedure<Result> {
    
    private static final Logger log = Logger.getLogger(BlobsWriteProc.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

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
    private boolean toldBNodes;
    
    public final boolean isToldBNodes() {
        
        return toldBNodes;
        
    }

    /**
     * De-serialization constructor.
     */
    public BlobsWriteProc() {
        
    }
    
	protected BlobsWriteProc(final IRabaCoder keySer, final IRabaCoder valSer,
			final int fromIndex, final int toIndex, final byte[][] keys,
			final byte[][] vals, final boolean readOnly,
			final boolean storeBlankNodes) {

		super(keySer, valSer, fromIndex, toIndex, keys, vals);

		this.readOnly = readOnly;

		this.toldBNodes = storeBlankNodes;

	}

	public static class BlobsWriteProcConstructor extends
			AbstractKeyArrayIndexProcedureConstructor<BlobsWriteProc> {

		private final boolean readOnly;
		private final boolean toldBNodes;

		@Override
		public final boolean sendValues() {

			return true;
            
        }

        public BlobsWriteProcConstructor(final boolean readOnly,
                final boolean toldBNodes) {

            this.readOnly = readOnly;

            this.toldBNodes = toldBNodes;

        }

        @Override
        public BlobsWriteProc newInstance(final IRabaCoder keySer,
                final IRabaCoder valSer, final int fromIndex,
                final int toIndex, final byte[][] keys, final byte[][] vals) {

            if(log.isInfoEnabled())
				log.info("ntuples=" + (toIndex - fromIndex));

			return new BlobsWriteProc(keySer, valSer, fromIndex, toIndex, keys,
					vals, readOnly, toldBNodes);

        }

    } // class BlobsWriteProcConstructor

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
	@Override
    public Result applyOnce(final IIndex ndx, final IRaba keys, final IRaba vals) {
        
        final int numTerms = keys.size();
        
        assert numTerms > 0 : "numTerms="+numTerms;

		// Helper class for index operations.
		final BlobsIndexHelper helper = new BlobsIndexHelper();
		
		// used to format the keys for the TERMS index.
		final IKeyBuilder keyBuilder = helper.newKeyBuilder();

        // used to store the discovered / assigned hash collision counters.
        final int[] counters = new int[numTerms];

        /*
         * Note: The baseKey is shorter than the full key (it does not include
         * the hash collision counter).
         */
        final byte[] baseKey = new byte[keyBuilder.capacity()
                - BlobsIndexHelper.SIZEOF_COUNTER];

        // Temporary buffer used to format the [toKey] for the bucket scan.
        final byte[] tmp = new byte[BlobsIndexHelper.SIZEOF_PREFIX_KEY];
        
//        // Used to report the size of each collision bucket.
//        final AtomicInteger bucketSize = new AtomicInteger(0);
        
        // Incremented by the size of each collision bucket probed.
        long totalBucketSize = 0L;
        
        // The size of the largest collision bucket.
        int maxBucketSize = 0;

        final DataOutputBuffer kbuf = new DataOutputBuffer(
                0/* existingDataLength */, baseKey);

        for (int i = 0; i < numTerms; i++) {

            // Copy key into reused buffer to reduce allocation.
//            final byte[] baseKey = getKey(i);
			keys.copy(i, kbuf.reset());

			// decode the VTE from the flags.
			final VTE vte = AbstractIV
					.getVTE(KeyBuilder.decodeByte(baseKey[0]));

			final int counter;
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
                    counter = BlobsIndexHelper.NOT_FOUND;

                    /*
                     * FIXME Use this to track down people who pass in a blank
                     * node on a read-only request when we are not using told
                     * bnodes. Under these conditions we can not unify the blank
                     * node with the TERMS index so the node should not have
                     * been passed in at all (for efficiency reasons).
                     */
                    //throw new UnsupportedOperationException();

                } else {

					/*
					 * We are not in a told bnode mode and this is not a
					 * read-only request. The TERMS index will be used to assign
					 * a unique counter to complete the blank node's key. That
					 * counter is just the current size of the collision bucket
					 * at the time that we check the index. The collision bucket
					 * is increased by ONE since we insert the blank node into
					 * the index.
					 */

					// The size of the collision bucket (aka the assigned ctr).
					counter = helper.addBNode(ndx, keyBuilder, baseKey,
							vals.get(i), tmp);

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
            	final byte[] val = vals.get(i);
            	
				counter = helper.resolveOrAddValue(ndx, readOnly,
						keyBuilder, baseKey, val, tmp, null/* bucketSize */);

            }

			if (!readOnly && counter < 0)
				throw new AssertionError("counter=" + counter);

			counters[i] = counter;

			if (counter != BlobsIndexHelper.NOT_FOUND) {

				/*
				 * TODO This does not update the bucketSize when the Value was
				 * not found in the index. We could do this by changing the
				 * return value of resolveOrAddValue() to -rangeCount and
				 * casting to an (int). The (-rangeCount) could then be
				 * normalized to a marker as we pass the information back to the
				 * client. [Or just enable the bucketSize argument above.]
				 */
				if (maxBucketSize < counter) {

					maxBucketSize += counter;

				}

				totalBucketSize += counter;
				
			}
            
        }

		return new Result(totalBucketSize, maxBucketSize, counters);

    } // apply(ndx)

	@Override
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
	@Override
    protected void writeMetadata(final ObjectOutput out) throws IOException {

        super.writeMetadata(out);

        out.writeBoolean(readOnly);

    }

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
         * The counters assigned to each {@link Value} in the request. The
         * indices of this array are correlated with the indices of the array
         * provided to the request.
         * <p>
         * Note: The actual counters values are SHORTs, not INTs. However,
         * {@link BlobsIndexHelper#NOT_FOUND} is an INT value used to indicate
         * that the desired {@link BlobIV} was not discovered in the index. That
         * means that we need to interchange and represent the counters as an
         * int[].
         */
		public int[] counters;
        
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
		 *            The assigned/resolved collision counters.
		 */
		public Result(final long totalBucketSize, final int maxBucketSize,
				final int[] counters) {

			if(counters == null)
				throw new IllegalArgumentException();
            
            this.totalBucketSize = totalBucketSize;
            
            this.maxBucketSize = maxBucketSize;
            
            this.counters = counters;
            
        }

        private final static transient short VERSION0 = 0x0;
        
        @Override
		public void readExternal(final ObjectInput in) throws IOException,
				ClassNotFoundException {

			final short version = ShortPacker.unpackShort(in);

			if (version != VERSION0) {

				throw new IOException("Unknown version: " + version);

			}

			final int n = (int) LongPacker.unpackLong(in);

            totalBucketSize = LongPacker.unpackLong(in);

            maxBucketSize = LongPacker.unpackInt(in);
			
			counters = new int[n];

            for (int i = 0; i < n; i++) {
                
				final short tmp = ShortPacker.unpackShort(in);

				counters[i] = tmp == Short.MAX_VALUE ? BlobsIndexHelper.NOT_FOUND
						: tmp;
                
            }
            
        }

        @Override
        public void writeExternal(final ObjectOutput out) throws IOException {

            final int n = counters.length;
            
            ShortPacker.packShort(out, VERSION0);

            // The #of results.
			LongPacker.packLong(out, n);

			// The total bucket size across all buckets examined.
			LongPacker.packLong(out, totalBucketSize);
			
			// The size of the largest collision bucket examined.
			LongPacker.packLong(out, maxBucketSize);

			/*
			 * Write out the assigned/resolved collision counters.
			 * 
			 * Note: This uses a packed short encoding for the collision
			 * counters. If we see the marker for an unresolved collision
			 * counter (NOT_FOUND) then it is replaced with [Short.MAX_VALUE].
			 * This is fine as long as the collision counter is a byte. Since
			 * the [short] value is in [0:Short.MAX_VALUE] we can then pack it
			 * into the output stream.
			 */
			for (int i = 0; i < n; i++) {

				final int c = counters[i];

				final short tmp = c == BlobsIndexHelper.NOT_FOUND ? Short.MAX_VALUE
						: (short) c;

				ShortPacker.packShort(out, tmp);
                
            }
            
        }
        
    } // class Result

    /**
	 * {@link Split}-wise aggregation followed by combining the results across
	 * those splits in order to return an aggregated result whose counters[] is
	 * 1:1 with the original keys[][].
	 */
	@Override
	protected IResultHandler<Result, Result> newAggregator() {
	
		return new BlobResultAggregator(getKeys().size());

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
	private class BlobResultAggregator extends AbstractLocalSplitResultAggregator<Result> {

		/**
		 * 
		 * @param size
		 *            The #of elements in the request (which is the same as the
		 *            cardinality of the aggregated result).
		 */
		public BlobResultAggregator(final int size) {
			super(size);
		}

		@Override
		protected Result newResult(final int size, SplitValuePair<Split, Result>[] a) {

			long totalBucketSize = 0;

			int maxBucketSize = 0;

			final int[] counters = new int[size];

			for (int i = 0; i < a.length; i++) {

				final Split split = a[i].key;

				final Result tmp = a[i].val;

				totalBucketSize += tmp.totalBucketSize;

				maxBucketSize = Math.max(maxBucketSize, tmp.maxBucketSize);

				System.arraycopy(tmp.counters/* src */, 0/* srcPos */, counters/* dest */, split.fromIndex/* destPos */,
						split.ntuples/* length */);

			}

			/*
			 * Return the aggregated result.
			 */
			final Result r = new Result(totalBucketSize, maxBucketSize, counters);

			return r;
		}

	} // BlobResultHandler

}

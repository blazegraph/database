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

import org.apache.log4j.Logger;
import org.openrdf.model.Value;

import com.bigdata.btree.ICounter;
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
    private boolean storeBlankNodes;
    
    public final boolean isStoreBlankNodes() {
        
        return storeBlankNodes;
        
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

		this.storeBlankNodes = storeBlankNodes;

	}

	public static class TermsWriteProcConstructor extends
			AbstractKeyArrayIndexProcedureConstructor<TermsWriteProc> {

		private final boolean readOnly;
		private final boolean storeBlankNodes;

		public final boolean sendValues() {

			return true;
            
        }

        public TermsWriteProcConstructor(final boolean readOnly,
                final boolean storeBlankNodes) {

            this.readOnly = readOnly;

            this.storeBlankNodes = storeBlankNodes;

        }

        public TermsWriteProc newInstance(final IRabaCoder keySer,
                final IRabaCoder valSer, final int fromIndex,
                final int toIndex, final byte[][] keys, final byte[][] vals) {

            if(log.isInfoEnabled())
				log.info("ntuples=" + (toIndex - fromIndex));

			return new TermsWriteProc(keySer, valSer, fromIndex, toIndex, keys,
					vals, readOnly, storeBlankNodes);

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
        
        // used to assign IVs for blank nodes when NOT in told bnodes mode.
        final ICounter counter = ndx.getCounter();
        
		final byte[] baseKey = new byte[keyBuilder.capacity()];
        
        for (int i = 0; i < numTerms; i++) {

            // Copy key into reused buffer to reduce allocation.
//            final byte[] baseKey = getKey(i);
			getKeys().copy(i, new DataOutputBuffer(0, baseKey));

			// decode the VTE from the flags.
			final VTE vte = AbstractIV.getVTE(baseKey[0]);
            
			if (!storeBlankNodes && vte == VTE.BNODE) {

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

                } else {

					/*
					 * FIXME We may have to revisit this logic for constructing
					 * a unique blank node identifier.
					 * 
					 * If you are inserting blank nodes and you are not in told
					 * bnodes mode, then each time we insert a blank node it
					 * needs to be assigned a distinct IV. (In fact, there is no
					 * reason to send a blank node to the TERMS index if you are
					 * only reading on it and you are not in told bnodes mode
					 * because the unification will always fail.)
					 * 
					 * That could be done using a counter to choose a hash code,
					 * but it must be a hash code within the key range of a
					 * shard. Perhaps decoding the from/to key of the range and
					 * then rolling a random number for the hash code would be
					 * efficient. Since there could already be one (or more)
					 * values in the TERMS index for that hash code we then have
					 * to range count the collision bucket and insert a new
					 * tuple whose collision counter is that range count.
					 * 
					 * It would seem to be far more efficient to use UUIDs for
					 * blank nodes and then inline them if we are not in a told
					 * bnodes mode than to permit them to be written onto the
					 * TERMS index. When we are in a told bnodes mode, then they
					 * can be unified against the TERMS index but it is still
					 * more efficient to inline whenever possible.
					 */
                	
//                    // assign a term identifier.
//					final long termId = counter.incrementAndGet();
//
//                    ivs[i] = new TermId(VTE(code), termId);
                	
                	throw new UnsupportedOperationException();
                    
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
						keyBuilder, baseKey, val);

				if (key != null) {

					// Note: The first byte of the key is the IV's flags.
					ivs[i] = new TermId<BigdataValue>(key);
					
				} else {
					
					// Not found (and read-only).
					
				}
    
            }
            
        }

        return new Result(ivs);

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
    static class Result implements Externalizable {

        public IV[] ivs;
        
        private static final long serialVersionUID = 1L;

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
            
            LongPacker.packLong(out,n);

            for (int i = 0; i < n; i++) {
                                
                out.writeObject(ivs[i]);
                
            }
            
        }
        
    } // class Result

}

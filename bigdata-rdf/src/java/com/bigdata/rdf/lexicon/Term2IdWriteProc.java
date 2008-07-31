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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.AbstractIndexProcedureConstructor;
import com.bigdata.btree.AbstractKeyArrayIndexProcedure;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.ICounter;
import com.bigdata.btree.IDataSerializer;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IParallelizableIndexProcedure;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.store.IRawTripleStore;

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
 * against the terms index as satisifactory evidence that the term also exists
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
 * may be analyzed as a batch and variant of the following pseudocode.
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
        IParallelizableIndexProcedure {
    
    protected static final Logger log = Logger.getLogger(Term2IdWriteProc.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

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

    /*
     * Flag enables optional ground truth verification. This is not a scaleable
     * option! It is only enabled at the DEBUG level IFF this flag is ALSO set.
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
     * De-serialization constructor.
     */
    public Term2IdWriteProc() {
        
    }
    
    protected Term2IdWriteProc(IDataSerializer keySer, int fromIndex, int toIndex,
            byte[][] keys, boolean readOnly) {

        super(keySer, null, fromIndex, toIndex, keys, null /* vals */);
        
        this.readOnly = readOnly;
        
    }

    public static class Term2IdWriteProcConstructor extends
            AbstractIndexProcedureConstructor<Term2IdWriteProc> {

        public static Term2IdWriteProcConstructor READ_WRITE = new Term2IdWriteProcConstructor(false);
        
        public static Term2IdWriteProcConstructor READ_ONLY = new Term2IdWriteProcConstructor(true);

        private final boolean readOnly;
        
        private Term2IdWriteProcConstructor(boolean readOnly) {
            
            this.readOnly = readOnly;
            
        }
        
        public Term2IdWriteProc newInstance(IDataSerializer keySer,
                IDataSerializer valSer,int fromIndex, int toIndex,
                byte[][] keys, byte[][] vals) {

            assert vals == null;
            
            return new Term2IdWriteProc(keySer, fromIndex, toIndex, keys, readOnly);

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
     */
    public Object apply(IIndex ndx) {
        
        final int numTerms = getKeyCount();
        
        assert numTerms > 0 : "numTerms="+numTerms;
        
        // used to store the discovered / assigned term identifiers.
        final long[] ids = new long[numTerms];
        
        // used to assign term identifiers.
        final ICounter counter = ndx.getCounter();
        
//        if (counter.get() == IRawTripleStore.NULL) {
//            /*
//             * Note: we never assign this value as a term identifier.
//             */
//            counter.incrementAndGet();
//        }
        
        // used to serialize term identifers.
        final DataOutputBuffer idbuf = new DataOutputBuffer(Bytes.SIZEOF_LONG);
        
        // #of new terms (#of writes on the index).
        int nnew = 0;
        for (int i = 0; i < numTerms; i++) {

            final byte[] key = getKey(i);
            
            final long termId;

            // Lookup in the forward index.
            final byte[] tmp = ndx.lookup(key);

            if (tmp == null) {

                if(readOnly) continue;
                
                /*
                 * Not found - assign the termId.
                 * 
                 * Note: We set the low two bits to indicate whether a term is a
                 * literal, bnode, URI, or statement so that we can tell at a
                 * glance (without lookup up the term in the index) what "kind"
                 * of thing the term identifier stands for.
                 * 
                 * @todo we could use negative term identifiers except that we
                 * pack the termId in a manner that does not allow negative
                 * integers. a different pack routine would allow us all bits.
                 */
                final long id0 = counter.incrementAndGet();
                
                // 0L is never used as a counter value.
                assert id0 != IRawTripleStore.NULL;

                /*
                 * left shift two bits to make room for term type coding.
                 * 
                 * Note: when using partitioned indices the partition identifier
                 * is already in the high word like [partitionId|counter], so
                 * this shifts everything left by two bits. The result is that
                 * the #of partitions is reduced four-fold rather than the #of
                 * distinct counter values within a given index partition.
                 */
                long id = id0 << 2;
                
                // this byte encodes the kind of term (URI, Literal, BNode, etc.)
                final byte code = KeyBuilder.decodeByte(key[0]);
                
                switch(code) {
                case ITermIndexCodes.TERM_CODE_URI:
                    id |= ITermIdCodes.TERMID_CODE_URI;
                    break;
                case ITermIndexCodes.TERM_CODE_LIT:
                case ITermIndexCodes.TERM_CODE_DTL:
                case ITermIndexCodes.TERM_CODE_LCL:
                    id |= ITermIdCodes.TERMID_CODE_LITERAL;
                    break;
                case ITermIndexCodes.TERM_CODE_BND:
                    id |= ITermIdCodes.TERMID_CODE_BNODE;
                    break;
                case ITermIndexCodes.TERM_CODE_STMT:
                    id |= ITermIdCodes.TERMID_CODE_STATEMENT;
                    break;
                default: 
                    throw new AssertionError("Unknown term type: code=" + code);
                }
                
                termId = id;
                
                if (DEBUG && enableGroundTruth) {
                    
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
                        final long pid = id0 >> 32;
                        final long mask = 0xffffffffL;
                        final int ctr = (int) (id0 & mask);
                        
                        throw new AssertionError("assignment not unique"+//
                                ": termId=" + termId +//
                                ", oldKey=" + BytesUtil.toString(oldKey) + //
                                ", newKey=" + BytesUtil.toString(key)+//
                                ", pmd="+ndx.getIndexMetadata().getPartitionMetadata()+//
                                ", pid="+pid+", ctr="+ctr+//
                                ", counter="+counter.getClass().getName());
                        
                    }
                    

                }
                
                // format as packed long integer.
                try {
                    
                    idbuf.reset().packLong(termId);
                    
                } catch(IOException ex) {
                    
                    throw new RuntimeException(ex);
                    
                }

                // insert into index.
                if (ndx.insert(key, idbuf.toByteArray()) != null) {

                    throw new AssertionError();

                }
                
                nnew++;

            } else { // found.

                try {
                    
                    termId = new DataInputBuffer(tmp).unpackLong();
                    
                } catch (IOException ex) {
                    
                    throw new RuntimeException(ex);
                    
                }
                   
            }

            ids[i] = termId;

        }

        /*
         * comment out - this is for debugging (it does not actually rely on
         * ground truth and was used to help track down a lost updates problem).
         */
        if (enableGroundTruth && ndx.getIndexMetadata().getPartitionMetadata() != null) {
            
            // the partition identifier (assuming index is partitioned).
            final long id0 = counter.get();
            final long pid = id0 >> 32;
            final long mask = 0xffffffffL;
            final int ctr = (int) (id0 & mask);

            // note: the mutable btree - accessed here for debugging only.
            final BTree btree;
            if (ndx instanceof AbstractBTree) {
                btree = (BTree) ndx;
            } else {
                btree = (BTree) ((FusedView) ndx).getSources()[0];
            }
            
            log.warn("after task"+
            ": nnew="+nnew+//
            ", partitionId="+ndx.getIndexMetadata().getPartitionMetadata().getPartitionId()+//
            ", pid="+pid+//
            ", ctr="+ctr+//
            ", counter="+counter.getClass().getName()+//
            ", sourceCheckpoint="+btree.getCheckpoint()// btree was loaded from here.
            );
            
        }
        
        return new Result(ids);

    }
    protected void readMetadata(ObjectInput in) throws IOException, ClassNotFoundException {
        
        super.readMetadata(in);
        
        readOnly = in.readBoolean();
        
    }

    /**
     * Writes metadata (not the keys or values, but just other metadata used by
     * the procedure).
     * <p>
     * The default implementation writes <code>toIndex - fromIndex</code>, which
     * is the #of keys.
     * 
     * @param out
     * 
     * @throws IOException
     */
    protected void writeMetadata(ObjectOutput out) throws IOException {
        
        super.writeMetadata(out);
        
        out.writeBoolean(readOnly);
        
    }
    
    /**
     * Object encapsulates the discovered / assigned term identifiers and
     * provides efficient serialization for communication of those data to
     * the client.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
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
            
            for(int i=0; i<n; i++) {
                
                ids[i] = LongPacker.unpackLong(in);
                
            }
            
        }

        public void writeExternal(ObjectOutput out) throws IOException {

            final int n = ids.length;
            
            ShortPacker.packShort(out, VERSION0);
            
            LongPacker.packLong(out,n);

            for(int i=0; i<n; i++) {
                
                LongPacker.packLong(out, ids[i]);
                
            }
            
        }
        
    }
    
}

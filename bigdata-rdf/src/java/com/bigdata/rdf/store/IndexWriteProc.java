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
 * Created on Jan 25, 2008
 */
package com.bigdata.rdf.store;

import it.unimi.dsi.mg4j.io.InputBitStream;
import it.unimi.dsi.mg4j.io.OutputBitStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractKeyArrayIndexProcedure;
import com.bigdata.btree.IDataSerializer;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.AbstractIndexProcedureConstructor;
import com.bigdata.btree.IParallelizableIndexProcedure;
import com.bigdata.btree.IRandomAccessByteArray;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.SPO;

/**
 * Procedure for batch index on a single statement index (or index
 * partition).
 * <p>
 * The key for each statement encodes the {s:p:o} of the statement in the
 * order that is appropriate for the index (SPO, POS, OSP, etc).
 * <p>
 * The value for each statement is a single byte that encodes the
 * {@link StatementEnum} and also encodes whether or not the "override" flag
 * is set.  See {@link SPO#override}.
 * <p>
 * Note: This needs to be a custom batch operation using a conditional
 * insert so that we do not write on the index when the data would not be
 * changed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexWriteProc extends AbstractKeyArrayIndexProcedure implements
        IParallelizableIndexProcedure {

    protected static final Logger log = Logger.getLogger(IndexWriteProc.class);

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

    /**
     * 
     */
    private static final long serialVersionUID = 3969394126242598370L;

    @Override
    protected IDataSerializer getKeySerializer() {

        return new FastRDFKeyCompression(IRawTripleStore.N);

    }

    @Override
    protected IDataSerializer getValSerializer() {

        return new FastRDFValueCompression();

    }

    /**
     * De-serialization constructor.
     */
    public IndexWriteProc() {

    }

    /**
     * 
     * @param fromIndex
     * @param toIndex
     * @param keys
     * @param vals
     */
    protected IndexWriteProc(IDataSerializer keySer,
            IDataSerializer valSer,int fromIndex, int toIndex, byte[][] keys, byte[][] vals) {

        super(keySer,valSer,fromIndex, toIndex, keys, vals);

        assert vals != null;

    }

    public static class IndexWriteProcConstructor extends
            AbstractIndexProcedureConstructor<IndexWriteProc> {

        public static IndexWriteProcConstructor INSTANCE = new IndexWriteProcConstructor();
        
        private IndexWriteProcConstructor() {}
        
        public IndexWriteProc newInstance(IDataSerializer keySer,
                IDataSerializer valSer,int fromIndex, int toIndex,
                byte[][] keys, byte[][] vals) {

            return new IndexWriteProc(keySer,valSer,fromIndex, toIndex, keys, vals);

        }
        
    }
    
    /**
     * 
     * @return The #of statements actually written on the index as an
     *         {@link Long}.
     */
    public Object apply(IIndex ndx) {

        // #of statements actually written on the index partition.
        long writeCount = 0;

        final int n = getKeyCount();

        for (int i = 0; i < n; i++) {

            // the key encodes the {s:p:o} of the statement.
            final byte[] key = getKey(i);
            assert key != null;

            // the value encodes the statement type.
            final byte[] val = getValue(i);
            assert val != null;
            assert val.length == 1;

            // figure out if the override bit is set.
            final boolean override = StatementEnum.isOverride(val[0]);

            /*
             * Decode the new (proposed) statement type (override bit is
             * masked off).
             */
            final StatementEnum newType = StatementEnum.decode(val[0]);

            /*
             * The current statement type in this index partition (iff the
             * stmt is defined.
             */
            final byte[] oldval = ndx.lookup(key);

            if (oldval == null) {

                /*
                 * Statement is NOT pre-existing.
                 */

                ndx.insert(key, newType.serialize());

                writeCount++;

            } else {

                /*
                 * Statement is pre-existing.
                 */

                // old statement type.
                final StatementEnum oldType = StatementEnum.deserialize(oldval);

                if (override) {

                    if (oldType != newType) {

                        /*
                         * We are downgrading a statement from explicit to
                         * inferred during TM
                         */

                        ndx.insert(key, newType.serialize());

                        writeCount++;

                    }

                } else {

                    // choose the max of the old and the proposed type.
                    final StatementEnum maxType = StatementEnum.max(oldType,
                            newType);

                    if (oldType != maxType) {

                        /*
                         * write on the index iff the type was actually
                         * changed.
                         */

                        ndx.insert(key, maxType.serialize());

                        writeCount++;

                    }

                }

            }

        }

        return Long.valueOf(writeCount);

    }
    
    /**
     * A fast bit-coding of the keys and values for an RDF statement index. The
     * approach uses a fixed length code for the statement keys and a fixed bit
     * length (3 bits) for the statement values.
     * <p>
     * Each key is logically N 64-bit integers, where N is 3 for a triple store
     * or 4 for a quad store. The distinct long values in the keys are
     * identified - these form the symbols of the alphabet. Rather than using a
     * frequency distribution over those symbols (ala hamming or hu-tucker) a
     * fixed code length is choosen based on the #of distinctions that we need
     * to preserve and codes are assigned based an arbitrary sequence. Since the
     * codes are fixed length we do not need the prefix property. The goal of
     * this approach is to compress the keys as quickly as possible. Non-goals
     * include minimum information entropy and order-preserving compression (the
     * keys need to be decompressed before they can be processed by the
     * procedure on in the data service so there is no reason to use an order
     * preserving compression).
     * 
     * @todo try a variant that uses huffman and another that uses hu-tucker in
     *       order to assess the relative cost of those methods.
     * 
     * @todo test suite.
     *       
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class FastRDFKeyCompression implements IDataSerializer, Externalizable {

        /**
         * 
         */
        private static final long serialVersionUID = -6920004199519508113L;

        private int N;

        /**
         * The natural log of 2.
         */
        final static transient private double LOG2 = Math.log(2);

        /**
         * Triple store.
         */
        final static transient public IDataSerializer N3 = new FastRDFKeyCompression(3);

        /**
         * Quad store.
         */
        final static transient public IDataSerializer N4 = new FastRDFKeyCompression(4);
        
        /**
         * De-serialization ctor.
         */
        public FastRDFKeyCompression() {
            
        }
        
        /**
         * @param N
         *            Either 3 or 4 depending on whether it is a triple or a
         *            quad store index.
         */
        public FastRDFKeyCompression(int N) {

            this.N = N;

            assert N == 3 || N == 4 : "Expecting either 3 or 4 not "+N;

        }

        protected void add(HashMap<Long, Integer> symbols, Long v) {

            if (symbols.containsKey(v))
                return;

            symbols.put(v, symbols.size());

        }

        /**
         * Identifies the distinct symbols (64-bit long integers) in the keys
         * and assigns each symbol a unique integer code.
         * 
         * @param keys
         * @return A map from the long value to the size of the map at the time
         *         that the value was encountered (a one up integer in [0:n-1]
         *         where n := toIndex-fromIndex).
         */
        protected HashMap<Long, Integer> getSymbols(IRandomAccessByteArray raba) {

            int n = raba.getKeyCount();
            
            assert n >= 0;
            
            final HashMap<Long, Integer> symbols = new HashMap<Long, Integer>(n
                    * N);

            for (int i = 0; i < n; i++) {

                final byte[] key = raba.getKey( i );

                assert key.length == N * Bytes.SIZEOF_LONG : "Expecting key with "
                        + N * Bytes.SIZEOF_LONG + " bytes, not " + key.length;

                for (int j = 0, off = 0; j < N; j++, off += 8) {

                    add(symbols, KeyBuilder.decodeLong(key, off));

                }

            }

            return symbols;

        }

        public void read(DataInput in, IRandomAccessByteArray raba) throws IOException {
          
            InputBitStream ibs = new InputBitStream((InputStream) in, 0/* unbuffered! */);

            /*
             * read the header.
             */
            final int n = ibs.readNibble();
            if(n==0) return;
            final int nsymbols = ibs.readNibble();
            final int codeBitLength = ibs.readNibble();

            /*
             * read the dictionary, building a reverse lookup from code to
             * value.
             * 
             * Note: An array of length [nsymbols] is used since the codes are
             * one up integers in [0:nsymbols-1]. We just index into the by the
             * code to store the symbol or translate a code to a symbol.
             */
            final long[] symbols = new long[nsymbols];
            {

                for (int i = 0; i < nsymbols; i++) {

                    final long v = ibs.readLongNibble();

                    final int code = ibs.readInt(codeBitLength);

                    symbols[code] = v;

                }

            }

            /*
             * read the codes, expanding them into keys.
             */
//            final byte[][] keys = new byte[deserializedSize][];
            {

                KeyBuilder keyBuilder = new KeyBuilder(N * Bytes.SIZEOF_LONG);

                for (int i = 0; i < n; i++) {

                    keyBuilder.reset();

                    for (int j = 0; j < N; j++) {

                        final int code = ibs.readInt(codeBitLength);

                        final long v = symbols[code];

                        keyBuilder.append(v);

                    }

//                    keys[i] = keyBuilder.getKey();
                    raba.add(keyBuilder.getBuffer(), 0/*off*/, keyBuilder.getLength());

                }

            }
            
//            return keys;
            
        }

        public void write( DataOutput out, IRandomAccessByteArray raba)
                throws IOException {

            final OutputBitStream obs = new OutputBitStream((OutputStream) out,0/*unbuffered*/);

            final int n = raba.getKeyCount();
            
            assert n >= 0;
            
            obs.writeNibble(n);
            
            if(n==0) {
             
                /*
                 * Note: ALWAYS flush.
                 */
                obs.flush();

                return;
                
            }
        
            final HashMap<Long, Integer> symbols = getSymbols(raba);

            final int nsymbols = symbols.size();

            /*
             * The bit length of the code.
             * 
             * Note: The code for a long value is simply its index in the
             * symbols[].
             */
            final int codeBitLength = (int) Math.ceil(Math.log(nsymbols)
                    / LOG2);

            assert codeBitLength > 0 : "nstmts=" + n + ", nsymbols="
                    + nsymbols + ", codeBitLength=" + codeBitLength;

            {

                /*
                 * write the header {nsymbols, codeBitLength}.
                 */
                // obs.writeNibble(n);
                obs.writeNibble(nsymbols);
                obs.writeNibble(codeBitLength);

                /*
                 * write the dictionary:
                 * 
                 * {packed(symbol) -> bits(code)}*
                 * 
                 * The entries are written in an arbitrary order.
                 */
                {

                    Iterator<Map.Entry<Long, Integer>> itr = symbols
                            .entrySet().iterator();

                    while (itr.hasNext()) {

                        Map.Entry<Long, Integer> entry = itr.next();

                        obs.writeLongNibble(entry.getKey());

                        obs.writeInt(entry.getValue(), codeBitLength);

                    }

                }

                /*
                 * write the codes for the keys.
                 */
                {

                    for (int i = 0; i < n; i++) {

                        final byte[] key = raba.getKey(i);

                        for (int j = 0, off = 0; j < N; j++, off += 8) {

                            final long v = KeyBuilder.decodeLong(key, off);

                            obs.writeInt(symbols.get(v).intValue(),
                                    codeBitLength);

                        }

                    }

                }

                /*
                 * Note: ALWAYS flush.
                 */
                obs.flush();

                /*
                 * @todo counters for time in each phase of this process.
                 * 
                 * @todo since the procedure tends to write large byte[] make
                 * sure that the RPC buffers are good at that, e.g., either they
                 * auto-extend aggressively or they are writing onto a fixed
                 * buffer that writes on a socket.
                 */
                if(INFO)
                log.info(n + " statements were serialized in "
                        + obs.writtenBits() + " bytes using " + nsymbols
                        + " symbols with a code length of " + codeBitLength
                        + " bits.");

                // // copy onto the output buffer.
                // out.write(buf);

            }
            
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            
            out.writeByte(N);
            
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

            N = in.readByte();
            
        }

    }

    /**
     * We encode the value in 3 bits per statement. The 1st bit is the override
     * flag. The remaining two bits are the statement type {inferred, explicit,
     * or axiom}.  The value b111 is used as a placeholder for a deleted index
     * entry and will be present iff delete markers are used by the index - it
     * de-serializes to a [null].
     * <p>
     * Note: the 'override' flag is NOT stored in the statement indices, but it
     * is passed by the procedure that writes on the statement indices so that
     * we can decide whether or not to override the type when the statement is
     * pre-existing in the index.
     * 
     * @todo test suite.
     * 
     * @see StatementEnum
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class FastRDFValueCompression implements IDataSerializer, Externalizable {

        /**
         * 
         */
        private static final long serialVersionUID = 1933430721504168533L;

        /**
         * Sole constructor (handles de-serialization also).
         */
        public FastRDFValueCompression() {

        }

        public void read(DataInput in, IRandomAccessByteArray raba) throws IOException {

            InputBitStream ibs = new InputBitStream((InputStream) in, 0/* unbuffered! */);

            /*
             * read the values.
             */
            
            final int n = ibs.readNibble();

            for (int i = 0; i < n; i++) {

                int b = ibs.readInt(3);
                
                if (b == 7) {
                 
                    // A deleted value.
                    
                    raba.add(null);
                    
                } else {
                
                    raba.add(new byte[] { (byte) b });
                    
                }

            }
            
        }

        public void write(DataOutput out, IRandomAccessByteArray raba) throws IOException {

            final OutputBitStream obs = new OutputBitStream((OutputStream) out, 0 /* unbuffered! */);

            /*
             * write the values.
             */

            final int n = raba.getKeyCount();
            
            assert n >= 0;
            
            obs.writeNibble(n);

            for (int i = 0; i < n; i++) {

                if (raba.isNull(i)) {

                    // flag a deleted value (de-serialize to a null).
                    obs.writeInt( 7, 3 );
                    
                } else {

                    final byte[] val = raba.getKey(i);

                    obs.writeInt((int) val[0], 3);
                    
                }

            }

            /*
             * Note: ALWAYS flush!
             */
            obs.flush();

        }

        public void writeExternal(ObjectOutput out) throws IOException {

            // NOP
            
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            
            // NOP
            
        }

    }

}

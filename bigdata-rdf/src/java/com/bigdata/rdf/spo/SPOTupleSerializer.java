/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 23, 2008
 */

package com.bigdata.rdf.spo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.IDataSerializer;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.accesspath.IKeyOrder;

/**
 * (De-)serializes {@link SPO}s for statement indices.
 * <p>
 * Note: the encoded key for a statement is formed from the 64-bit
 * <code>long</code> term identifier for the subject, predicate, and object
 * positions of the statement. Each statement index uses a permutation of those
 * term identifiers, e.g., {s,p,o}, {o,s,p}, or {p,o,s}. The {@link SPOKeyOrder}
 * identifies the specific permutation for a given index. The keys are fully
 * decodable and are NOT stored redundently in the tuple's value.
 * <p>
 * The tuple value encodes the {@link StatementEnum}, indicating whether the
 * statement is {explicit, inferred, or an axiom}, and optionally the unique
 * statement identifier.
 * <p>
 * Note: While the static methods used to decode an existing key are safe for
 * concurrent readers, concurrent readers also form keys using
 * {@link #statement2Key(long, long, long)} and therefore require a thread-local
 * {@link IKeyBuilder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOTupleSerializer extends DefaultTupleSerializer<SPO,SPO> {

    private static final long serialVersionUID = 2893830958762265104L;

    private static transient final int N = IRawTripleStore.N;
    
    /**
     * The natural order for the index.
     */
    private SPOKeyOrder keyOrder;
    
    /**
     * Used to format the value.
     */
    private final transient ByteArrayBuffer buf = new ByteArrayBuffer(0);

//    /**
//     * Used to format the key.
//     */
//    private final transient IKeyBuilderFactory keyBuilderFactory = new ThreadLocalKeyBuilderFactory(
//            new ASCIIKeyBuilderFactory(N * Bytes.SIZEOF_LONG));
//
//    public IKeyBuilder getKeyBuilder() {
//      
//        return keyBuilderFactory.getKeyBuilder();
//        
//    };
    
    /**
     * De-serialization constructor.
     */
    public SPOTupleSerializer() {
        
    }

    /**
     * Create an {@link ITupleSerializer} for the indicated access path.
     * 
     * @param keyOrder
     *            The access path.
     */
    public SPOTupleSerializer(SPOKeyOrder keyOrder) {

        this(keyOrder, getDefaultLeafKeySerializer(),
                getDefaultValueKeySerializer());

    }
    
    /**
     * Create an {@link ITupleSerializer} for the indicated access path.
     * 
     * @param keyOrder
     *            The access path.
     * @param leafKeySer
     * @param leafValSer
     */
    public SPOTupleSerializer(SPOKeyOrder keyOrder, IDataSerializer leafKeySer,
            IDataSerializer leafValSer) {

        super(new ASCIIKeyBuilderFactory(N * Bytes.SIZEOF_LONG), leafKeySer,
                leafValSer);
        
        if (keyOrder == null)
            throw new IllegalArgumentException();
        
        this.keyOrder = keyOrder;
        
    }
    
    public SPO deserialize(ITuple tuple) {

        if (tuple == null)
            throw new IllegalArgumentException();

//      // clone of the key.
//      final byte[] key = itr.getKey();
      
        // copy of the key in a reused buffer.
        final byte[] key = tuple.getKeyBuffer().array(); 

//        long[] ids = new long[IRawTripleStore.N];
        
        /*
         * Note: GTE since the key is typically a reused buffer which may be
         * larger than the #of bytes actually holding valid data.
         */
        assert key.length >= 8 * N;
//      assert key.length == 8 * IRawTripleStore.N + 1;
        
//        final long _0 = KeyBuilder.decodeLong(key, 1);
//      
//        final long _1 = KeyBuilder.decodeLong(key, 1+8);
//      
//        final long _2 = KeyBuilder.decodeLong(key, 1+8+8);

        /*
         * Decode the key.
         */
        
        final long _0 = KeyBuilder.decodeLong(key, 0);
        
        final long _1 = KeyBuilder.decodeLong(key, 8);
      
        final long _2 = KeyBuilder.decodeLong(key, 8+8);
        
        /*
         * Re-order the key into SPO order.
         */
        
        final long s, p, o;
        
        switch (keyOrder.index()) {

        case SPOKeyOrder._SPO:
            s = _0;
            p = _1;
            o = _2;
            break;
            
        case SPOKeyOrder._POS:
            p = _0;
            o = _1;
            s = _2;
            break;
            
        case SPOKeyOrder._OSP:
            o = _0;
            s = _1;
            p = _2;
            break;

        default:

            throw new UnsupportedOperationException();

        }
        
        if((tuple.flags()&IRangeQuery.VALS)==0) {
        
            // Note: No type or statement identifier information.
            final SPO spo = new SPO(s, p, o);
            
            return spo;
            
        }
        
        /*
         * Decode the StatementEnum and the optional statement identifier.
         */

        final ByteArrayBuffer vbuf = tuple.getValueBuffer();
        
        final StatementEnum type = StatementEnum.decode( vbuf.array()[0] ); 
        
        final SPO spo = new SPO(s, p, o, type);
        
        if (vbuf.limit() == 1 + 8) {

            /*
             * The value buffer appears to contain a statement identifier, so we
             * read it.
             */
            
            spo.setStatementIdentifier( vbuf.getLong(1) );

            // @todo asserts.
//            assert AbstractTripleStore.isStatement(sid) : "Not a statement identifier: "
//                    + toString(sid);
//
//            assert type == StatementEnum.Explicit : "statement identifier for non-explicit statement : "
//                    + toString();
//
//            assert sid != NULL : "statement identifier is NULL for explicit statement: "
//                    + toString();

        }

        return spo;
        
    }

    public SPO deserializeKey(ITuple tuple) {
        
        // just de-serialize the whole tuple.
        return deserialize(tuple);
        
    }

    public byte[] serializeKey(Object obj) {

        if (obj == null)
            throw new IllegalArgumentException();
        
        if (obj instanceof SPO)
            return serializeKey((SPO) obj);

        //@todo could allow long[3].
        throw new UnsupportedOperationException();
        
    }

    public byte[] serializeKey(SPO spo) {
        
        return statement2Key(keyOrder, spo);
        
    }

    /**
     * Forms the statement key.
     * 
     * @param keyOrder
     *            The key order.
     * @param spo
     *            The statement.
     * 
     * @return The key.
     */
    public byte[] statement2Key(IKeyOrder<SPO> keyOrder, SPO spo) {
        
        switch (((SPOKeyOrder)keyOrder).index()) {

        case SPOKeyOrder._SPO:
        
            return statement2Key(spo.s, spo.p, spo.o);
            
        case SPOKeyOrder._POS:
            
            return statement2Key(spo.p, spo.o, spo.s);
            
        case SPOKeyOrder._OSP:
            
            return statement2Key(spo.o, spo.s, spo.p);
            
        default:
            throw new UnsupportedOperationException("keyOrder=" + keyOrder);
        
        }
        
    }
    
    /**
     * Encodes a statement represented as three long integers as an unsigned
     * byte[] sort key.
     * <p>
     * Note: while the conversion of long integers into the byte[] is
     * non-trivial the value identifiers are mapped onto 8 bytes at a time and
     * the contents of the array could be rearranged into alternative orders
     * directly. For example, if you provide (s,p,o) then you could form the
     * (p,o,s) key by copying 8 byte sections of the returned sort key around to
     * generate the desired permutation.
     * <p>
     * Note: When an identifier is {@link IRawTripleStore#NULL} we can generate
     * a shorter key by not including the NULL value.  This should be fine since
     * identifiers SHOULD NOT be NULL unless they are in the tail position(s) of
     * a triple pattern.  Such keys are always used for rangeCount or rangeQuery
     * purposes where the additional length does not matter (unless it interacts
     * with how we choose to compact the keys for RPC calls).
     * 
     * @param id1
     *            An RDF value identifier from the term index.
     * @param id2
     *            An RDF value identifier from the term index.
     * @param id3
     *            An RDF value identifier from the term index.
     * 
     * @return The sort key for the statement with those values.
     */
    public byte[] statement2Key(long id1, long id2, long id3) {

        return getKeyBuilder().reset().append(id1).append(id2).append(id3)
                .getKey();

    }

    /**
     * Encodes the {@link StatementEnum} and the optional statement identifier.
     */
    public byte[] serializeVal(SPO spo) {

        if (spo == null)
            throw new IllegalArgumentException();

        buf.reset();

        final StatementEnum type = spo.getType();

        // optionally set the override bit on the value.
        final byte b = (byte) (spo.override ? (type.code() | StatementEnum.MASK_OVERRIDE)
                : type.code());

        buf.putByte(b);

        if (spo.hasStatementIdentifier()) {

            assert type == StatementEnum.Explicit : "Statement identifier not allowed: type="
                    + type;

            buf.putLong(spo.getStatementIdentifier());

        }

        return buf.toByteArray();
        
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        super.readExternal(in);
        
        keyOrder = SPOKeyOrder.valueOf(in.readByte());

    }

    public void writeExternal(ObjectOutput out) throws IOException {

        super.writeExternal(out);
        
        out.writeByte(keyOrder.index());

    }

}

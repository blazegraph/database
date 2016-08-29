/*

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
 * Created on Jun 23, 2008
 */

package com.bigdata.rdf.spo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.model.StatementEnum;

/**
 * (De-)serializes {@link SPO}s for statement indices.
 * <p>
 * Note: the encoded key for a statement is formed from the 64-bit
 * <code>long</code> term identifier for the subject, predicate, and object
 * positions of the statement. Each statement index uses a permutation of those
 * term identifiers, e.g., {s,p,o}, {o,s,p}, or {p,o,s}. The {@link SPOKeyOrder}
 * identifies the specific permutation for a given index. The keys are fully
 * decodable and are NOT stored redundantly in the tuple's value.
 * <p>
 * The tuple value encodes the {@link StatementEnum}, indicating whether the
 * statement is {explicit, inferred, or an axiom}, and optionally the unique
 * statement identifier.
 * <p>
 * Note: While the static methods used to decode an existing key are safe for
 * concurrent readers, concurrent readers also form keys using
 * {@link #statement2Key(IV, IV, IV)} and therefore require a thread-local
 * {@link IKeyBuilder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class SPOTupleSerializer extends DefaultTupleSerializer<SPO,SPO> {

    private static final long serialVersionUID = 2893830958762265104L;
    
//    private static final transient Logger log = Logger.getLogger(SPOTupleSerializer.class);
    
    /**
     * The natural order for the index.
     */
    private SPOKeyOrder keyOrder;
    
    /**
     * If true, explicit SPOs decoded from index tuples will have a sid attached.
     */
    private boolean sids;
    
//    /**
//     * Used to format the value.
//     */
//    private final transient ByteArrayBuffer buf = new ByteArrayBuffer(0);

    public SPOKeyOrder getKeyOrder() {

        return keyOrder;
        
    }
    
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
    public SPOTupleSerializer(final SPOKeyOrder keyOrder, final boolean sids) {

        this(keyOrder, sids, getDefaultLeafKeysCoder(), getDefaultValuesCoder());

    }
    
    /**
     * Create an {@link ITupleSerializer} for the indicated access path.
     * 
     * @param keyOrder
     *            The access path.
     * @param sids
     *            If true, attach sids to decoded SPOs where appropriate.            
     * @param leafKeySer
     * @param leafValSer
     */
    public SPOTupleSerializer(final SPOKeyOrder keyOrder,
            final boolean sids,
            final IRabaCoder leafKeySer, final IRabaCoder leafValSer) {

        super(new ASCIIKeyBuilderFactory(), leafKeySer, leafValSer);
        
//        if (keyOrder == null)
//            throw new IllegalArgumentException();
        
        this.keyOrder = keyOrder;
        
        this.sids = sids;
        
    }
    
    @Override
    public byte[] serializeKey(final Object obj) {

        if (obj == null)
            throw new IllegalArgumentException();
        
        if (obj instanceof SPO)
            return serializeKey((SPO) obj);

        throw new UnsupportedOperationException();
        
    }

    /**
     * Forms the statement key.
     * 
     * @param spo
     *            The statement.
     * 
     * @return The key.
     */
    public byte[] serializeKey(final ISPO spo) {
        
        return keyOrder.encodeKey(getKeyBuilder(), spo);
        
    }

    /**
     * Variant duplicates the behavior of {@link #serializeVal(SPO)} to provide
     * support for non-{@link SPO} {@link ISPO}s.
     */
    public byte[] serializeVal(final ISPO spo) {

        if (spo == null)
            throw new IllegalArgumentException();

        return serializeVal(//buf,
                spo.isOverride(), spo.getUserFlag(), spo.getStatementType());
        
    }
    
    /**
     * Encodes the {@link StatementEnum} and the optional statement identifier.
     */
    @Override
    public byte[] serializeVal(final SPO spo) {

        if (spo == null)
            throw new IllegalArgumentException();

        return serializeVal(//buf,
                spo.isOverride(), spo.getUserFlag(), spo.getStatementType());

    }

    /**
     * Return the byte[] that would be written into a statement index for this
     * {@link SPO}, including the optional {@link StatementEnum#MASK_OVERRIDE}
     * bit. If the statement identifier is non-null then it will be included in
     * the returned byte[].
     * 
     * @param override
     *            <code>true</code> iff you want the
     *            {@link StatementEnum#MASK_OVERRIDE} bit set (this is only set
     *            when serializing values for a remote procedure that will write
     *            on the index, it is never set in the index itself).
     * @param userFlag
     *            <code>true</code> iff you want the
     *            {@link StatementEnum#MASK_USER_FLAG} bit set.
     * @param type
     *            The {@link StatementEnum}.
     * 
     * @return The value that would be written into a statement index for this
     *         {@link SPO}.
     */
//    * @param buf
//    *            A buffer supplied by the caller. The buffer will be reset
//    *            before the value is written on the buffer.
    public byte[] serializeVal(//final ByteArrayBuffer buf,
            final boolean override, final boolean userFlag,
            final StatementEnum type) {
        
//      buf.reset();

        // optionally set the override and user flag bits on the value.
        final byte b = (byte) 
            (type.code()
                | (override ? StatementEnum.MASK_OVERRIDE : 0x0) 
                | (userFlag ? StatementEnum.MASK_USER_FLAG : 0x0)
                );

//      buf.putByte(b);
//
//      final byte[] a = buf.toByteArray();
//
//        assert a.length == 1 : "Expecting one byte, but have "
//                + BytesUtil.toString(a);
        
        return RDFValueFactory.getValue(b);

    }
    


    public SPO deserialize(final ITuple tuple) {

        if (tuple == null)
            throw new IllegalArgumentException();
        
        // copy of the key in a reused buffer.
        final byte[] key = tuple.getKeyBuffer().array();

        final SPO spo = keyOrder.decodeKey(key);

        if ((tuple.flags() & IRangeQuery.VALS) == 0) {

            // Note: No type or statement identifier information.
            return spo;
            
        }

        // Decode the StatementEnum, bit flags, and attach a sid.
        final ByteArrayBuffer vbuf = tuple.getValueBuffer();

        decodeValue(spo, vbuf.array());
        
        return spo;
        
    }

    public SPO deserializeKey(final ITuple tuple) {
        
        // just de-serialize the whole tuple.
        return deserialize(tuple);
        
    }

    /**
     * Set the statement type, bit flags, and optional sid based on the tuple
     * value.
     */
    public ISPO decodeValue(final ISPO spo, final byte[] val) {
        
        final byte code = val[0];

        final StatementEnum type = StatementEnum.decode(code);

        spo.setStatementType(type);
        
        spo.setOverride(StatementEnum.isOverride(code));

        spo.setUserFlag(StatementEnum.isUserFlag(code));

//        if (sids) {
//            
//            // SIDs only valid for triples.
//            assert keyOrder.getKeyArity() == 3;
//          
//            if (spo.isExplicit()) {
//                
//                spo.setStatementIdentifier(true);
//            
//            }
//            
//        }
        
        return spo;

    }

    /**
     * The initial version.
     */
    private final static transient byte VERSION0 = 0;

    /**
     * The new version for the inline sids refactor.
     */
    private final static transient byte VERSION1 = 1;

    /**
     * The current version.
     */
    private final static transient byte VERSION = VERSION1;

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        super.readExternal(in);
        
        final byte version = in.readByte();
        
        switch (version) {
        case VERSION0:
            keyOrder = SPOKeyOrder.valueOf(in.readByte());
            /*
             * New version is not backwards compatible with old journals that
             * used sids.
             */
            sids = false;
            break;
        case VERSION1:
            keyOrder = SPOKeyOrder.valueOf(in.readByte());
            sids = in.readByte() > 0;
            break;
        default:
            throw new UnsupportedOperationException("Unknown version: "
                    + version);
        }


    }

    public void writeExternal(ObjectOutput out) throws IOException {

        super.writeExternal(out);

        out.writeByte(VERSION);

        out.writeByte(keyOrder.index());
        
        out.writeByte(sids ? 1 : 0);

    }

}

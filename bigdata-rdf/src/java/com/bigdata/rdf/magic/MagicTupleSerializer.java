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

package com.bigdata.rdf.magic;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.TermId;

public class MagicTupleSerializer extends DefaultTupleSerializer<MagicTuple,MagicTuple> {

    /**
     * The natural order for the index.
     */
    private MagicKeyOrder keyOrder;
    
    /**
     * De-serialization constructor.
     */
    public MagicTupleSerializer() {
        
    }

    /**
     * Create an {@link ITupleSerializer} for the indicated access path.
     * 
     * @param keyOrder
     *            The access path.
     */
    public MagicTupleSerializer(MagicKeyOrder keyOrder) {

        super(new ASCIIKeyBuilderFactory(
                keyOrder.getKeyArity() * Bytes.SIZEOF_LONG), 
                getDefaultLeafKeysCoder(),
                getDefaultValuesCoder());
        
        if (keyOrder == null)
            throw new IllegalArgumentException();
        
        this.keyOrder = keyOrder;
        
    }
    
    public MagicTuple deserialize(final ITuple tuple) {

        if (tuple == null)
            throw new IllegalArgumentException();

        // copy of the key in a reused buffer.
        final byte[] key = tuple.getKeyBuffer().array(); 

        final int arity = keyOrder.getKeyArity();
        
        final int[] keyMap = keyOrder.getKeyMap();
        
        /*
         * Note: GTE since the key is typically a reused buffer which may be
         * larger than the #of bytes actually holding valid data.
         */
        assert key.length >= 8 * arity;

        /*
         * Decode the key.
         */
        
        final IV[] terms = new IV[arity];
        
        for (int i = 0; i < arity; i++) {
            
            terms[keyMap[i]] = new TermId(KeyBuilder.decodeLong(key, 8*i));
            
        }
        
        // Note: No type or statement identifier information.
        final MagicTuple magicTuple = new MagicTuple(terms);
        
        return magicTuple;
        
    }

    public MagicTuple deserializeKey(final ITuple tuple) {
        
        // just de-serialize the whole tuple.
        return deserialize(tuple);
        
    }

    public byte[] serializeKey(final Object obj) {

        if (obj == null)
            throw new IllegalArgumentException();
        
        if (obj instanceof MagicTuple)
            return serializeKey((MagicTuple) obj);

        //@todo could allow long[].
        throw new UnsupportedOperationException();
        
    }

    public byte[] serializeKey(final MagicTuple magicTuple) {
        
        return magicTuple2Key(keyOrder, magicTuple);
        
    }

    /**
     * Forms the magic tuple key.
     * 
     * @param keyOrder
     *            The key order.
     * @param magicTuple
     *            The magic tuple.
     * 
     * @return The key.
     */
    public byte[] magicTuple2Key(final MagicKeyOrder keyOrder, 
            final IMagicTuple magicTuple) {
        
        final int arity = keyOrder.getKeyArity();
        
        final int[] keyMap = keyOrder.getKeyMap();
        
        final IV[] terms = new IV[arity];
        
        for (int i = 0; i < arity; i++) {
            
            terms[i] = magicTuple.getTerm(keyMap[i]); 
                  
        }
        
        return magicTuple2Key(terms);
            
    }
    
    /**
     * Encodes a magic tuple represented as a set of long integers as an 
     * unsigned byte[] sort key.
     * <p>
     * 
     * @param terms
     *            RDF value identifiers from the term index.
     * 
     * @return The sort key for the magic tuple with those values.
     */
    public byte[] magicTuple2Key(final IV[] terms) {

        IKeyBuilder keyBuilder = getKeyBuilder().reset();
        
        for (IV term : terms) {
            
            /*
             * This is the implementation for backwards
             * compatibility.  We should not see inline values here.
             */
            if (term.isInline()) {
                throw new RuntimeException();
            }
            
            keyBuilder.append(term.getTermId());
            
        }
                
        return keyBuilder.getKey();

    }

    public byte[] serializeVal(MagicTuple spo) {

        return null; // new byte[0];
        
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        keyOrder = (MagicKeyOrder)in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(keyOrder);
    }
    
}

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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.btree.ASCIIKeyBuilderFactory;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.IKeyBuilderFactory;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.ThreadLocalKeyBuilderFactory;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.inf.Justification;

/**
 * (De-)serializes {@link Justification}s.
 * <p>
 * Note: the encoded key for a {@link Justification} is formed from the 64-bit
 * <code>long</code> term identifier for the head of the rule (the entailment)
 * followed by the term identifier bindings for the tail(s) of the rule. The
 * bindings are represented as a long[] and indexing into the bindings is by
 * position. Bindings in the tail of a rule MAY be ZERO (0L) in which case they
 * are interpreted as wildcards.
 * <p>
 * Note: No values are stored for this index - all the information is in the
 * keys.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JustificationTupleSerializer implements
        ITupleSerializer<Justification, Justification>, Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = -3930463865005938874L;

    private int N;

    /**
     * Used to format the key.
     * <p>
     * Note: While the static methods used to decode an existing key are safe
     * for concurrent readers, concurrent readers also form keys using
     * {@link #statement2Key(long, long, long)} and therefore require a
     * thread-local {@link IKeyBuilder}.
     */
    private transient IKeyBuilderFactory keyBuilderFactory;

    public IKeyBuilder getKeyBuilder() {
      
        return keyBuilderFactory.getKeyBuilder();
        
    };
    
    /**
     * De-serialization constructor.
     */
    public JustificationTupleSerializer() {

    }

    /**
     * 
     * @param N The #of slots in a statement (3 or 4).
     */
    public JustificationTupleSerializer(int N) {
        
        if (N != 3 && N != 4)
            throw new IllegalArgumentException();
        
        this.N = N;

        this.keyBuilderFactory = new ThreadLocalKeyBuilderFactory(
                new ASCIIKeyBuilderFactory(N * Bytes.SIZEOF_LONG));

    }

    public Justification deserialize(ITuple tuple) {

        if (tuple == null)
            throw new IllegalArgumentException();

        final ByteArrayBuffer kbuf = tuple.getKeyBuffer();
        
        final int keyLen = kbuf.limit();
        
        final byte[] data = kbuf.array();
        
        // verify key is even multiple of (N*sizeof(long)).
        assert keyLen % (N * Bytes.SIZEOF_LONG) == 0;

        // #of term identifiers in the key.
        final int m = keyLen / Bytes.SIZEOF_LONG;

        // A justification must include at least a head and one tuple in the tail.
        assert m >= N * 2 : "keyLen="+keyLen+", N="+N+", m="+m;
        
        final long[] ids = new long[m];
        
        for (int i = 0; i < m; i++) {

            ids[i] = KeyBuilder.decodeLong(data, i * Bytes.SIZEOF_LONG);
            
        }

        return new Justification(ids);
        
    }

    public Justification deserializeKey(ITuple tuple) {
        
        // just de-serialize the whole tuple.
        return deserialize(tuple);
        
    }

    public byte[] serializeKey(Object obj) {

        if (obj == null)
            throw new IllegalArgumentException();
        
        if (obj instanceof Justification)
            return Justification.getKey(getKeyBuilder(), (Justification) obj);

        //@todo could allow long[].
        throw new UnsupportedOperationException();
        
    }

    /**
     * There is no value for the justifications index. All data is in the key.
     * 
     * @throws UnsupportedOperationException
     *             always
     */
    public byte[] serializeVal(Justification jst) {

        throw new UnsupportedOperationException();
        
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        N = in.readByte();

        this.keyBuilderFactory = new ThreadLocalKeyBuilderFactory(
                new ASCIIKeyBuilderFactory(N * Bytes.SIZEOF_LONG));

    }

    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeByte(N);

    }

}

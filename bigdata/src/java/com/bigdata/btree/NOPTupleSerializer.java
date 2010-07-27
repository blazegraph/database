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
 * Created on May 28, 2008
 */

package com.bigdata.btree;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;

import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.DefaultKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;

/**
 * Default implementation uses the {@link KeyBuilder} to format the object as a
 * key and requires that the values are byte[]s which it passes on without
 * change. Deserialization of the tuple value always the byte[] itself.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NOPTupleSerializer extends DefaultTupleSerializer {

    /**
     * 
     */
    private static final long serialVersionUID = 2211020411074955099L;

    public static transient final ITupleSerializer INSTANCE = new NOPTupleSerializer(
            new DefaultKeyBuilderFactory(new Properties()));

    /**
     * De-serialization ctor.
     */
    public NOPTupleSerializer() {
        
    }

    /**
     * Normally callers will use an {@link ASCIIKeyBuilderFactory} since
     * Unicode support is not required 
     * @param keyBuilderFactory
     * 
     * @see ASCIIKeyBuilderFactory
     */
    public NOPTupleSerializer(IKeyBuilderFactory keyBuilderFactory) {

        super(keyBuilderFactory);
        
    }

    /**
     * @param obj
     *            The key.
     *            
     * @return <i>obj</i> iff it is a <code>byte[]</code> and otherwise
     *         converts <i>obj</i> to a byte[] using
     *         {@link IKeyBuilder#append(Object)}.
     */
    public byte[] serializeKey(Object obj) {

        if (obj == null)
            throw new IllegalArgumentException();
        
        if(obj instanceof byte[]) {
            
            return (byte[]) obj;
            
        }
        
        return getKeyBuilder().reset().append(obj).getKey();
        
    }

    public byte[] serializeVal(Object obj) {

        return (byte[])obj;
        
    }

    public Object deserialize(ITuple tuple) {

        if (tuple == null)
            throw new IllegalArgumentException();
        
        return tuple.getValue();
        
    }

    /**
     * This is an unsupported operation. Additional information is required to
     * either decode the internal unsigned byte[] keys or to extract the key
     * from the de-serialized value (if it is being stored in that value). You
     * can either write your own {@link ITupleSerializer} or you can specialize
     * this one so that it can de-serialize your keys using whichever approach
     * makes the most sense for your data.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    public Object deserializeKey(ITuple tuple) {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * The initial version (no additional persistent state).
     */
    private final static transient byte VERSION0 = 0;

    /**
     * The current version.
     */
    private final static transient byte VERSION = VERSION0;

    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        super.readExternal(in);
        
        final byte version = in.readByte();
        
        switch (version) {
        case VERSION0:
            break;
        default:
            throw new UnsupportedOperationException("Unknown version: "
                    + version);
        }

    }

    public void writeExternal(final ObjectOutput out) throws IOException {

        super.writeExternal(out);
        
        out.writeByte(VERSION);
        
    }

}

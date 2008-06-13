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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Default implementation uses the {@link KeyBuilder} to format the object as a
 * key and requires that the values are byte[]s which it passes on without
 * change. Deserialization of the tuple value always the byte[] itself.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NOPTupleSerializer implements ITupleSerializer, Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = 2211020411074955099L;

    public static transient final ITupleSerializer INSTANCE = new NOPTupleSerializer();
    
    public NOPTupleSerializer() {
        
    }

    /**
     * Serialization using {@link KeyBuilder#asSortKey(Object)}
     */
    public byte[] serializeKey(Object obj) {

        if (obj == null)
            throw new IllegalArgumentException();
        
        // @todo thread-local keybuilder instead?
        return KeyBuilder.asSortKey(obj);
        
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

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        // NOP
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        // NOP
        
    }

}

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
 * Created on Sep 7, 2009
 */

package com.bigdata.btree.raba.codec;

import com.bigdata.btree.Leaf;
import com.bigdata.btree.Node;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.MutableKeyBuffer;
import com.bigdata.btree.raba.MutableValueBuffer;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;

/**
 * This "codes" a raba as a {@link MutableKeyBuffer} or
 * {@link MutableValueBuffer} depending on whether it represents B+Tree keys or
 * values. This class is used by some unit tests as a convenience for
 * establishing a baseline for the performance of {@link ICodedRaba}s against
 * the core mutable {@link IRaba} implementations actually used by {@link Node}
 * and {@link Leaf}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MutableRabaCoder implements IRabaCoder {

    /**
     * 
     */
    private static final long serialVersionUID = -7123556255775810548L;

    public ICodedRaba decode(AbstractFixedByteArrayBuffer data) {
        
        // Note: an alternative class is used to encode/decode.
        final IRaba raba = SimpleRabaCoder.INSTANCE.decode(data);
        
        if (raba.isKeys()) {
        
            return new KeysRabaImpl(raba, data);
            
        } else {
            
            return new ValuesRabaImpl(raba, data);
            
        }
        
    }

    public AbstractFixedByteArrayBuffer encode(IRaba raba, DataOutputBuffer buf) {

        return encodeLive(raba, buf).data();
        
    }

    public ICodedRaba encodeLive(IRaba raba, DataOutputBuffer buf) {

        // Note: an alternative class is used to encode/decode.
        final AbstractFixedByteArrayBuffer data = SimpleRabaCoder.INSTANCE
                .encode(raba, buf);
        
        if (raba.isKeys()) {
        
            return new KeysRabaImpl(raba, data);
            
        } else {
            
            return new ValuesRabaImpl(raba, data);
            
        }
        
    }

    /**
     * Yes.
     */
    final public boolean isKeyCoder() {
        
        return true;
        
    }

    /**
     * Yes.
     */
    final public boolean isValueCoder() {
        
        return true;
        
    }

    /**
     * {@link MutableKeyBuffer} with mock implementation of {@link ICodedRaba}
     * methods.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class KeysRabaImpl extends MutableKeyBuffer implements ICodedRaba {

        private final AbstractFixedByteArrayBuffer data;

        public KeysRabaImpl(IRaba raba, AbstractFixedByteArrayBuffer data) {

            super(raba.capacity(), raba);

            this.data = data;
            
        }

        public AbstractFixedByteArrayBuffer data() {
            
            return data;
            
        }
        
    }
    
    /**
     * {@link MutableValueBuffer} with mock implementation of {@link ICodedRaba}
     * methods.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class ValuesRabaImpl extends MutableValueBuffer implements ICodedRaba {

        private final AbstractFixedByteArrayBuffer data;
        
        public ValuesRabaImpl(IRaba raba,AbstractFixedByteArrayBuffer data) {
            
            super(raba.capacity(), raba);
        
            this.data = data;
            
        }

        public AbstractFixedByteArrayBuffer data() {
            
            return data;
            
        }
        
    }
    
}

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
 * Created on Feb 1, 2008
 */

package com.bigdata.btree;

import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.rawstore.IBlock;

/**
 * An {@link ITuple} wrapping a delegate that may be used to override some of
 * the methods on the delegate object.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DelegateTuple<E> implements ITuple<E> {
    
    protected final ITuple<E> delegate;
    
    public DelegateTuple(ITuple<E> delegate) {
        
        this.delegate = delegate;
        
    }

    public int getSourceIndex() {
        return delegate.getSourceIndex();
    }
    
    public int flags() {
        return delegate.flags();
    }
   
    public byte[] getKey() {
        return delegate.getKey();
    }

    public ByteArrayBuffer getKeyBuffer() {
        return delegate.getKeyBuffer();
    }

    public boolean getKeysRequested() {
        return delegate.getKeysRequested();
    }

    public DataInputBuffer getKeyStream() {
        return delegate.getKeyStream();
    }

    public byte[] getValue() {
        return delegate.getValue();
    }

    public boolean isNull() {
        return delegate.isNull();
    }
    
    public ByteArrayBuffer getValueBuffer() {
        return delegate.getValueBuffer();
    }

    public boolean getValuesRequested() {
        return delegate.getValuesRequested();
    }

    public DataInputBuffer getValueStream() {
        return delegate.getValueStream();
    }

    public long getVersionTimestamp() {
        return delegate.getVersionTimestamp();
    }

    public long getVisitCount() {
        return delegate.getVisitCount();
    }

    public boolean isDeletedVersion() {
        return delegate.isDeletedVersion();
    }

    public String toString() {
        return delegate.toString();
    }

    public IBlock readBlock(long addr) {
        return delegate.readBlock(addr);
    }
    
    public E getObject() {
        return delegate.getObject();
    }

    public ITupleSerializer getTupleSerializer() {
        return delegate.getTupleSerializer();
    }

}

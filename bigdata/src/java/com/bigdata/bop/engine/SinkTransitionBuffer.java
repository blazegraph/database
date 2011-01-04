/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Jan 1st, 2011
 */
package com.bigdata.bop.engine;

import java.util.concurrent.Future;

import com.bigdata.bop.IBindingSet;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * Delegation pattern handles the {@link SinkTransitionMetadata}.
 * 
 * @deprecated along with {@link SinkTransitionMetadata}
 */
class SinkTransitionBuffer implements
        IBlockingBuffer<IBindingSet[]> {

    private final IBlockingBuffer<IBindingSet[]> b;

    private final SinkTransitionMetadata stm;

    /**
     * 
     */
    public SinkTransitionBuffer(final IBlockingBuffer<IBindingSet[]> b,
            final SinkTransitionMetadata stm) {

        this.b = b;

        this.stm = stm;

    }
    
    public IAsynchronousIterator<IBindingSet[]> iterator() {
        return b.iterator();
    }

    public void setFuture(final Future future) {
        b.setFuture(future);
    }

    public void abort(final Throwable cause) {
        b.abort(cause);
    }

    public void close() {
        b.close();
    }

    public Future getFuture() {
        return b.getFuture();
    }

    public boolean isOpen() {
        return b.isOpen();
    }

    public long flush() {
        return b.flush();
    }

    public void add(final IBindingSet[] e) {
        for (IBindingSet bset : e) {
            stm.handleBindingSet(bset);
        }
        b.add(e);
    }

    public boolean isEmpty() {
        return b.isEmpty();
    }

    public void reset() {
        b.reset();
    }

    public int size() {
        return b.size();
    }
    
}
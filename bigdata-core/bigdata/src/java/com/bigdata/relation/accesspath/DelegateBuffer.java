/**

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
 * Created on Aug 11, 2011
 */

package com.bigdata.relation.accesspath;

import java.util.concurrent.Future;

/**
 * A delegation pattern which does not pass on the {@link #close()} method.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DelegateBuffer<E> implements IBlockingBuffer<E> {

    private final IBlockingBuffer<E> delegate; 

    public DelegateBuffer(final IBlockingBuffer<E> delegate) {

        if (delegate == null)
            throw new IllegalArgumentException();
        
        this.delegate = delegate;
        
    }

    public void add(E e) {
        delegate.add(e);
    }

    public int size() {
        return delegate.size();
    }

    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    public boolean isOpen() {
        return delegate.isOpen();
    }

    public void close() {
        delegate.close();
    }

    public void abort(Throwable cause) {
        delegate.abort(cause);
    }

    public void reset() {
        delegate.reset();
    }

    public Future getFuture() {
        return delegate.getFuture();
    }

    public IAsynchronousIterator<E> iterator() {
        return delegate.iterator();
    }

    public long flush() {
        return delegate.flush();
    }

    public void setFuture(Future future) {
        delegate.setFuture(future);
    }

}

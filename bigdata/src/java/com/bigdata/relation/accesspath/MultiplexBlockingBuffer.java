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
 * Created on Oct 7, 2010
 */

package com.bigdata.relation.accesspath;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;


/**
 * A factory for skins which may be used to multiplex writes against a
 * {@link BlockingBuffer}. Each skin writes through to the backing
 * {@link BlockingBuffer} but may be closed independently of the backing
 * {@link BlockingBuffer}. This allows multiple produces to share a single
 * {@link BlockingBuffer} as long as they use a subset of the
 * {@link IBlockingBuffer} API (they can not set the {@link Future} on the
 * objects returned by this factory or obtain its
 * {@link IBlockingBuffer#iterator()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Does this need to close automatically when the last open inner buffer
 *       is closed or should it be closed explicitly and close all inner buffers
 *       when it is closed?
 */
public class MultiplexBlockingBuffer<E> {

    /** The delegate. */
    private final IBlockingBuffer<E> b;

    /** Lock guarding internal state. */
    private final ReentrantLock lock = new ReentrantLock();

    /** The set of opened buffered which have not yet been closed. */
    private final LinkedHashSet<IBlockingBuffer<E>> set = new LinkedHashSet<IBlockingBuffer<E>>();

    /** The #of currently open buffers. */
    private int counter = 0;
    
    public MultiplexBlockingBuffer(final IBlockingBuffer<E> b) {
        if (b == null)
            throw new IllegalArgumentException();
        this.b = b;
    }

    public boolean isOpen() {
        return b.isOpen();
    }
    
    public IBlockingBuffer<E> newInstance() {
        lock.lock();
        try {
            if(!isOpen())// ???
                throw new BufferClosedException();
            final IBlockingBuffer<E> n = new InnerBlockingBuffer();
            if (!set.add(n))
                throw new AssertionError();
            counter++;
            return n;
        } finally {
            lock.unlock();
        }
    }
    
    public void flushAndCloseAll() {
        lock.lock();
        try {
            final Iterator<IBlockingBuffer<E>> itr = set.iterator();
            while(itr.hasNext()) {
                final IBlockingBuffer<E> n = itr.next();
                n.close();
            }
            assert counter == 0;
            b.flush();
            b.close();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The {@link IBlockingBuffer} reference provided to the constructor.
     */
    public IBlockingBuffer<E> getBackingBuffer() {
        return b;
    }
    
    /**
     * Inner "skin" writes through to the backing buffer shared by all skins.
     * <p>
     * Note: This inner class does not support several of the
     * {@link IBlockingBuffer} methods whose semantics are likely to cause
     * problems when interpreted in the light of a skin over a shared buffer.
     * The only way these methods could be given clear semantics is if the skin
     * were actually a full {@link BlockingBuffer} which was coupled to the
     * shared buffer. However, that involves double buffering and double copying
     * and I do not think that this is worth it.
     */
    private class InnerBlockingBuffer implements IBlockingBuffer<E> {

        public InnerBlockingBuffer() {
        }

        private boolean innerBufferOpen = true;

        public IAsynchronousIterator<E> iterator() {
            throw new UnsupportedOperationException();
        }

        public void setFuture(Future future) {
            throw new UnsupportedOperationException();
        }

        public void abort(final Throwable cause) {
            lock.lock();
            try {
                if (!innerBufferOpen)
                    throw new BufferClosedException();
                b.abort(cause);
            } finally {
                lock.unlock();
            }
        }

        public void close() {
            lock.lock();
            try {
                if (!innerBufferOpen)
                    return;
                innerBufferOpen = false;
                if (!set.remove(this))
                    throw new AssertionError();
                counter--;
                if (counter == 0) {
                    /*
                     * Note: We flush the backing buffer before we close it in
                     * case it has anything buffered. This covers the normal,
                     * which is where the caller has already invoked flush() on
                     * this skin and should not create any harm otherwise.
                     */
                     b.flush();
                     b.close();
                }
            } finally {
                lock.unlock();
            }
        }

        public Future getFuture() {
            return b.getFuture();
        }

        public boolean isOpen() {
            return innerBufferOpen && b.isOpen();
        }

        public long flush() {
            /*
             * Nothing to flush. The target is flushed when the outer class is
             * closed.
             */
            return 0;
        }

        public void add(E e) {
            if (!innerBufferOpen)
                throw new BufferClosedException();
            b.add(e);
        }

        public boolean isEmpty() {
            return b.isEmpty();
        }

        public void reset() {
            throw new UnsupportedOperationException();
        }

        public int size() {
            return b.size();
        }

    }

}

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
 * Created on May 15, 2009
 */

package com.bigdata.cache;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link BlockingQueue} based on the {@link RingBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated This implementation is incomplete. Realizing this implementation
 *             would require adding support for signals when the buffer becomes
 *             "nonEmpty" and when it becomes "notFull". However, the
 *             {@link ArrayBlockingQueue} already provides a well-testing ring
 *             buffer which implements the {@link BlockingQueue} API. The only
 *             reason to pursue this implementation further would be to gain
 *             more control over the backing array. For example, so as to be
 *             able to combine chunks together when the generic type of the
 *             array is itself an array type.
 */
public class BlockingRingBuffer<E> implements BlockingQueue<E> {

    private final RingBuffer<E> buffer;
    
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * @param capacity
     */
    public BlockingRingBuffer(final int capacity) {

        buffer = new RingBuffer<E>(capacity);

    }

    public boolean add(E e) {
        lock.lock();
        try {
            return buffer.add(e);
        } finally {
            lock.unlock();
        }
    }

    public final int capacity() {
        return buffer.capacity();
    }

    public void clear() {
        lock.lock();
        try {
            buffer.clear();
        } finally {
            lock.unlock();
        }
    }

    public boolean contains(Object ref) {
        lock.lock();
        try {
            return buffer.contains(ref);
        } finally {
            lock.unlock();
        }
    }

    public boolean containsAll(Collection<?> c) {
        lock.lock();
        try {
            return buffer.containsAll(c);
        } finally {
            lock.unlock();
        }
    }

    public E element() throws NoSuchElementException {
        lock.lock();
        try {
            return buffer.element();
        } finally {
            lock.unlock();
        }
    }

    public final E get(int index) {
        lock.lock();
        try {
            return buffer.get(index);
        } finally {
            lock.unlock();
        }
    }

    public final boolean isEmpty() {
        lock.lock();
        try {
            return buffer.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    public final boolean isFull() {
        lock.lock();
        try {
            return buffer.isFull();
        } finally {
            lock.unlock();
        }
    }

    public boolean offer(E ref) {
        lock.lock();
        try {
            return buffer.offer(ref);
        } finally {
            lock.unlock();
        }
    }

    public final E peek() {
        lock.lock();
        try {
            return buffer.peek();
        } finally {
            lock.unlock();
        }
    }

    public E poll() {
        lock.lock();
        try {
            return buffer.poll();
        } finally {
            lock.unlock();
        }
    }

    public E remove() throws NoSuchElementException {
        lock.lock();
        try {
            return buffer.remove();
        } finally {
            lock.unlock();
        }
    }

    public boolean remove(Object ref) {
        lock.lock();
        try {
            return buffer.remove(ref);
        } finally {
            lock.unlock();
        }
    }

    public final int size() {
        lock.lock();
        try {
            return buffer.size();
        } finally {
            lock.unlock();
        }
    }

    public Object[] toArray() {
        lock.lock();
        try {
            return buffer.toArray();
        } finally {
            lock.unlock();
        }
    }

    public <TX> TX[] toArray(TX[] a) {
        lock.lock();
        try {
            return buffer.toArray(a);
        } finally {
            lock.unlock();
        }
    }

    /**
     * The lock is not held across the entire operation to avoid the possibility
     * of deadlock if the queue becomes full.
     */
    public boolean addAll(Collection<? extends E> c) {

        boolean modified = false;
        
        for (E e : c) {

            add(e);

            modified = true;
            
        }

        return modified;
        
    }

    public boolean removeAll(Collection<?> c) {
        lock.lock();
        try {
            return buffer.removeAll(c);
        } finally {
            lock.unlock();
        }
    }

    public boolean retainAll(Collection<?> c) {
        lock.lock();
        try {
            return buffer.retainAll(c);
        } finally {
            lock.unlock();
        }
    }

    public int drainTo(Collection<? super E> c) {
        // TODO Auto-generated method stub
        return 0;
    }

    public int drainTo(Collection<? super E> c, int maxElements) {
        // TODO Auto-generated method stub
        return 0;
    }

    public boolean offer(E e, long timeout, TimeUnit unit)
            throws InterruptedException {
        // TODO Auto-generated method stub
        return false;
    }

    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    // @todo must gain lock and then await signal if buffer is full 
    public void put(E e) throws InterruptedException {
        // TODO Auto-generated method stub

    }

    // @todo must gain lock and then await signal if buffer is empty 
    public E take() throws InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    public int remainingCapacity() {
        lock.lock();
        try {
            return buffer.capacity() - buffer.size();
        } finally {
            lock.unlock();
        }
    }

    //
    public Iterator<E> iterator() {
        // TODO Auto-generated method stub
        return null;
    }

}

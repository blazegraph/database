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
 * Created on Nov 17, 2008
 */

package com.bigdata.service.proxy;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.Remote;
import java.util.concurrent.TimeUnit;

import com.bigdata.io.IStreamSerializer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;

/**
 * A helper object that provides the API of {@link IAsynchronousIterator} but
 * whose methods throw {@link IOException} and are therefore compatible with
 * {@link Remote} and {@link Exporter}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 */
public class RemoteAsynchronousIteratorImpl<E> implements
        RemoteAsynchronousIterator<E> {

    private final IAsynchronousIterator<E> itr;

    private final IStreamSerializer<E> serializer;

    /**
     * Ctor variant does not support {@link #nextElement()}.
     * 
     * @param itr
     *            The source iterator (local object).
     */
    public RemoteAsynchronousIteratorImpl(final IAsynchronousIterator<E> itr) {

        this(itr, null/* serializer */);
        
    }
    
    /**
     * Ctor variant optionally supports {@link #nextElement()}.
     * 
     * @param itr
     *            The source iterator (local object).
     * @param serializer
     *            Optional, but {@link #nextElement()} only works when you
     *            specify an {@link IStreamSerializer}.
     */
    public RemoteAsynchronousIteratorImpl(
            final IAsynchronousIterator<E> itr,
            final IStreamSerializer<E> serializer
            ) {

        if (itr == null)
            throw new IllegalArgumentException();

//        if (serializer == null)
//            throw new IllegalArgumentException();

        this.itr = itr;

        this.serializer = serializer;
        
    }

    public void close() throws IOException {

        itr.close();
        
    }

    public boolean hasNext() throws IOException {
        
        return itr.hasNext();
        
    }

    public boolean hasNext(long timeout, TimeUnit unit) throws IOException,
            InterruptedException {
        
        return itr.hasNext(timeout, unit);
        
    }

    public boolean isExhausted() throws IOException {

        return itr.isExhausted();
        
    }

    public E next() throws IOException {

        return itr.next();
        
    }

    public E next(long timeout, TimeUnit unit) throws IOException,
            InterruptedException {

        return itr.next(timeout, unit);
        
    }

    public void remove() throws IOException {
        
        itr.remove();
        
    }
    
    /**
     * @throws UnsupportedOperationException
     *             if you did not specify an {@link IStreamSerializer} to the ctor.
     */
    public RemoteElement<E> nextElement() {

        if (serializer == null)
            throw new UnsupportedOperationException();
        
        final E e = itr.next();

        return new RemoteElementImpl<E>(e, serializer);
        
    }
    
    /**
     * {@link RemoteElement} impl.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    private static class RemoteElementImpl<E> implements RemoteElement<E>,
            Externalizable {

        /**
         * 
         */
        private static final long serialVersionUID = -167351084165715123L;

        private transient E e;
        private transient IStreamSerializer<E> ser;
        
        /**
         * De-serialization ctor.
         */
        public RemoteElementImpl() {
            
        }
        
        public RemoteElementImpl(final E e, final IStreamSerializer<E> ser) {

            if (e == null)
                throw new IllegalArgumentException();

            if (ser == null)
                throw new IllegalArgumentException();
            
            this.e = e;
            
            this.ser = ser;
            
        }

        public E get() {

            return e;
            
        }

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
            
            ser = (IStreamSerializer<E>) in.readObject();

            e = ser.deserialize(in);
            
        }

        public void writeExternal(ObjectOutput out) throws IOException {

            out.writeObject(ser);
            
            ser.serialize(out, e);
            
        }

    }

}

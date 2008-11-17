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

import java.io.IOException;
import java.rmi.Remote;
import java.util.concurrent.TimeUnit;

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

//    private final ISerializer<E> serializer;

    public RemoteAsynchronousIteratorImpl(
            final IAsynchronousIterator<E> itr
//            final ISerializer<E> serializer
            ) {

        if (itr == null)
            throw new IllegalArgumentException();

//        if (serializer == null)
//            throw new IllegalArgumentException();

        this.itr = itr;

//        this.serializer = serializer;
        
    }

    public void close() throws IOException {

        itr.close();
        
    }

    public boolean hasNext() throws IOException {
        
        return itr.hasNext();
        
    }

    public boolean hasNext(long timeout, TimeUnit unit) throws IOException {
        
        return itr.hasNext(timeout, unit);
        
    }

    public boolean isExhausted() throws IOException {

        return itr.isExhausted();
        
    }

    public E next() throws IOException {

        return itr.next();
        
    }

    public E next(long timeout, TimeUnit unit) throws IOException {
    
        return itr.next(timeout, unit);
        
    }

    public void remove() throws IOException {
        
        itr.remove();
        
    }
    
}

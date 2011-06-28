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
 * Created on Jun 5, 2009
 */

package com.bigdata.service.proxy;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.Future;

import com.bigdata.relation.accesspath.IRunnableBuffer;

/**
 * {@link Serializable} class wraps a {@link RemoteRunnableBuffer} delegating
 * methods through to the {@link IRunnableBuffer} on the remote service while
 * masquerading {@link IOException}s so that we can implement the
 * {@link IRunnableBuffer} API.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ClientRunnableBuffer<E, V> extends ClientBuffer<E> implements
        IRunnableBuffer<E/*, V*/> {

    private final RemoteRunnableBuffer<E, V> buffer;
    
    /**
     * @param buffer
     */
    public ClientRunnableBuffer(final RemoteRunnableBuffer<E, V> buffer) {

        super(buffer);

        this.buffer = buffer;
        
    }

    public boolean isOpen() {
        try {
            return buffer.isOpen();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void abort(Throwable cause) {
        try {
            buffer.abort(cause);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void close() {
        try {
            buffer.close();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Future<V> getFuture() {
        try {
            return buffer.getFuture();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

}

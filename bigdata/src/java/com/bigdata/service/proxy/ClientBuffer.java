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
 * Created on Nov 15, 2008
 */

package com.bigdata.service.proxy;

import java.io.IOException;
import java.io.Serializable;

import com.bigdata.relation.accesspath.IBuffer;

/**
 * {@link Serializable} class wraps a {@link RemoteBuffer} delegating methods
 * through to the {@link IBuffer} on the remote service while masquerading
 * {@link IOException}s so that we can implement the {@link IBuffer} API.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ClientBuffer<E> implements IBuffer<E>, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 3820783912360132819L;
    
    private final RemoteBuffer<E> buffer;

    /**
     * 
     * @param buffer
     *            A proxy for the {@link RemoteBuffer}.
     */
    public ClientBuffer(final RemoteBuffer<E> buffer) {

        if (buffer == null)
            throw new IllegalArgumentException();

        this.buffer = buffer;

    }
    
    public void add(E e) {
        try {
            buffer.add(e);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public long flush() {
        try {
            return buffer.flush();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public boolean isEmpty() {
        try {
            return buffer.isEmpty();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void reset() {
        try {
            buffer.reset();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public int size() {
        try {
            return buffer.size();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

}

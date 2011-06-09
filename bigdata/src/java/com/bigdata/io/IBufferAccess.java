/*

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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

package com.bigdata.io;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Interface for access to and release of a direct {@link ByteBuffer} managed by
 * the {@link DirectBufferPool}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IBufferAccess {

    /**
     * Return the direct {@link ByteBuffer}.
     * <p>
     * <strong>Caution:</strong> DO NOT hold onto a reference to the returned
     * {@link ByteBuffer} without also retaining the {@link IBufferAccess}
     * object. This can cause the backing {@link ByteBuffer} to be returned to
     * the pool, after which it may be handed off to another thread leading to
     * data corruption through concurrent modification to the backing bytes!
     * 
     * @throws IllegalStateException
     *             if the buffer has been released.
     */
    public ByteBuffer buffer();

    /**
     * Release the {@link ByteBuffer}, returning to owning pool.
     * 
     * @throws IllegalStateException
     *             if the buffer has been released.
     */
    public void release() throws InterruptedException;

    /**
     * Release the {@link ByteBuffer}, returning to owning pool.
     *
     * @param time
     * @param unit
     * @throws IllegalStateException
     *             if the buffer has been released.
     */
    public void release(long time, TimeUnit unit) throws InterruptedException;

}

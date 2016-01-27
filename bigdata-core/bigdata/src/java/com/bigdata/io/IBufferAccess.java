/*

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

package com.bigdata.io;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Interface for access to, and release of, a direct {@link ByteBuffer} managed
 * by the {@link DirectBufferPool}. A "direct" buffer is a region from the C
 * heap of the native JVM process. Such buffers can be efficient for NIO. They
 * are also efficient when the application needs to store large amounts of data
 * without putting pressure on the Garbage Collector.
 * <p>
 * <strong>CAUTION: Applications MUST use specific patterns when they work with
 * the {@link DirectBufferPool}</strong> Applications which acquire an
 * {@link IBufferAccess} object <em>MUST</em> hold a hard reference to that
 * {@link IBufferAccess} while they are using the associated memory block.
 * Failure to follow this pattern WILL result in the backing {@link ByteBuffer}
 * being returned to the {@link DirectBufferPool} while the application is still
 * using the {@link ByteBuffer} (GC will drive this). This situation will lead
 * to corruption for data stored within the {@link ByteBuffer} if the buffer
 * recycled and handed off once again via {@link DirectBufferPool#acquire()}.
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
     * Release the {@link ByteBuffer}, returning to owning pool. This method
     * will silently succeed if the buffer has already been released.
     * 
     * @throws InterruptedException
     *             if an interrupt is noticed while attempting to return the
     *             buffer to the pool.
     */
    public void release() throws InterruptedException;

    /**
     * Release the {@link ByteBuffer}, returning to owning pool. This method
     * will silently succeed if the buffer has already been released.
     * 
     * @param timeout
     *            The timeout.
     * @param unit
     *            The units for that timeout.
     *            
     * @throws InterruptedException
     *             if an interrupt is noticed while attempting to return the
     *             buffer to the pool.
     */
    public void release(long timeout, TimeUnit unit)
            throws InterruptedException;

}

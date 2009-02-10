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
 * Created on Feb 10, 2009
 */

package com.bigdata.io;

import java.io.IOException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/**
 * Interface for objects which know how to re-open the backing
 * {@link FileChannel} for some resource and also understand when the resource
 * has been closed and therefore should not be reopened. This is used in
 * combination with {@link FileChannelUtility} to support the transparent
 * re-opening of a file whose channel was closed asynchronously by an interrupt
 * in another thread during an NIO operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IReopenChannel {

    /**
     * Transparently re-opens the {@link FileChannel} for the backing file if it
     * is closed.
     * <p>
     * Note: Java will close the backing {@link FileChannel} if a {@link Thread}
     * is interrupted during an NIO operation.
     * <p>
     * Note: This method MUST NOT be invoked if the channel was closed by an
     * interrupt in the <strong>caller's</strong> thread. The caller can detect
     * this condition by a thrown {@link ClosedByInterruptException} rather than
     * either an {@link AsynchronousCloseException} or an
     * {@link ClosedChannelException}. See {@link FileChannelUtility} which
     * knows how to handle this.
     * <p>
     * Note: This method MUST synchronized so that concurrent operations do not
     * try to re-open the {@link FileChannel} at the same time.
     * <p>
     * Note: While the {@link FileChannel} may be open within the implementation
     * of this method, it IS NOT possible guarenteed that it will be open by the
     * time you try to use it except by synchronizing all activity on that
     * {@link FileChannel}. In general, that will limit throughput.
     * <p>
     * Note: Platforms and volumes (such as NFS) which DO NOT support
     * {@link FileLock} should re-open the file anyway without throwing an
     * exception. This behavior is required to run in those contexts.
     * 
     * @return The {@link FileChannel} and never <code>null</code>.
     * 
     * @throws IllegalStateException
     *             if the resource has been closed and is therefore no longer
     *             permitting reads or writes on the file.
     * 
     * @throws IOException
     *             if a required {@link FileLock} could not be obtained on a
     *             platform and volume where {@link FileLock} is supported.
     */
    public FileChannel reopenChannel() throws IOException;

    /**
     * Should include the name of the backing file (if known).
     */
    public String toString();
}

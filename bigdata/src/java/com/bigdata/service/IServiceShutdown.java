/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Mar 17, 2007
 */

package com.bigdata.service;

/**
 * Local API for service shutdown.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo declare on the various "Manager" interfaces, all of which use these
 *       method signatures.  Perhaps rename as "IShutdown".
 */
public interface IServiceShutdown {

    /**
     * Return <code>true</code> iff the service is running.
     */
    public boolean isOpen();
    
    /**
     * The service will no longer accept new requests, but existing requests
     * will be processed (sychronous). This method should await the termination
     * of pending requests, but no longer than the timeout specified by
     * {@link Options#SHUTDOWN_TIMEOUT}. Implementations SHOULD be
     * <strong>synchronized</strong>.  If the service is aleady shutdown, then
     * this method should be a NOP.
     */
    public void shutdown();
    
    /**
     * The service will no longer accept new requests and will make a best
     * effort attempt to terminate all existing requests and return ASAP. This
     * method should terminate any asynchronous processing, release all
     * resources and return immediately. Implementations SHOULD be
     * <strong>synchronized</strong>. If the service is aleady shutdown, then
     * this method should be a NOP.
     */
    public void shutdownNow();

    /**
     * Options for {@link IServiceShutdown} implementations.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options {

        /**
         * The maximum time in milliseconds that {@link #shutdown()} should wait
         * termination of the various services -or- ZERO (0) to wait forever
         * (default is to wait forever).
         * <p>
         * Note: since services will continue to execute tasks that are already
         * running but SHOULD NOT accept queued tasks once shutdown begins, this
         * primarily effects whether or not tasks that are already executing
         * will be allowed to run until completion.
         * <p>
         * Note: You can use {@link #shutdownNow()} to terminate the service
         * immediately.
         * 
         * @see #DEFAULT_SHUTDOWN_TIMEOUT
         */
        String SHUTDOWN_TIMEOUT = "shutdownTimeout";

        /**
         * The default timeout (0).
         */
        String DEFAULT_SHUTDOWN_TIMEOUT = "0";

    }

}

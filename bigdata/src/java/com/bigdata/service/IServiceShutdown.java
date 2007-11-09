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
 * @todo reconcile with Jini - does Jini provide for both polite and immediate
 *       shutdown or only a single kind (destroyService).
 */
public interface IServiceShutdown {

    /**
     * The service will no longer accept new requests, but existing requests
     * will be processed (sychronous).
     * 
     * @return Once the service has finished processing pending requests.
     */
    public void shutdown();
    
    /**
     * The service will no longer accept new requests and will make a best
     * effort attempt to terminate all existing requests and return ASAP.
     * 
     * @return Once the service has shutdown.
     */
    public void shutdownNow();
    
}

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
 * Created on Jan 8, 2009
 */

package com.bigdata.jini.start;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

import net.jini.core.lookup.ServiceItem;

import com.bigdata.service.jini.AbstractServer.RemoteDestroyAdmin;

/**
 * Extended to prefer {@link RemoteDestroyAdmin} for {@link #destroy()}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JiniProcessHelper extends ProcessHelper {

    /**
     * @param name
     * @param builder
     * @param listener
     * @throws IOException
     */
    public JiniProcessHelper(String name, ProcessBuilder builder,
            IServiceListener listener) throws IOException {

        super(name, builder, listener);

    }

    private ServiceItem serviceItem = null;
    
    synchronized public void setServiceItem(final ServiceItem serviceItem) {
        
        if (serviceItem == null)
            throw new IllegalArgumentException();
        
        if (this.serviceItem != null)
            throw new IllegalStateException();

        this.serviceItem = serviceItem;

    }

    /**
     * Uses {@link RemoteDestroyAdmin} to request that the service destroy
     * itself if {@link #setServiceItem(ServiceItem)} was invoked and the
     * service implements that interface.
     */
    public void destroy() {
        
        if (serviceItem != null) {

            final Remote proxy = (Remote) serviceItem.service;

            if (proxy instanceof RemoteDestroyAdmin) {

                try {

                    ((RemoteDestroyAdmin) proxy).destroy();

                } catch (RemoteException e) {

                    log.error(this, e);

                }

                // fall through!
                
            }

        }
        
        super.destroy();
        
    }
    
}

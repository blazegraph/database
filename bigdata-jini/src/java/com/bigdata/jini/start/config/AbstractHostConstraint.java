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
 * Created on Jan 10, 2009
 */

package com.bigdata.jini.start.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.bigdata.service.jini.JiniFederation;

/**
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractHostConstraint implements IServiceConstraint {

    protected static final Logger log = Logger.getLogger(AbstractHostConstraint.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();

    protected final InetAddress[] addr;
    
    public String toString() {
        
        return getClass() + "{addr=" + Arrays.toString(addr) + "}";
        
    }
    
    public AbstractHostConstraint(final String host) throws UnknownHostException {

        this(new String[]{host});    

    }

    public AbstractHostConstraint(final String[] host)
            throws UnknownHostException {

        if (host == null)
            throw new IllegalArgumentException();

        if (host.length == 0)
            throw new IllegalArgumentException();

        this.addr = new InetAddress[host.length];

        int i = 0;

        for (String a : host) {

            if (a == null)
                throw new IllegalArgumentException();

            this.addr[i++] = InetAddress.getByName(a);

        }

    }

    public AbstractHostConstraint(final InetAddress addr) {
        
        this(new InetAddress[]{addr});
        
    }

    public AbstractHostConstraint(final InetAddress[] addr) {

        if (addr == null)
            throw new IllegalArgumentException();

        if (addr.length == 0)
            throw new IllegalArgumentException();
        
        for(InetAddress a : addr) {
            
            if (a == null)
                throw new IllegalArgumentException();
            
        }
        
        this.addr = addr;
        
    }

    protected boolean allow(final boolean accept) {

        final InetAddress[] localAddresses;
        try {
            localAddresses = InetAddress.getAllByName("localhost");
        } catch (UnknownHostException e) {
            // should not be thrown for localhost.
            throw new AssertionError(e);
        }

        for (InetAddress tmp : addr) {

            if (DEBUG)
                log.debug("Considering: addr=" + tmp);

            if (tmp.isLoopbackAddress()) {

                return accept;

            } else {

                for (InetAddress a : localAddresses) {

                    if (tmp.equals(a)) {

                        return accept;

                    }

                }

            }

        }

        return false;

    }

    abstract public boolean allow();
    
    public boolean allow(JiniFederation ignored) {
        
        return allow();
        
    }
    
}

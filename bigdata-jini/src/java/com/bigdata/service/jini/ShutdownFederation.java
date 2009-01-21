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

package com.bigdata.service.jini;

/**
 * Utility will <strong>shutdown</strong> all services in the federation to
 * which it connects. The services may be restarted at a later time.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ShutdownFederation {

    /**
     * @param args
     *            Configuration file and optional overrides.
     * 
     * @throws InterruptedException
     */
    public static void main(final String[] args) throws InterruptedException {

        final JiniFederation fed = JiniClient.newInstance(args).connect();

        System.out.println("Waiting for service discovery.");

        Thread.sleep(5000/* ms */);

        fed.distributedFederationShutdown(true/* immediateShutdown */);

        System.out.println("Shutdown.");
        
        System.exit(0);

    }

}

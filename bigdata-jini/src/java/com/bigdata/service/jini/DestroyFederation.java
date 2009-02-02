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

import net.jini.config.ConfigurationException;

/**
 * Utility will <strong>destroy</strong> the federation to which it connects.
 * All discoverable services for the federation and all persistent state for
 * those services will be destroyed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DestroyFederation {

    protected static final String COMPONENT = DestroyFederation.class.getName(); 

    /**
     * @param args
     *            Configuration file and optional overrides.
     * 
     * @throws InterruptedException
     * @throws ConfigurationException
     */
    public static void main(final String[] args) throws InterruptedException,
            ConfigurationException {

        final JiniFederation fed = JiniClient.newInstance(args).connect();

        final long discoveryDelay = (Long) fed
                .getClient()
                .getConfiguration()
                .getEntry(COMPONENT, "discoveryDelay", Long.TYPE, 5000L/* default */);

        System.out.println("Waiting " + discoveryDelay
                + "ms for service discovery.");

        Thread.sleep(discoveryDelay/* ms */);

        fed.destroy();

        System.out.println("Destroyed.");

        System.exit(0);

    }

}

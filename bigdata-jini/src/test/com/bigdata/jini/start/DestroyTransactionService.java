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

package com.bigdata.jini.start;

import java.rmi.RemoteException;

import com.bigdata.service.IService;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.RemoteDestroyAdmin;
import com.bigdata.service.jini.TransactionServer;

/**
 * Destroys a specific service - the {@link TransactionServer}. This is for use
 * in testing the behavior of the {@link ServicesManagerServer} and the behavior
 * of the other services in the federation when the {@link TransactionServer} is
 * lost.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DestroyTransactionService {

    /**
     * @param args
     *            Configuration file and optional overrides.
     *            
     * @throws InterruptedException 
     * @throws RemoteException 
     */
    public static void main(String[] args) throws InterruptedException,
            RemoteException {

        final JiniFederation<?> fed = JiniClient.newInstance(args).connect();

        try {

            final IService service = fed.getTransactionService();

            if (service == null) {

                System.err.println("Service not found.");

            } else {

                ((RemoteDestroyAdmin) service).destroy();

                System.err.println("Service destroyed.");

            }

        } finally {

            fed.shutdown();

        }

    }

}

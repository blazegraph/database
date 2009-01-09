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
 * Created on Jan 5, 2009
 */

package com.bigdata.jini.start;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.TransactionServer;
import com.bigdata.util.NV;

/**
 * Configuration for the {@link TransactionServer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TransactionServiceConfiguration extends
        BigdataServiceConfiguration {

    /**
     * 
     */
    private static final long serialVersionUID = 2616176455506215566L;

    /**
     * @param config
     */
    public TransactionServiceConfiguration(Configuration config)
            throws ConfigurationException {

        super(TransactionServer.class, config);

    }

    public AbstractServiceStarter newServiceStarter(JiniFederation fed,
            IServiceListener listener, String zpath) throws Exception {

        return new TransactionServiceStarter(fed, listener, zpath);

    }

    protected class TransactionServiceStarter<V extends JiniProcessHelper>
            extends BigdataServiceStarter<V> {

        /**
         * @param fed
         * @param listener
         * @param zpath
         */
        protected TransactionServiceStarter(JiniFederation fed,
                IServiceListener listener, String zpath) {

            super(fed, listener, zpath);

        }

        protected NV getDataDir() {
            
            return new NV(TransactionServer.Options.DATA_DIR, serviceDir
                    .toString());
            
        }
        
    }

}

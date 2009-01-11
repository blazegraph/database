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

package com.bigdata.jini.start.config;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import com.bigdata.jini.start.IServiceListener;
import com.bigdata.jini.start.process.JiniServiceProcessHelper;
import com.bigdata.service.jini.DataServer;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.util.NV;

/**
 * Configuration for the {@link DataServer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DataServiceConfiguration extends
        BigdataServiceConfiguration {

    /**
     * 
     */
    private static final long serialVersionUID = -4902890360167993576L;

    /**
     * @param config
     */
    public DataServiceConfiguration(Configuration config)
            throws ConfigurationException {

        super(DataServer.class, config);

    }

    public DataServiceStarter newServiceStarter(JiniFederation fed,
            IServiceListener listener, String zpath) throws Exception {

        return new DataServiceStarter(fed, listener, zpath);

    }

    public class DataServiceStarter<V extends JiniServiceProcessHelper>
            extends BigdataServiceStarter<V> {

        /**
         * @param fed
         * @param listener
         * @param zpath
         */
        protected DataServiceStarter(JiniFederation fed,
                IServiceListener listener, String zpath) {

            super(fed, listener, zpath);

        }

        @Override
        protected NV getDataDir() {
            
            return new NV(DataServer.Options.DATA_DIR, serviceDir
                    .toString());
            
        }
        
    }

}

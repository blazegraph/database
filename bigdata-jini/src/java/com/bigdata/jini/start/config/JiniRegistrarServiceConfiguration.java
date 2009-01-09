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
 * Created on Jan 4, 2009
 */

package com.bigdata.jini.start.config;

import java.io.File;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import com.bigdata.jini.start.IServiceListener;
import com.bigdata.service.jini.JiniFederation;

/**
 * Somewhat specialized configuration for a jini registrar instance.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JiniRegistrarServiceConfiguration extends ServiceConfiguration {

    /**
     * 
     */
    private static final long serialVersionUID = 4601254369003651017L;

    /**
     * Configuration options.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options extends ServiceConfiguration.Options {
        
        /**
         * {@link File} that will be executed to start jini (could be "cmd" option
         * and then we could get rid of the "java" option).
         * 
         * @todo not used yet. probably move into the
         *       {@link JiniRegistrarServiceConfiguration} (an subclass of
         *       {@link ServiceConfiguration} that is specialized for starting
         *       jini).
         */
        String JINI = "jini";

    }
    
    /**
     * @param config
     * 
     * @throws ConfigurationException
     * 
     * FIXME extract the params from the AdvertDescription.
     * 
     * @todo path to the installed jini directory.
     */
    public JiniRegistrarServiceConfiguration(Configuration config)
            throws ConfigurationException {

        super("jini", config);

    }

    /**
     * FIXME not implemented yet.
     */
    @Override
    public AbstractServiceStarter newServiceStarter(JiniFederation fed,
            IServiceListener listener, String zpath) throws Exception {

        throw new UnsupportedOperationException();

    }

}

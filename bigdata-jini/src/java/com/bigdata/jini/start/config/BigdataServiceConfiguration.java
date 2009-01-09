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

import java.util.Properties;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import com.bigdata.jini.start.IServiceListener;
import com.bigdata.jini.start.process.JiniProcessHelper;
import com.bigdata.service.jini.AbstractServer;
import com.bigdata.service.jini.DataServer;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.util.NV;

/**
 * Any of the bigdata services. Concrete instances handle required parameters
 * such as the data directory for the service.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class BigdataServiceConfiguration extends
        AbstractJiniServiceConfiguration {

    /**
     * 
     */
    private static final long serialVersionUID = 734513805833840009L;

    /**
     * Options for the bigdata services.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options extends AbstractJiniServiceConfiguration.Options {
        
//        /**
//         * Service instance parameters represented as a {@link NV}[].
//         */
//        String PARAMS = "params";
        
    }
    
//    /**
//     * The initial properties for new instances of the service type.
//     */
//    public final NV[] params;

    /**
     * @param cls
     * @param config
     * @throws ConfigurationException
     */
    public BigdataServiceConfiguration(Class<? extends AbstractServer> cls,
            Configuration config) throws ConfigurationException {

        super(cls, config);

//        this.params = getParams(cls.getName(), config);
        
        if (log4j == null) {
            
            throw new ConfigurationException("Must specify: " + Options.LOG4J);
            
        }
        
    }

    protected void toString(StringBuilder sb) {

        super.toString(sb);

//        sb.append(", " + Options.PARAMS + "=" + Arrays.toString(params));

    }

//    public static NV[] getParams(String className, Configuration config)
//            throws ConfigurationException {
//
//        return (NV[]) config.getEntry(className, Options.PARAMS, NV[].class,
//                new NV[] {}/* defaultValue */);
//
//    }

    public AbstractServiceStarter newServiceStarter(final JiniFederation fed,
            final IServiceListener listener, final String logicalServiceZPath)
            throws Exception {

        return new BigdataServiceStarter(fed, listener, logicalServiceZPath);

    }
    
    /**
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <V>
     */
    public class BigdataServiceStarter<V extends JiniProcessHelper> extends
            JiniServiceStarter<V> {

        /**
         * @param fed
         * @param listener
         * @param logicalServiceZPath
         */
        protected BigdataServiceStarter(JiniFederation fed,
                IServiceListener listener, String logicalServiceZPath) {

            super(fed, listener, logicalServiceZPath);
            
        }

        /**
         * Returns the "dataDir" configuration property for the service -or-
         * <code>null</code> if the service does not use a data directory.
         * <p>
         * Note: Subclasses for {@link DataServer}, etc must add service
         * specific properties, such the dataDir, which can only be determined
         * at runtime.
         * 
         * @see JavaServiceStarter#serviceDir
         */
        protected NV getDataDir() {
        
            return null;
            
        }

        /**
         * Returns the service configuration properties (allows the override or
         * addition of those properties at service creation time).
         * <p>
         * Note: If {@link #getDataDir()} returns non-<code>null</code> then
         * that property will be included in the returned array.
         */
        @Override
        protected Properties getProperties(final Properties properties) {

            final NV dataDir = getDataDir();

            if (dataDir != null) {

                // the data directory for this service type.
                properties.setProperty(dataDir.getName(),dataDir.getValue());

            }

            return properties;

        }
        
    }

}

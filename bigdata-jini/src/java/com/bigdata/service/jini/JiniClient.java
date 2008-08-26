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
 * Created on Mar 24, 2007
 */

package com.bigdata.service.jini;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationProvider;
import net.jini.core.discovery.LookupLocator;
import net.jini.discovery.LookupDiscovery;

import com.bigdata.service.AbstractClient;
import com.bigdata.util.NV;

/**
 * A client capable of connecting to a distributed bigdata federation using
 * JINI.
 * <p>
 * Clients are configured using a Jini service configuration file. The name of
 * that file is passed to {@link #newInstance(String[])}.  The configuration 
 * must be consistent with the configuration of the federation to which you
 * wish to connect.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JiniClient extends AbstractClient {

    /**
     * The federation and <code>null</code> iff not connected.
     */
    private JiniFederation fed = null;

    synchronized public boolean isConnected() {
        
        return fed != null;
        
    }
    
    synchronized public void disconnect(boolean immediateShutdown) {
        
        if (fed != null) {

            if(immediateShutdown) {
                
                fed.shutdownNow();
                
            } else {
                
                fed.shutdown();
                
            }
            
        }
        
        fed = null;

    }

    synchronized public JiniFederation getFederation() {

        if (fed == null) {

            throw new IllegalStateException();

        }

        return fed;

    }

    synchronized public JiniFederation connect() {

        if (fed == null) {

            fed = new JiniFederation(this, jiniConfig);

        }

        return fed;

    }

    /**
     * The jini configuration which will be used to connect to the federation.
     */
    private final JiniConfig jiniConfig;

    /**
     * 
     * @param jiniConfig
     */
    protected JiniClient(JiniConfig jiniConfig) {

        super(jiniConfig.properties);

        this.jiniConfig = jiniConfig;

    }
    
    /**
     * Helper class for passing pre-extracted Jini configuration information to
     * the {@link JiniFederation}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class JiniConfig {
        
        final Configuration config;
        final String[] groups;
        final LookupLocator[] lookupLocators;
        final Properties properties;

        public JiniConfig(Configuration config, String[] groups,
                LookupLocator[] lookupLocators, Properties properties) {

            this.config = config;
            
            this.groups = groups;

            this.lookupLocators = lookupLocators;
            
            this.properties = properties;

        }

        public String toString() {
            
            return "JiniConfig"//
                    + "{groups="
                    + (groups == null ? "N/A" : "" + Arrays.toString(groups))//
                    + ",locators="
                    + (lookupLocators == null ? "N/A" : ""
                            + Arrays.toString(lookupLocators))//
                    + ",properties="+properties
                    + "}";
            
        }
        
    }

    /**
     * Conditionally installs a {@link SecurityManager}, reads
     * {@link Configuration} data from the file(s) named by <i>args</i>, reads
     * the <i>properties</i> file named in the {@value #CLIENT_LABEL} section
     * of the {@link Configuration} file, and returns the configured client.
     * 
     * @param args
     *            The command line arguments.
     * 
     * @return The new client.
     * 
     * @throws RuntimeException
     *             if there is a problem: reading the jini configuration for the
     *             client; reading the properties for the client; starting
     *             service discovery, etc.
     */
    public static JiniClient newInstance(String[] args) {

        // set the security manager.
        setSecurityManager();

        // read all the configuration data and the properties file.
        final JiniConfig jiniConfig = readConfiguration(args);

        // return the client.
        return new JiniClient(jiniConfig);
        
    }

    /**
     * Conditionally install a suitable security manager if there is none in
     * place. This is required before the client can download code. The code
     * will be downloaded from the HTTP server identified by the
     * <code>java.rmi.server.codebase</code> property specified for the VM
     * running the service.
     */
    static protected void setSecurityManager() {

        SecurityManager sm = System.getSecurityManager();
        
        if (sm == null) {

            System.setSecurityManager(new SecurityManager());
         
            log.info("Set security manager");

        } else {
            
            if (log.isInfoEnabled())
                log.info("Security manager already in place: " + sm.getClass());

        }

    }
    
    /**
     * Return the configuration data for the client.
     * <p>
     * This helper method reads {@link Configuration} data from the file(s)
     * named by <i>args</i>, reads the <i>properties</i> file named in the
     * {@value #CLIENT_LABEL} section of the {@link Configuration} file, and
     * returns the configured client.
     * 
     * @param args
     *            The command line arguments.
     * 
     * @return The configuration data for the client.
     * 
     * @throws RuntimeException
     *             if there is a problem reading the jini configuration for the
     *             client or reading the identified properties file.
     */
    static protected JiniConfig readConfiguration(String[] args) {

        final String[] groups;
        final LookupLocator[] lookupLocators;
        final Properties properties;
        try {

            // Obtain the configuration object.
            final Configuration config = ConfigurationProvider
                    .getInstance(args);

            /*
             * Extract how the client will discover services from the
             * Configuration.
             */
            groups = (String[]) config
                    .getEntry(AbstractServer.ADVERT_LABEL, "groups",
                            String[].class, LookupDiscovery.ALL_GROUPS/* default */);

            /*
             * Note: multicast discovery is used regardless if
             * LookupDiscovery.ALL_GROUPS is selected above. That is why there
             * is no default for the lookupLocators. The default "ALL_GROUPS"
             * means that the lookupLocators are ignored.
             */

            lookupLocators = (LookupLocator[]) config
                    .getEntry(
                    AbstractServer.ADVERT_LABEL, "unicastLocators",
                    LookupLocator[].class, null/* default */);

            {
                
                /*
                 * Extract the name of the optional properties file.
                 */
                
                final File propertyFile = (File) config.getEntry(
                        AbstractServer.SERVICE_LABEL, "propertyFile",
                        File.class, null/* defaultValue */);

                if (propertyFile != null) {

                    /*
                     * Read the properties file.
                     */

                    properties = getProperties(propertyFile);

                } else {

                    /*
                     * Start with an empty properties map.
                     */
                    
                    properties = new Properties();

                }

            }
            
            {
                
                /*
                 * Read the optional [properties] array.
                 */
                
                final NV[] tmp = (NV[]) config
                        .getEntry(AbstractServer.SERVICE_LABEL, "properties",
                                NV[].class, new NV[] {}/* defaultValue */);
                
                for(NV nv : tmp) {

                    if(log.isInfoEnabled()) {
                        
                        log.info(nv.toString());
                        
                    }
                    
                    properties.setProperty(nv.getName(), nv.getValue());
                    
                }
            
            }

            return new JiniConfig(config, groups, lookupLocators, properties);
            
        } catch (Exception ex) {

            /*
             * Note: No asynchronous processes have been started so we just wrap
             * the exception and throw it out.
             */

            throw new RuntimeException(ex);

        }

    }

    /**
     * Read and return the content of the properties file.
     * 
     * @param propertyFile
     *            The properties file.
     * 
     * @throws IOException
     */
    static protected Properties getProperties(File propertyFile)
            throws IOException {

        if(log.isInfoEnabled()) {
            
            log.info("Reading properties: file="+propertyFile);
            
        }
        
        final Properties properties = new Properties();

        InputStream is = null;

        try {

            is = new BufferedInputStream(new FileInputStream(propertyFile));

            properties.load(is);

            if(log.isInfoEnabled()) {
                
                log.info("Read properties: " + properties);
                
            }
            
            return properties;

        } finally {

            if (is != null)
                is.close();

        }

    }
    
}

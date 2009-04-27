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
import java.util.Properties;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;

import org.apache.zookeeper.ZooKeeper;

import com.bigdata.jini.start.config.ServiceConfiguration;
import com.bigdata.jini.start.config.ZookeeperClientConfig;
import com.bigdata.service.AbstractScaleOutClient;
import com.bigdata.service.jini.lookup.DataServicesClient;
import com.bigdata.util.NV;
import com.sun.jini.start.ServiceDescriptor;

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
 * @param <T>
 *            The generic type of the client or service.
 */
public class JiniClient<T> extends AbstractScaleOutClient<T> {

    /**
     * Options understood by the {@link JiniClient}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends AbstractScaleOutClient.Options {
        
        /**
         * The timeout in milliseconds that the client will await the discovery
         * of a service if there is a cache miss (default
         * {@value #DEFAULT_CACHE_MISS_TIMEOUT}).
         * 
         * @see DataServicesClient
         */
        String CACHE_MISS_TIMEOUT = "cacheMissTimeout";

        String DEFAULT_CACHE_MISS_TIMEOUT = "" + (2 * 1000);
     
    }
    
    /**
     * The federation and <code>null</code> iff not connected.
     */
    private JiniFederation<T> fed = null;

    synchronized public boolean isConnected() {
        
        return fed != null;
        
    }
    
    /**
     * Note: Immediate shutdown can cause odd exceptions to be logged. Normal
     * shutdown is recommended unless there is a reason to force immediate
     * shutdown.
     * 
     * @param immediateShutdown
     *            When <code>true</code> the shutdown is <em>abrubt</em>.
     *            You can expect to see messages about interrupted IO such as
     * 
     * <pre>
     * java.rmi.MarshalException: error marshalling arguments; nested exception is: 
     *     java.io.IOException: request I/O interrupted
     *     at net.jini.jeri.BasicInvocationHandler.invokeRemoteMethodOnce(BasicInvocationHandler.java:785)
     *     at net.jini.jeri.BasicInvocationHandler.invokeRemoteMethod(BasicInvocationHandler.java:659)
     *     at net.jini.jeri.BasicInvocationHandler.invoke(BasicInvocationHandler.java:528)
     *     at $Proxy5.notify(Ljava.lang.String;Ljava.util.UUID;Ljava.lang.String;[B)V(Unknown Source)
     * </pre>
     * 
     * These messages may be safely ignored if they occur during immediate
     * shutdown.
     */
    synchronized public void disconnect(final boolean immediateShutdown) {
        
        if (fed != null) {

            if(immediateShutdown) {
                
                fed.shutdownNow();
                
            } else {
                
                fed.shutdown();
                
            }
            
        }
        
        fed = null;

    }

    synchronized public JiniFederation<T> getFederation() {

        if (fed == null) {

            throw new IllegalStateException();

        }

        return fed;

    }

    synchronized public JiniFederation<T> connect() {

        if (fed == null) {

            fed = new JiniFederation<T>(this, jiniConfig, zooConfig);

        }

        return fed;

    }

    /**
     * The {@link JiniClient} configuration.
     */
    public final JiniClientConfig jiniConfig;

    /**
     * The {@link ZooKeeper} client configuration.
     */
    public final ZookeeperClientConfig zooConfig;

    /**
     * The {@link Configuration} object used to initialize this class.
     */
    private final Configuration config;
    
    /**
     * The {@link JiniClient} configuration.
     */
    public JiniClientConfig getJiniClientConfig() {
        
        return jiniConfig;
        
    }
    
    /**
     * The {@link ZooKeeper} client configuration.
     */
    public final ZookeeperClientConfig getZookeeperClientConfig() {

        return zooConfig;
        
    }
    
    /**
     * The {@link Configuration} object used to initialize this class.
     */
    public final Configuration getConfiguration() {
        
        return config;
        
    }

    /**
     * {@inheritDoc}
     * 
     * @see #getProperties(String component)
     */
    @Override
    public Properties getProperties() {
        
        return super.getProperties();
        
    }
    
    /**
     * Return the {@link Properties} for the {@link JiniClient} merged with
     * those for the named component in the {@link Configuration}. Any
     * properties found for the {@link JiniClient} "component" will be read
     * first. Properties for the named component are next, and therefore will
     * override those given for the {@link JiniClient}. You can specify
     * properties for either the {@link JiniClient} or the <i>component</i>
     * using:
     * 
     * <pre>
     * properties = NV[]{...};
     * </pre>
     * 
     * in the appropriate section of the {@link Configuration}. For example:
     * 
     * <pre>
     * 
     * // Jini client configuration
     * com.bigdata.service.jini.JiniClient {
     * 
     *     // ...
     * 
     *     // optional JiniClient properties.
     *     properties = new NV[] {
     *        
     *        new NV(&quot;foo&quot;,&quot;bar&quot;)
     *        
     *     };
     * 
     * }
     * </pre>
     * 
     * And overrides for a named component as:
     * 
     * <pre>
     * 
     * com.bigdata.service.FooBaz {
     * 
     *    properties = new NV[] {
     *    
     *        new NV(&quot;foo&quot;,&quot;baz&quot;),
     *        new NV(&quot;goo&quot;,&quot;12&quot;),
     *    
     *    };
     * 
     * }
     * </pre>
     * 
     * @param component
     *            The name of the component.
     * 
     * @return The properties specified for that component.
     * 
     * @see #getConfiguration()
     */
    public Properties getProperties(final String component)
            throws ConfigurationException {

        return JiniClient.getProperties(component, getConfiguration());

    }
    
    /**
     * Read {@value JiniClientConfig.Options#PROPERTIES} for the optional
     * application or server class identified by [cls].
     * <p>
     * Note: Anything read for the specific class will overwrite any value for
     * the same properties specified for {@link JiniClient}.
     * 
     * @param className
     *            The class name of the client or service (optional). When
     *            specified, properties defined for that class in the
     *            configuration will be used and will override those specified
     *            for the {@value Options#NAMESPACE}.
     * @param config
     *            The {@link Configuration}.
     * 
     * @todo this could be replaced by explicit use of the java identifier
     *       corresponding to the Option and simply collecting all such
     *       properties into a Properties object using their native type (as
     *       reported by the ConfigurationFile).
     */
    static public Properties getProperties(final String className,
            final Configuration config) throws ConfigurationException {
    
        final Properties properties = new Properties();

        final NV[] a = (NV[]) config
                .getEntry(JiniClient.class.getName(),
                        JiniClientConfig.Options.PROPERTIES, NV[].class,
                        new NV[] {}/* defaultValue */);

        final NV[] b;
        if (className != null) {

            b = (NV[]) config.getEntry(className,
                    JiniClientConfig.Options.PROPERTIES, NV[].class,
                    new NV[] {}/* defaultValue */);
    
        } else
            b = null;
    
        final NV[] tmp = ServiceConfiguration.concat(a, b);
    
        for (NV nv : tmp) {
    
            properties.setProperty(nv.getName(), nv.getValue());
    
        }
    
        return properties;
    
    }

    /**
     * Installs a {@link SecurityManager} and returns a new client.
     * 
     * @param args
     *            Either the command line arguments or the arguments from the
     *            {@link ServiceDescriptor}. Either way they identify the jini
     *            {@link Configuration} (you may specify either a file or URL)
     *            and optional overrides for that {@link Configuration}.
     * 
     * @return The new client.
     * 
     * @throws RuntimeException
     *             if there is a problem reading the jini configuration for the
     *             client, reading the properties for the client, etc.
     */
    public static JiniClient newInstance(final String[] args) {

        // set the security manager.
        setSecurityManager();

        try {

            return new JiniClient(args);

        } catch (ConfigurationException e) {

            throw new RuntimeException(e);
            
        }

    }

    /**
     * Configures a client.
     * 
     * @param args
     *            The jini {@link Configuration} (you may specify either a file
     *            or URL) and optional overrides for that {@link Configuration}.
     * 
     * @throws ConfigurationException
     */
    public JiniClient(final String[] args) throws ConfigurationException {

        this(JiniClient.class, ConfigurationProvider.getInstance(args));

    }

    /**
     * Configures a client.
     * 
     * @param cls
     *            The class of the client (optional) determines the component
     *            whose configuration will be read in addition to that for the
     *            {@link JiniClient} itself. Component specific values will
     *            override those specified for the {@link JiniClient} in the
     *            {@link Configuration}.
     * @param config
     *            The configuration object.
     * 
     * @throws ConfigurationException
     */
    public JiniClient(final Class cls, final Configuration config)
            throws ConfigurationException {

        super(JiniClient.getProperties(cls.getName(), config));
        
        this.jiniConfig = new JiniClientConfig(cls.getName(), config);

        this.zooConfig = new ZookeeperClientConfig(config);

        this.config = config;
        
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
         
            if (log.isInfoEnabled())
                log.info("Set security manager");

        } else {

            if (log.isInfoEnabled())
                log.info("Security manager already in place: " + sm.getClass());

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
    static protected Properties getProperties(final File propertyFile)
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

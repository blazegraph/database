package com.bigdata.zookeeper;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.zookeeper.ZooKeeper;

/**
 * Helper class for the zookeeper client configuration.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ZookeeperClientConfig {

    /**
     * Zookeeper client options.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public interface Options {

        /**
         * The namespace for these options.
         */
        String NAMESPACE = "com.bigdata.zookeeper";

        /**
         * The root node in the zookeeper instance for the federation.
         */
        String ZROOT = "zroot";

        /**
         * The "sessionTimeout" for a {@link ZooKeeper} client.
         */
        String SESSION_TIMEOUT = "sessionTimeout";

        int DEFAULT_SESSION_TIMEOUT = 5000;

        /**
         * A comma delimited list of host:port pairs for the zookeeper servers,
         * where the port is the <strong>client port</strong>.
         * 
         * <pre>
         * zoo1:2181,zoo2:2181,zoo3:2181
         * </pre>
         */
        String SERVERS = "servers";

    }

    public final String zroot;

    public final int sessionTimeout;

    public final String servers;

    public ZookeeperClientConfig(final String zroot, final int sessionTimeout,
            final String servers) {

        this.zroot = zroot;

        this.sessionTimeout = sessionTimeout;

        this.servers = servers;

    }

    public String toString() {

        return "ZooConfig"//
                + "{ zroot=" + zroot//
                + ", sessionTimeout=" + sessionTimeout//
                + ", servers=" + servers//
                + "}";

    }

    /**
     * Reads the zookeeper client configuration from a {@link Configuration}.
     * 
     * @param config
     *            The configuration object.
     *            
     * @return The client configuration.
     * 
     * @throws ConfigurationException
     */
    static public ZookeeperClientConfig readConfiguration(
            final Configuration config) throws ConfigurationException {

        // root node for federation within zookeeper.
        final String zroot = (String) config.getEntry(
                Options.NAMESPACE, Options.ZROOT,
                String.class);

        // session timeout.
        final int sessionTimeout = (Integer) config.getEntry(
                Options.NAMESPACE, Options.SESSION_TIMEOUT,
                Integer.TYPE, Options.DEFAULT_SESSION_TIMEOUT);

        // comma separated list of zookeeper services.
        final String servers = (String) config.getEntry(
                Options.NAMESPACE, Options.SERVERS,
                String.class);

        return new ZookeeperClientConfig(zroot, sessionTimeout, servers);

    }

}

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
 * Created on Jan 13, 2009
 */

package com.bigdata.jini.start.config;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.io.Writer;
import java.net.BindException;
import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.NoSuchEntryException;

import org.apache.zookeeper.server.quorum.QuorumPeerMain;

import com.bigdata.io.FileLockUtility;
import com.bigdata.jini.start.IServiceListener;
import com.bigdata.jini.start.process.ZookeeperProcessHelper;
import com.bigdata.zookeeper.ZooHelper;

/**
 * zookeeper server configuration.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ZookeeperServerConfiguration extends JavaServiceConfiguration {

    /**
     * 
     */
    private static final long serialVersionUID = 7786331380952066582L;

    /**
     * Zookeeper server configuration options.
     * <p>
     * Note: These options have the same names as those defined by zookeeper,
     * but there are some additional options which have to do with how we manage
     * zookeeper instances, the name of the zookeeper configuration file, etc.
     * The only zookeeper option that you CAN NOT specify is <code>server</code>.
     * Use {@link #SERVERS} instead to accomplish the same thing.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @see http://hadoop.apache.org/zookeeper/docs/current/zookeeperAdmin.html
     * @see http://hadoop.apache.org/zookeeper/docs/current/zookeeperStarted.html
     */
    public interface Options extends ServiceConfiguration.Options {

        /*
         * Meta-options used by the outer class.
         */
        
        /**
         * The namespace for the zookeeper server options.
         */
        String NAMESPACE = QuorumPeerMain.class.getName();

        /**
         * The basename of the zookeeper configuration file. The file itself
         * will be located in the zookeeper data directory.
         */
        String CONFIG_FILE = "configFile";

        /**
         * Default for {@link #CONFIG_FILE}
         */
        String DEFAULT_CONFIG_FILE = "zoo.config";

        /**
         * Options specifies the server configuration entries. This is a comma
         * deliminted set of zookeeper server descriptions. Each element of the
         * set has the form <code>myid=hostname:port:port</code> where <i>myid</i>
         * is the identifier for that service instance.
         */
        String SERVERS = "servers";
        
        /*
         * Minimum configuration.
         */
        
        /**
         * The port at which clients will connect to the zookeeper ensemble.
         */
        String CLIENT_PORT = "clientPort";

        String TICK_TIME = "tickTime";

        /**
         * Option specifies the data directory (defaults to a directory named
         * "zookeeper" that is a child of the value specified for
         * {@link ServiceConfiguration.Options#SERVICE_DIR}).
         * <p>
         * Note: This name (dataDir) is being used here because that is the name
         * that zookeeper uses in its own configuration file. The value given
         * for the {@link ServiceConfiguration.Options#SERVICE_DIR} is the
         * default for this option. So the serviceDir is actually ignored if you
         * specify this option as well.
         */
        String DATA_DIR = "dataDir";
        
        /*
         * Advanced configuration.
         */
        
        /**
         * Option specifies the directory for the data recovery logs (optional,
         * but it is highly advised to place this on a fast local and dedicated
         * device; defaults to value resolved for the data directory).
         */
        String DATA_LOG_DIR = "dataLogDir";

        String GLOBAL_OUTSTANDING_LIMIT = "globalOutstandingLimit";

        String PRE_ALLOC_SIZE = "preAllocSize";

        String SNAP_COUNT = "snapCount";

        String TRACE_FILE = "traceFile";

        /*
         * cluster options.
         */

        String ELECTION_ALG = "electionAlg";

        String INIT_LIMIT = "initLimit";

        String LEADER_SERVES = "leaderServes";

        String SYNC_LIMIT = "syncLimit";
        
        /*
         * Unsafe options.
         */
        
        String FORCE_SYNC = "forceSync";
        
        String SKIP_ACL= "skipACL";
        
    }
    
    /*
     * Meta-options.
     */
    
    public final String servers;

    /**
     * The zookeeper client port.
     */
    public final int clientPort;

    /**
     * The data directory as specified by {@link Options#DATA_DIR}.
     */
    public final File dataDir;
    
    /**
     * The data log directory as specified by {@link Options#DATA_LOG_DIR}.
     */
    public final File dataLogDir;
   
    /**
     * The basename of the generated zookeeper configuration file.
     * 
     * @see Options#CONFIG_FILE
     */
    public final String configFile;

    /**
     * Zookeeper properties that we don't need to know about explictly (just
     * passed through).
     */
    public final Map<String, String> other;

    /**
     * Adds value to {@link #other} if found in the {@link Configuration}.
     * 
     * @param config
     * @param k
     * @param cls
     * 
     * @throws ConfigurationException
     */
    private void putIfDefined(Configuration config, String k, Class cls)
            throws ConfigurationException {

        try {
            
            Object v = config.getEntry(Options.NAMESPACE, k, cls);
            
            other.put(k, v.toString());

        } catch (NoSuchEntryException ex) {

            // ignore.
            
        }

    }
    
    /**
     * The default used for {@link Options#TIMEOUT}.
     */
    protected static long getDefaultTimeout() {
        
        return TimeUnit.SECONDS.toMillis(4);

    }

    /**
     * @param className
     * @param config
     * @throws ConfigurationException
     */
    public ZookeeperServerConfiguration(final Configuration config)
            throws ConfigurationException {

        super(QuorumPeerMain.class.getName(), config);

        servers = (String) config.getEntry(Options.NAMESPACE, Options.SERVERS,
                String.class);
        
        // validate [servers].
        getZookeeperServerEntries();

        if (INFO)
            log.info(Options.SERVERS + "=" + servers);

        clientPort = (Integer) config.getEntry(Options.NAMESPACE,
                Options.CLIENT_PORT, Integer.TYPE);

        if (INFO)
            log.info(Options.CLIENT_PORT + "=" + clientPort);
   
        this.dataDir = (File) config
                .getEntry(Options.NAMESPACE, Options.DATA_DIR, File.class,
                        new File(serviceDir, "zookeeper"));

        if (INFO)
            log.info(Options.DATA_DIR + "=" + dataDir);

        dataLogDir = (File) config.getEntry(Options.NAMESPACE,
                Options.DATA_LOG_DIR, File.class, dataDir);

        if (INFO)
            log.info(Options.DATA_LOG_DIR + "=" + dataLogDir);

        configFile = (String) config.getEntry(Options.NAMESPACE,
                Options.CONFIG_FILE, String.class, Options.DEFAULT_CONFIG_FILE);

        if (INFO)
            log.info(Options.CONFIG_FILE + "=" + configFile);
        
        other = new LinkedHashMap<String,String>();

        /*
         * Minimum configuration.
         */
        putIfDefined(config, Options.TICK_TIME, Integer.TYPE);
        /*
         * Advanced configuration.
         */
        putIfDefined(config, Options.GLOBAL_OUTSTANDING_LIMIT, Integer.TYPE);
        putIfDefined(config, Options.PRE_ALLOC_SIZE, Long.TYPE);
        putIfDefined(config, Options.SNAP_COUNT, Integer.TYPE);
        putIfDefined(config, Options.TRACE_FILE, String.class);
        /*
         * Cluster options.
         */
        putIfDefined(config, Options.ELECTION_ALG, Integer.TYPE);
        putIfDefined(config, Options.LEADER_SERVES, Boolean.TYPE);
        putIfDefined(config, Options.SYNC_LIMIT, Integer.TYPE);
        /*
         * Unsafe options.
         */
        putIfDefined(config, Options.FORCE_SYNC, Boolean.TYPE);
        putIfDefined(config, Options.SKIP_ACL, Boolean.TYPE);

    }

    /**
     * Parses out the zookeeper server descriptions.
     * 
     * @return An array of zero or more descriptions of zookeeper instances.
     * 
     * @throws ConfigurationException
     * 
     * @todo check for duplicate server ids.
     */
    public ZookeeperServerEntry[] getZookeeperServerEntries()
            throws ConfigurationException {

        return getZookeeperServerEntries(servers);

    }

    /**
     * Parses out the zookeeper server descriptions.
     * 
     * @param The
     *            servers per {@link Options#SERVERS}
     * 
     * @return An array of zero or more descriptions of zookeeper instances.
     * 
     * @throws ConfigurationException
     * 
     * @todo check for duplicate server ids.
     */
    static public ZookeeperServerEntry[] getZookeeperServerEntries(
            final String servers) throws ConfigurationException {

        final List<ZookeeperServerEntry> serverEntries = new LinkedList<ZookeeperServerEntry>();

        // split into zero or more server entry declarations.
        final String[] fields = servers.split("\\s*,\\s*");
        
//        System.err.println("servers: "+servers);
//        System.err.println("fields : "+Arrays.toString(fields));
//        
        for (String field : fields) {
            
//            System.err.println("field  : "+field);
            
            if (field.length() == 0) {
                
                // note: handles missing "servers" option.
                continue;
                
            }
            
            final int pos = field.indexOf('=');

            if (pos == -1) {

                throw new ConfigurationException("Expecting '=' : " + servers);
                
            }
            
            final String idField = field.substring(0,pos);

            final String entryFields = field.substring(pos+1);
            

            // System.err.println("idField="+idField);
            // System.err.println("entryFields="+entryFields);

            final int id = Integer.parseInt(idField);

            final ZookeeperServerEntry entry = new ZookeeperServerEntry(id,
                    entryFields);

            serverEntries.add(entry);

            if (INFO)
                log.info(entry.toString());
            
        }

        return serverEntries.toArray(new ZookeeperServerEntry[] {});

    }

    /**
     * (Re-)starts the zookeeper server instance identified in the
     * {@link ZookeeperServerEntry} unless it is already running using the
     * properties defined in the {@link Configuration}.
     * <p>
     * The difference between a "start" and a "restart" is whether the zookeeper
     * "myid" file exists in the data directory for the service. If it does,
     * then we "restart" zookeeper using the existing configuration. If it does
     * not, then we generate the zookeeper configuration file and the myid file
     * and then start zookeeper. Of course, how a server restart for zookeeper
     * fairs depends on whether you took down the service or if it crashed, etc.
     * 
     * @param listener
     * @param entry
     *            Identifies the specific server instance to start.
     * @return
     * 
     * @todo You CAN NOT start multiple instances using this class.
     *       <p>
     *       A tighter zookeeper integration could probably fix this issue. In
     *       order to tell whether or not zookeeper is already running we test
     *       "ruok" for the client port (it we don't test then we might see a
     *       {@link BindException} in the process output, but we don't get an
     *       error when we execute the JVM to run zookeeper). Since all
     *       instances use the same {@link Options#CLIENT_PORT}, all we know is
     *       whether or not there are any instances running which were
     *       configured for that client port, but not how many and not which
     *       ones.
     *       <p>
     *       Also see {@link #kill()}, which has difficulties knowing which
     *       instance should be killed - it will kill which one is currently
     *       answering at the clientPost on the localhost!
     */
    public ZookeeperServiceStarter newServiceStarter(IServiceListener listener,
            ZookeeperServerEntry entry) {

        return new ZookeeperServiceStarter(listener, entry);

    }

    /**
     * Not supported.
     * 
     * @see #newServiceStarter(IServiceListener, ZookeeperServerEntry)
     */
    public JavaServiceStarter newServiceStarter(IServiceListener listener) {

        throw new UnsupportedOperationException();

    }

    /**
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class ZookeeperServiceStarter<V extends ZookeeperProcessHelper>
            extends JavaServiceStarter<V> {
        
        /**
         * Identifies the specific server instance to start.
         */
        protected final ZookeeperServerEntry entry;

        /*
         * These are all defined once we have the server id (myid). They use
         * the values on the outer class as a starting point.
         */
        final File dataDir;
        final File dataLogDir;
        final File configFile;
        final File myidFile;
        
        /**
         * @param entry
         *            Identifies the specific server instance to start.
         * @param listener
         */
        protected ZookeeperServiceStarter(IServiceListener listener,
                ZookeeperServerEntry entry) {
            
            super(listener);

            if (entry == null)
                throw new IllegalArgumentException();
            
            this.entry = entry;
            
            /*
             * Note: The [myid] value is used as the last component of the
             * zookeeper dataDir path so that different instances will be kept
             * apart even if they are using the same data directory.
             */
            dataDir = new File(ZookeeperServerConfiguration.this.dataDir,
                    Integer.toString(entry.id));

            if (INFO)
                log.info(Options.DATA_DIR + "=" + dataDir);

            /*
             * Note: The [myid] value is used as the last component of the
             * zookeeper dataDir path so that different instances will be kept
             * apart even if they are using the same data directory. The default
             * is whatever was specified for the dataDir.
             */
            dataLogDir = new File(ZookeeperServerConfiguration.this.dataLogDir,
                    Integer.toString(entry.id));

            if (INFO)
                log.info(Options.DATA_LOG_DIR + "=" + dataLogDir);

            // the zookeeper configuration file to be generated.
            configFile = new File(dataDir,
                    ZookeeperServerConfiguration.this.configFile);

            // the server id is written on this file.
            myidFile = new File(dataDir, "myid");

        }

        public V call() throws Exception {

            if (!dataDir.exists()) {

                dataDir.mkdirs();

            }

            if (!dataLogDir.exists()) {

                dataLogDir.mkdirs();

            }

            /*
             * Figure out whether or not this process will be the one to start
             * the zookeeper instance.
             * 
             * Note: Only the process that successfully writes the server [id]
             * on the [myid] file will start zookeeper. If the dataDir already
             * contains a non-empty [myid] file, then a new instance will not be
             * started.
             */
            final RandomAccessFile raf = FileLockUtility.openFile(myidFile,
                    "rw", true/* useFileLock */);

            try {

                if (raf.length() == 0) {

                    /*
                     * There is no [myid] file so this is a new server start.
                     */
                    
                    /*
                     * Note: writes ASCII digits in [0:9] (not unicode).
                     */
                    raf.writeBytes(Integer.toString(entry.id));

                    /*
                     * Write the zookeeper server configuration file.
                     */
                    writeConfigFile();

                    /*
                     * Start the server.
                     */
                    return super.call();

                } else {

                    /*
                     * There is an existing zookeeper server. If it is not
                     * running, then we will restart the server.
                     */

                    try {

                        /*
                         * Query for an instance already running on local host
                         * at that port.
                         */

                        ZooHelper.ruok(InetAddress.getLocalHost(), clientPort,
                                250/* timeout(ms) */);

                        throw new ZookeeperRunningException(
                                "Zookeeper already running on localhost: clientport="
                                        + clientPort);

                    } catch (IOException ex) {

                        /*
                         * Note: We are expecting an exception if zookeeper is
                         * not running on that port.
                         */ 

                        /*
                         * Restart the server.
                         */
                        return super.call();
                        
                    }

                }

            } finally {

                // Release the lock on the file.
                FileLockUtility.closeFile(myidFile, raf);

            }

        }

        /**
         * Extended to add the configuration file on the command line after the
         * class name.
         */
        @Override
        protected void addCommandArgs(List<String> cmds) {

            super.addCommandArgs(cmds);
        
            // the configuration file.
            cmds.add(configFile.toString());
            
        }

        @SuppressWarnings("unchecked")
        protected V newProcessHelper(String className,
                ProcessBuilder processBuilder, IServiceListener listener)
                throws IOException {

            return (V) new ZookeeperProcessHelper(className, processBuilder,
                    listener, clientPort);

        }
        
        protected void awaitServiceStart(final V processHelper,
                final long timeout, final TimeUnit unit) throws Exception {

            final long begin = System.nanoTime();
            
            long nanos = unit.toNanos(timeout);
            
            try {

                /*
                 * Wait up to the timeout for a process exitValue from the
                 * zookeeper instance that we started. If we get an exitValue,
                 * then that is positive proof that it died.
                 */
                
                final int exitValue = processHelper.exitValue(nanos,
                        TimeUnit.NANOSECONDS);

                throw new IOException("exitValue=" + exitValue);

            } catch (TimeoutException ex) {

                /*
                 * Since there was no timeout on the exitValue we can conclude
                 * that zookeeper started successfully.
                 */
                
            }

            // adjust for time remaining.
            nanos -= (begin - System.nanoTime());

            /*
             * Verify that an instance is up and running by connecting to the
             * client port on the local host.
             * 
             * @todo This does not tell us that the instance that we started is
             * running, just that there is a server responding at that
             * clientPort. That could have already been true.
             */

            ZooHelper.ruok(InetAddress.getLocalHost(), clientPort, (int) unit
                    .toMillis(timeout));

            log.warn("Started zookeeper");
            
        }

        /**
         * Writes the zookeeper configuration file.
         * 
         * @throws ConfigurationException
         * @throws IOException 
         */
        protected void writeConfigFile()
                throws ConfigurationException, IOException {
            
            final Properties properties = new Properties();
            
            for(Map.Entry<String, String> entry : other.entrySet()) {

                properties.setProperty(entry.getKey(),entry.getValue());
                
            }

            properties.setProperty(Options.DATA_DIR, dataDir.toString());

            properties.setProperty(Options.DATA_LOG_DIR, dataLogDir.toString());

            properties.setProperty(Options.CLIENT_PORT, Integer.toString(clientPort));

            /*
             * Write the properties into a flat text format.
             */
            final String contents;
            {

                final StringWriter out = new StringWriter();

                properties.store(out, "Zookeeper Configuration");

                contents = out.toString();

            }

            if (INFO)
                log.info("configFile=" + configFile + "\n" + contents);

            /*
             * Write the configuration onto the file.
             */
            {
                final Writer out2 = new OutputStreamWriter(
                        new BufferedOutputStream(new FileOutputStream(configFile)));

                try {

                    out2.write(contents);

                    out2.flush();

                } finally {

                    out2.close();

                }

            }

        }

    }

    /**
     * Exception throw if there is already a zookeeper server instance running
     * on the localhost.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ZookeeperRunningException extends IOException {

        /**
         * 
         */
        private static final long serialVersionUID = -3789944484005426184L;
        
        public ZookeeperRunningException(String msg) {
            
            super(msg);
            
        }
        
    }
    
}

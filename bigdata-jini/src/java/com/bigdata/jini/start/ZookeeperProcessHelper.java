package com.bigdata.jini.start;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationFile;

import org.apache.zookeeper.server.PurgeTxnLog;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

import com.bigdata.io.FileLockUtility;
import com.bigdata.jini.start.ServiceConfiguration.Options;
import com.bigdata.zookeeper.ZooHelper;

/**
 * Manages the life cycle of a zookeeper instance with extensions to send a
 * "kill" message to the service, periodically send "ruok" messages, etc.
 * 
 * @see QuorumPeerMain
 * @see PurgeTxnLog
 * 
 * @todo periodic [ruok] and alert operator if instance has failed (we could try
 *       to restart, but an operator should really intervene).
 *       <p>
 *       {@link PurgeTxnLog} must be used to periodically release snapshots and
 *       their associated logs which are no longer required for service restart.
 * 
 * @todo periodic purge of snapshots and logs, etc.
 * 
 * @todo rewrite as a {@link JavaServiceConfiguration} with a custom service
 *       starter? (This is the first service starter written and it is less well
 *       organized for that.)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ZookeeperProcessHelper extends ProcessHelper {

    /**
     * The zookeeper client port.
     */
    public final int clientPort;

    /**
     * @param name
     * @param builder
     * @param running
     * @param clientPort
     *            The zookeeper client port.
     * @throws IOException
     */
    public ZookeeperProcessHelper(String name, ProcessBuilder builder,
            IServiceListener listener, int clientPort) throws IOException {

        super(name, builder, listener);

        this.clientPort = clientPort;

    }

    /**
     * Extended to send the <code>kill</code> message to the local
     * zookeeper instance.
     * <p>
     * Note: killing zookeeper requires sending a <code>kill</code>
     * command from the host on which it is executing. zookeeper appears to
     * fork a process which is the "real" zookeeper, so just killing the
     * outer process does not do what we want.
     */
    public void destroy() {

        try {

            /*
             * Send a "kill" message to the zookeeper instance on the local
             * host.
             */

            ZooHelper.kill(clientPort);

        } catch (IOException e) {

            log.error(e.getLocalizedMessage(), e);

        }

        super.destroy();

    }

    /**
     * Parses out the zookeeper server descriptions.
     * <p>
     * Note: if there are no descriptions then the client just connects to the
     * instance on the localhost using the clientPort.
     * 
     * @return An array of zero or more descriptions of zookeeper instances.
     * 
     * @throws ConfigurationException
     */
    static public ZookeeperServerEntry[] getZookeeperServerEntries(
            Configuration config) throws ConfigurationException {

        final String servers = (String) config.getEntry(ZOOKEEPER_LABEL,
                "servers", String.class, null/* defaultValue */);

        if (servers == null) {

            log.warn("Assuming local zookeeper instance already running.");

            return new ZookeeperServerEntry[]{};
            
        }

        final List<ZookeeperServerEntry> serverEntries = new LinkedList<ZookeeperServerEntry>();

        // split into zero or more server entry declarations.
        final String[] fields = servers.split("\\s*,\\s*");
        
//        System.err.println("servers: "+servers);
//        System.err.println("fields : "+Arrays.toString(fields));
//        
        for (String field : fields) {

//            System.err.println("field  : "+field);
            
            final int pos = field.indexOf('=');
            
            if(pos==-1) {

                throw new ConfigurationException("Expecting '=' : " + servers);
                
            }
            
            final String idField = field.substring(0,pos);

            final String entryFields = field.substring(pos+1);
            
//            System.err.println("idField="+idField);
//            System.err.println("entryFields="+entryFields);
            
            final int id = Integer.parseInt(idField);

            final ZookeeperServerEntry entry = new ZookeeperServerEntry(id, entryFields);
            
            serverEntries.add(entry);

            if (INFO)
                log.info(entry.toString());
            
        }
        
        return serverEntries.toArray(new ZookeeperServerEntry[]{});

    }
    
    /**
     * Starts a zookeeper instance IFF this host is one of those identified by a
     * <code>server.#</code> property found in the {@link #ZOOKEEPER_LABEL} of
     * the {@link Configuration}.
     * <p>
     * A configuration file will be generated for the instance using the
     * properties specified in the {@link #ZOOKEEPER_LABEL} section of the
     * {@link #config}.
     * <p>
     * Required parameters:
     * <dl>
     * <dt>clientPort</dt>
     * <dd>The port at which clients will connect to the zookeeper ensemble
     * (required). This property is required, even if we will not be starting a
     * zookeeper instance, since all clients must connect to zookeeper.</dd>
     * </dl>
     * Optional parameters:
     * <dt>servers</dt>
     * <dd>A meta-property whose value is a semicolon deliminted set of
     * zookeeper server descriptions. Each element of the set has the form <i>id</i>=hostname:port:port.
     * This meta-property was required because constructions such as "server.1"
     * are not valid Java identifiers and hence can not be used within a
     * {@link Configuration}.</dd>
     * <dl>
     * <dt>tickTime</dt>
     * <dd></dd>
     * <dt>dataDir</dt>
     * <dd>The data directory for the zookeeper instance (optional). If this
     * property is not specified, then we will not attempt to start a zookeeper
     * instance. If the property is specified, a <code>server.#</code>
     * property exists, and the hostname component of that server property is a
     * hostname (or IP address) for this machine, and the dataDir does not
     * exist, then an attempt will be made to start a zookeeper instance in that
     * dataDir. Race conditions will be resolved by whichever process is able to
     * obtain an exclusive lock on the <code>myid</code> file, verify that the
     * file is empty while holding the lock, and write the service id# into the
     * file. The lock is then released, but only the process which wrote the
     * service # into the file will start the zookeeper instance.</dd>
     * </dl>
     * <dt>dataLogDir</dt>
     * <dd></dd>
     * <dt>...</dt>
     * <dd></dd>
     * </dl>
     * 
     * @return <code>true</code> iff a zookeeper instance was started.
     * 
     * @throws ConfigurationException
     * @throws IOException
     * 
     * @todo An alternative to specifying the zookeeper configuration inline
     *       would be to specify the name of the zookeeper configuration file
     *       and then parse that ourselves. We need the [clientPort] and the
     *       list of the {@link ZookeeperServerEntry}s in order to be able to
     *       connect to zookeeper instances, but those could be read out of its
     *       own property file.
     * 
     * @see http://hadoop.apache.org/zookeeper/docs/current/zookeeperStarted.html
     * @see http://hadoop.apache.org/zookeeper/docs/current/zookeeperAdmin.html
     */
    static public boolean startZookeeper(final ConfigurationFile config,
            final IServiceListener listener)
            throws ConfigurationException, IOException {

        final File dataDir = (File) config.getEntry(ZOOKEEPER_LABEL,
                "dataDir", File.class, null/* defaultValue */);

        if (dataDir == null) {

            /*
             * This is Ok. You can configure zookeeper separately or share an
             * instance that is already running.
             */
            
            log.warn("No zookeeper configuration.");
            
            return false;
            
        }
        
        /*
         * Note: jini can not handle the zookeeper [server.#] properties since
         * the property name is not a legal java identifier. Therefore I have
         * defined a meta-property "servers" whose value is the list of the
         * individual server properties.
         * 
         * Note: zookeeper interprets NO entries as the localhost with default
         * peer and leader ports. This will work as long as the localhost is
         * already running zookeeper, but this class will not automatically
         * start zookeeper for you if you do not specify the server properties.
         * 
         * @todo could check here for duplicate server ids.
         */
        final InetAddress[] addrs = InetAddress.getAllByName("localhost");

        final ZookeeperServerEntry[] entries = getZookeeperServerEntries(config);

        for (ZookeeperServerEntry entry : entries) {

            final InetAddress addr = InetAddress.getByName(entry.hostname);

            if (INFO)
                log.info("Considering: " + entry + " : addr=" + addr);

            boolean start = false;

            if (addr.isLoopbackAddress()) {

                start = true;

            } else {
                
                for (InetAddress a : addrs) {

                    if (addr.equals(a)) {

                        start = true;

                        break;

                    }

                }
                
            }

            if (start) {

                if (INFO)
                    log.info("Zookeeper instance is local: " + entry);

                return startZookeeper(config, listener, dataDir, entry);

            }

        }

        if(INFO)
            log.info("No local zookeeper instances.");
        
        return false;

    }

    /**
     * Attempts to start the server instance.
     * 
     * @param dataDir
     *            The data directory for the service.
     * @param entry
     *            A description of the service (host, ports).
     * 
     * @return <code>true</code> iff the instance was started by this process.
     * 
     * @throws IOException
     * @throws ConfigurationException 
     */
    static public boolean startZookeeper(final ConfigurationFile config,
            final IServiceListener listener, final File dataDir,
            final ZookeeperServerEntry entry) throws IOException,
            ConfigurationException {

        /*
         * Note: This is a required parameter. Even if we are not starting a
         * zookeeper instance, we need to know the port at which we can connect
         * to zookeeper.
         */
        final int clientPort = (Integer) config.getEntry(ZOOKEEPER_LABEL,
                "clientPort", Integer.TYPE);

        if (INFO)
            log.info("zooClientPort=" + clientPort);
        
        if(!dataDir.exists()) {
            
            dataDir.mkdirs();
            
        }

        /*
         * Figure out whether or not this process will be the one to start the
         * zookeeper instance.
         * 
         * Note: Only the process that successfully writes the server [id] on
         * the [myid] file will start zookeeper. If the dataDir already contains
         * a non-empty [myid] file, then a new instance will not be started.
         */
        final File myidFile = new File(dataDir, "myid");
        {
         
            final RandomAccessFile raf = FileLockUtility.openFile(myidFile,
                    "rw", true/* useFileLock */);

            try {

                if (raf.length() != 0) {

                    log
                            .warn("Service id already assigned - will not start zookeeper: file="
                                    + myidFile);
                    
                    // another process won the race.
                    return false;

                }

                /*
                 * Note: writes ASCII digits in [0:9] (not unicode).
                 */
                raf.writeBytes(Integer.toString(entry.id));

            } finally {

                FileLockUtility.closeFile(myidFile, raf);

            }
            
        }

        try {
        
            /*
             * Create the configuration file inside of the [dataDir].
             */
            {

                /*
                 * A set of reserved properties that are not copied through to
                 * the zookeeper configuration file.
                 */
                final Set<String> reserved = new HashSet<String>(
                        ServiceConfiguration.Options.reserved);

                reserved.add("servers");
                
                /*
                 * Collect all property values (other than "servers" or other
                 * special properties which we define) in a Properties object
                 * (it would be easier if the zookeeper properties had their own
                 * namespace).
                 */
                final Set<String> names = config.getEntryNames();

                final Properties properties = new Properties();

                for (String name : names) {

                    if (!name.startsWith(ZOOKEEPER_LABEL))
                        continue;

                    // the name of the property
                    final String s = name
                            .substring(ZOOKEEPER_LABEL.length() + 1);

                    // skip our meta-property.
                    if(reserved.contains(s)) continue;
                    
                    /*
                     * The value of that property (using whatever is its natural
                     * type).
                     */
                    final Object v = config.getEntry(ZOOKEEPER_LABEL, s,
                            ((ConfigurationFile) config).getEntryType(
                                    ZOOKEEPER_LABEL, s));

                    // add to the map.
                    properties.setProperty(s, v.toString());

                }

                /*
                 * Generate the zookeeper configuration file.
                 */
                final OutputStream out = new FileOutputStream(new File(dataDir,
                        ZOO_CONFIG));

                try {

                    properties.store(out, "Zookeeper Configuration");

                } finally {

                    out.close();

                }
            
            }
            
            /*
             * Start zookeeper.
             */
            {
                
                final List<String> cmds = new LinkedList<String>();

                cmds.add("java");

                /*
                 * Optional properties to be specified to java on the command
                 * line, e.g., the heap size, etc.
                 */
                final String[] jvmargs = (String[]) config.getEntry(
                        ZOOKEEPER_LABEL, Options.ARGS,
                        String[].class, new String[] {}/* defaultValue */);

                for (String arg : jvmargs) {

                    cmds.add(arg);

                }
                
                /*
                 * Optional classpath override.
                 */
                final String classpath = (String) config.getEntry(
                        ZOOKEEPER_LABEL,
                        JavaServiceConfiguration.Options.CLASSPATH,
                        String.class, null/* defaultValue */);

                if (classpath != null) {

                    /*
                     * When [classpath] is specified, we explicitly set that
                     * command line argument.
                     */
                    
                    cmds.add("-cp");
                    
                    cmds.add(classpath);
                    
                }

                final String log4jURI = (String) config.getEntry(
                        ZOOKEEPER_LABEL,
                        JavaServiceConfiguration.Options.LOG4J, String.class,
                        null/* defaultValue */);

                if (log4jURI != null) {

                    cmds.add("-Dlog4j.configuration=\"" + log4jURI + "\"");
                    
                }
                
                // the class to be executed.
                cmds.add("org.apache.zookeeper.server.quorum.QuorumPeerMain");

                // the configuration file (in the data directory).
                cmds.add(new File(dataDir,ZOO_CONFIG).toString());
                
                final ProcessBuilder b = new ProcessBuilder(cmds);
                
                /*
                 * Note: DO NOT change to the dataDir. That leads to a recursion
                 * where zookeeper creates a its own "dataDir" inside of the
                 * exiting dataDir.
                 */
//                 specify the startup directory.
//                b.directory(dataDir);
                
                // merge stdout and stderr.
                b.redirectErrorStream(true);

                // start zookeeper.
                final ZookeeperProcessHelper helper = new ZookeeperProcessHelper(
                        "zookeeper(" + entry.id + ")", b, listener, clientPort);

                /*
                 * Wait a bit to see if the process starts successfully. If not,
                 * then we need to do some cleanup.
                 * 
                 * @todo the cleanup could be specified as a runnable and given
                 * with a timeout to the process helper, but we don't want to
                 * delete the myid file if [ruok] has ever responded. After that
                 * the service should be restarted.
                 */
                try {

                    final int exitValue = helper.exitValue(1000,
                            TimeUnit.MILLISECONDS);

                    throw new IOException("exitValue=" + exitValue);
                    
                } catch (TimeoutException ex) {

                    /*
                     * Verify up and running by connecting to the client port on
                     * the local host.
                     */

                    ZooHelper.ruok(InetAddress.getLocalHost(), clientPort,
                                    100/* timeout(ms) */);

//                    log.warn(helper.stat(InetAddress.getLocalHost(),
//                            zooClientPort).toString());
//
//                    log.warn(helper.dump(InetAddress.getLocalHost(),
//                            zooClientPort).toString());
                    
                    log.warn("Started zookeeper");

                    return true;

                }
                
            }
            
        } catch(Throwable t) {
            
            log.warn("Zookeeper startup problem: " + t);
            
            /*
             * If anything goes wrong during service startup we will delete the
             * [myid] file so that someone else gets a shot at starting this
             * zookeeper instance.
             */
            
            myidFile.delete();
            
            throw new RuntimeException(t);
            
        }
        
    }

    /**
     * The label in the {@link Configuration} file for the zookeeper service
     * description.
     */
    private final static transient String ZOOKEEPER_LABEL = "org.apache.zookeeper";

    /**
     * The basename of the zookeeper configuration file.
     * 
     * @todo put in an Options interface w/ a default value.
     */
    private final static transient String ZOO_CONFIG = "zoo.config";

}

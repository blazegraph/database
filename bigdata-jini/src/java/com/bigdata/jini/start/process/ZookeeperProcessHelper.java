package com.bigdata.jini.start.process;

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
import com.bigdata.jini.start.IServiceListener;
import com.bigdata.jini.start.config.JavaServiceConfiguration;
import com.bigdata.jini.start.config.ServiceConfiguration;
import com.bigdata.jini.start.config.ZookeeperServerEntry;
import com.bigdata.service.jini.JiniFederation;
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
 * @todo While there is a lot of overflow with the {@link ServiceConfiguration}
 *       class, the two can not be readily merged. The problem is that this
 *       class must operate without reference to a {@link JiniFederation}. The
 *       shared logic deals mainly with options for java programs and starting
 *       java programs.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ZookeeperProcessHelper extends ProcessHelper {

    /**
     * Zookeeper server configuration options.
     * <p>
     * Note: ANY values in this namespace that are not recognized will be copied
     * into the generated zookeeper configuration file.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @see http://hadoop.apache.org/zookeeper/docs/current/zookeeperStarted.html
     * @see http://hadoop.apache.org/zookeeper/docs/current/zookeeperAdmin.html
     */
    public interface Options extends ServiceConfiguration.Options {

//        /**
//         * The namespace for the zookeeper server options.
//         */
//        String NAMESPACE = QuorumPeerMain.class.getName();

        /**
         * The basename of the zookeeper configuration file.
         */
        String CONFIG_FILE = "configFile";

        /**
         * Default for {@link #CONFIG_FILE}
         */
        String DEFAULT_CONFIG_FILE = "zoo.config";

        /**
         * The port at which clients will connect to the zookeeper ensemble.
         */
        String CLIENT_PORT = "clientPort";

        /**
         * Option specifies the data directory (required).
         */
        String DATA_DIR = "dataDir";
        
        /**
         * Options specifies the server configuration entries. This is a comma
         * deliminted set of zookeeper server descriptions. Each element of the
         * set has the form <code>myid=hostname:port:port</code> where <i>myid</i>
         * is the identifier for that service instance.
         */
        String SERVERS = "servers";
        
    }

    /**
     * The namespace for the {@link Options}.
     */
    public static final String COMPONENT = QuorumPeerMain.class.getName();
    
    /**
     * The zookeeper client port.
     */
    protected final int clientPort;

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
     * Extended to send the <code>kill</code> message to the local zookeeper
     * instance.
     * <p>
     * Note: killing zookeeper requires sending a <code>kill</code> command
     * from the host on which it is executing. zookeeper appears to fork a
     * process which is the "real" zookeeper, so just killing the outer process
     * does not do what we want.
     * <p>
     * Note: Due to how zookeeper peers work, we don't really know which
     * instance is answering requests for the clientPort. It could be any
     * instance in the ensemble. Therefore, DO NOT run multiple zookeeper
     * instances on the same host with this class! It will kill the current
     * master!
     * 
     * @throws InterruptedException
     */
    public int kill() throws InterruptedException {

        try {

            /*
             * Send a "kill" message to the zookeeper instance on the local
             * host.
             */

            ZooHelper.kill(clientPort);

        } catch (IOException e) {

            log.error(e.getLocalizedMessage(), e);
            
            return super.kill();

        }

        try {
            /*
             * Wait for zookeeper to die, but only up to a timeout.
             */
            final int exitValue = exitValue(5, TimeUnit.SECONDS);
            listener.remove(this);
            return exitValue;
        } catch (TimeoutException e) {
            /*
             * Since the process did not we probably killed the wrong instance.
             * 
             * @todo this tries to kill it using the super class to kill the
             * process. This could cause two instances of zookeeper (on the
             * localhost) to be killed since we may have already killed the
             * wrong one above. An alternative here would be to wrap and rethrow
             * the TimeoutException.
             */
            log.error(this, e);
            return super.kill();
        }
        
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

        final String servers = (String) config
                .getEntry(QuorumPeerMain.class.getName(), Options.SERVERS,
                        String.class, ""/* defaultValue */);

        if (INFO)
            log.info(Options.SERVERS + "=" + servers);
        
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
        
        return serverEntries.toArray(new ZookeeperServerEntry[]{});

    }
    
    /**
     * (Re-)starts any zookeeper server(s) for the localhost that are identified
     * in the {@link ConfigurationFile} and are not currently running. The
     * difference between a "start" and a "restart" is whether the zookeeper
     * "myid" file exists in the data directory for the service. If it does,
     * then we "restart" zookeeper using the existing configuration. If it does
     * not, then we generate the zookeeper configuration file and the myid file
     * and then start zookeeper. Of course, how a server restart for zookeeper
     * fairs depends on whether you took down the service or if it crashed, etc.
     * 
     * @return the #of instances (re-)started.
     * 
     * @throws ConfigurationException
     * @throws IOException
     * 
     * @see Options
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
    static public int startZookeeper(final Configuration config,
            final IServiceListener listener) throws ConfigurationException,
            IOException {

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

        int nstart = 0;
        
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

                try {
                    
                    if (startZookeeper(config, listener, entry)) {

                        nstart++;

                    }
                    
                } catch (InterruptedException ex) {
                    
                    log.error("Could not start: entry=" + entry, ex);
                    
                }

            }

        } // next entry

        if (INFO)
            log.info("started=" + nstart + " instances");
        
        return nstart;

    }

    /**
     * (Re-)starts the zookeeper server instance identified in the
     * {@link ZookeeperServerEntry} unless it is already running using the
     * properties defined in the {@link Configuration}.
     * 
     * @param config
     * @param listener
     * @param entry
     *            A description of the service (host, ports).
     * 
     * @return <code>true</code> iff the instance was started by this process.
     * 
     * @throws IOException
     * @throws ConfigurationException
     * @throws InterruptedException
     */
    static public boolean startZookeeper(final Configuration config,
            final IServiceListener listener, final ZookeeperServerEntry entry)
            throws IOException, ConfigurationException, InterruptedException {

        final int clientPort = (Integer) config.getEntry(COMPONENT,
                Options.CLIENT_PORT, Integer.TYPE);

        if (INFO)
            log.info(Options.CLIENT_PORT + "=" + clientPort);

        final File dataDir = (File) config.getEntry(COMPONENT, Options.DATA_DIR, File.class);

        if (INFO)
            log.info(Options.DATA_DIR + "=" + dataDir);

        if (!dataDir.exists()) {

            dataDir.mkdirs();

        }

        // the zookeeper configuration file to be generated. 
        final File configFile = new File(dataDir, (String) config.getEntry(
                COMPONENT, Options.CONFIG_FILE, String.class,
                Options.DEFAULT_CONFIG_FILE));

        if (INFO)
            log.info(Options.CONFIG_FILE + "=" + configFile);
        
        final File myidFile = new File(dataDir, "myid");

//        boolean wroteMyIdFile = false;
//        try {

            /*
             * Figure out whether or not this process will be the one to start the
             * zookeeper instance.
             * 
             * Note: Only the process that successfully writes the server [id] on
             * the [myid] file will start zookeeper. If the dataDir already contains
             * a non-empty [myid] file, then a new instance will not be started.
             */
            {

                final RandomAccessFile raf = FileLockUtility.openFile(myidFile,
                        "rw", true/* useFileLock */);

                try {

                    if (raf.length() != 0) {

                        /*
                         * restart the zookeeper instance.
                         */

                        try {

                            /*
                             * Query for an instance already running on local
                             * host at that port.
                             */

                            ZooHelper.ruok(InetAddress.getLocalHost(),
                                    clientPort, 250/* timeout(ms) */);

                            if (INFO)
                                log
                                        .info("Zookeeper already running on localhost: clientport="
                                                + clientPort);

                            return false;

                        } catch (IOException ex) {

                            return startZookeeper(config, configFile, listener,
                                    clientPort, "zookeeper(" + entry.id + ")");
                        }

                    }

                    /*
                     * Note: writes ASCII digits in [0:9] (not unicode).
                     */
                    raf.writeBytes(Integer.toString(entry.id));

//                    wroteMyIdFile = true;

                } finally {

                    FileLockUtility.closeFile(myidFile, raf);

                }

            }

            writeConfigFile(config, configFile);

            return startZookeeper(config, configFile, listener, clientPort,
                    "zookeeper(" + entry.id + ")");

//        } catch (Exception t) {
//
//            log.warn("Zookeeper startup problem: " + t);
//
//            /*
//             * If anything goes wrong during service startup and WE wrote the
//             * [myid] file, then we delete the [myid] file so that someone else
//             * gets a shot at starting this zookeeper instance.
//             */
//
//            if (wroteMyIdFile)
//                myidFile.delete();
//
//            throw new RuntimeException(t);
//
//        }

    }

    /**
     * Create the configuration file inside of the [dataDir].
     * 
     * @throws ConfigurationException
     * @throws IOException 
     */
    static void writeConfigFile(Configuration config, File configFile)
            throws ConfigurationException, IOException {

        /*
         * A set of reserved properties that are not copied through to the
         * zookeeper configuration file.
         */
        final Set<String> reserved = new HashSet<String>(
                ServiceConfiguration.Options.reserved);

        reserved.add("servers");

        /*
         * Collect all property values (other than "servers" or other special
         * properties which we define) in a Properties object (it would be
         * easier if the zookeeper properties had their own namespace).
         * 
         * @todo this introduces a dependency on ConfigurationFile. We could
         * just declare all the properties of interest in Options and handle
         * them explicitly if we had to, but then we need to stay onto of new
         * zookeeper options as they are defined (or maybe zookeeper declares
         * the known options in a source file somewhere and we can use those
         * decls?)
         */
        final Set<String> names = ((ConfigurationFile) config).getEntryNames();

        final Properties properties = new Properties();

        for (String name : names) {

            if (!name.startsWith(COMPONENT))
                continue;

            // the name of the property
            final String s = name.substring(COMPONENT.length() + 1);

            // skip our meta-property.
            if (reserved.contains(s))
                continue;

            /*
             * The value of that property (using whatever is its natural type).
             * 
             * @todo Again, this introduces a dependency on ConfigurationFile.
             * If we enumerate the zookeeper options in Options we would still
             * need their natural types to copy the properties into the
             * generated configuration file.
             */
            final Object v = config.getEntry(COMPONENT, s,
                    ((ConfigurationFile) config).getEntryType(COMPONENT, s));

            // add to the map.
            properties.setProperty(s, v.toString());

        }

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

    /**
     * Start zookeeper with an existing configuration file.
     * 
     * @param config
     * @param configFile
     * @param listener
     * @param clientPort
     * @param processName
     * 
     * @return
     * 
     * @throws ConfigurationException
     * @throws IOException
     * @throws InterruptedException
     */
    static private boolean startZookeeper(final Configuration config,
            final File configFile, final IServiceListener listener,
            final int clientPort, final String processName)
            throws ConfigurationException, IOException, InterruptedException {

        final String className = QuorumPeerMain.class.getName();

        final List<String> cmds = new LinkedList<String>();

        // executable.
        cmds.add(JavaServiceConfiguration.getJava(className, config));

        /*
         * Optional properties to be specified to java on the command line,
         * e.g., the heap size, etc.
         */
        final String[] args = ServiceConfiguration.concat(
                JavaServiceConfiguration.getDefaultJavaArgs(className, config),
                JavaServiceConfiguration.getArgs(className, config));

        for (String arg : args) {

            cmds.add(arg);

        }

        /*
         * Optional classpath override.
         */
        final String[] classpath = JavaServiceConfiguration.getClasspath(
                className, config);

        if (classpath != null) {

            /*
             * When [classpath] is specified, we explicitly set that command
             * line argument.
             */

            cmds.add("-cp");

            final StringBuilder sb = new StringBuilder();

            for (String e : classpath) {

                if (sb.length() > 0)
                    sb.append(File.pathSeparatorChar);

                sb.append(e);

            }

            cmds.add(sb.toString());

        }

        final String log4jURI = JavaServiceConfiguration.getLog4j(className,
                config);

        if (log4jURI != null) {

            cmds.add("-Dlog4j.configuration="
                    + ServiceConfiguration.q(log4jURI));

        }

        // the class to be executed.
        cmds.add(className);

        // the configuration file.
        cmds.add(configFile.toString());

        final ProcessBuilder b = new ProcessBuilder(cmds);

        /*
         * Note: DO NOT change to the dataDir. That leads to a recursion where
         * zookeeper creates a its own "dataDir" inside of the exiting dataDir.
         */
        // specify the startup directory.
        // b.directory(dataDir);
        // merge stdout and stderr.
        b.redirectErrorStream(true);

        // start zookeeper.
        final ZookeeperProcessHelper helper = new ZookeeperProcessHelper(
                processName, b, listener, clientPort);

        /*
         * Wait a bit to see if the process starts successfully. If not, then we
         * need to do some cleanup.
         */
        try {

            final int exitValue = helper.exitValue(1000, TimeUnit.MILLISECONDS);

            throw new IOException("exitValue=" + exitValue);

        } catch (TimeoutException ex) {

            /*
             * Verify up and running by connecting to the client port on the
             * local host.
             * 
             * @todo This does not tell us that the instance that we started is
             * running, just that there is a server responding at that
             * clientPort.  That could have already been true.
             */

            ZooHelper
                    .ruok(InetAddress.getLocalHost(), clientPort, 100/* timeout(ms) */);

            // log.warn(helper.stat(InetAddress.getLocalHost(),
            // zooClientPort).toString());
            //
            // log.warn(helper.dump(InetAddress.getLocalHost(),
            // zooClientPort).toString());

            log.warn("Started zookeeper");

            return true;

        }

    }

}

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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationFile;
import net.jini.config.ConfigurationProvider;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.entry.AbstractEntry;
import net.jini.jeri.BasicILFactory;
import net.jini.jeri.BasicJeriExporter;
import net.jini.jeri.tcp.TcpServerEndpoint;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.entry.Comment;
import net.jini.lookup.entry.Name;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import sun.security.jca.ServiceId;

import com.bigdata.jini.lookup.entry.Hostname;
import com.bigdata.jini.lookup.entry.ServiceToken;
import com.bigdata.jini.start.BigdataZooDefs;
import com.bigdata.jini.start.IServiceListener;
import com.bigdata.jini.start.process.JiniServiceProcessHelper;
import com.bigdata.jini.start.process.ProcessHelper;
import com.bigdata.service.jini.AbstractServer;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniClientConfig;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.JiniUtil;
import com.bigdata.zookeeper.ZNodeCreatedWatcher;
import com.bigdata.zookeeper.ZookeeperClientConfig;

/**
 * Abstract implementation for jini-based services.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class JiniServiceConfiguration extends
        JavaServiceConfiguration {

    /**
     * Additional {@link Configuration} options understood by
     * {@link JiniServiceConfiguration}.
     * <p>
     * Note: A <strong>canonical</strong> {@link Name} will be automatically
     * added to the {@link Entry}[] using the class and znode of the service
     * instance. It is a good idea not to specify additional service names as
     * something might break :-).
     * <p>
     * Note: A {@link Hostname} attribute will be automatically added to the
     * entries.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options extends JavaServiceConfiguration.Options,
            JiniClientConfig.Options {
     
        /**
         * These options are fully qualified "name=value" parameter overrides
         * for the {@link Configuration} and will appear at the end of the
         * command line. This may be used to pass in overrides for the Jini
         * {@link Configuration} that apply to either a specific service or to
         * all jini services.
         * 
         * @see ConfigurationProvider#getInstance(String[])
         */
        String JINI_OPTIONS = "jiniOptions";

    }

    public final Entry[] entries;
    public final String[] groups;
    public final LookupLocator[] locators;
    public final Properties properties;
    public final String[] jiniOptions;
    
    protected void toString(StringBuilder sb) {

        super.toString(sb);

        sb.append(", entries=" + Arrays.toString(entries));

        sb.append(", groups=" + Arrays.toString(groups));
        
        sb.append(", locators=" + Arrays.toString(locators));

        sb.append(", properties=" + properties);

        sb.append(", jiniOptions=" + Arrays.toString(jiniOptions));

    }
    
    /**
     * @param cls
     * @param config
     * @throws ConfigurationException
     */
    public JiniServiceConfiguration(final Class cls, final Configuration config)
            throws ConfigurationException {
 
        super(cls, config);
        
        JiniClientConfig tmp = new JiniClientConfig(cls,config);
        
        entries = tmp.entries;

        groups = tmp.groups;
        
        locators = tmp.locators;
        
        properties = tmp.properties;
        
        jiniOptions = getJiniOptions(cls.getName(), config);
        
    }

//    public AbstractServiceStarter newServiceStarter(
//            ServicesManager servicesManager, String zpath) throws Exception {
//
//        return new JiniServiceStarter(servicesManager, zpath);
//        
//    }
    
    /**
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <V>
     */
    public class JiniServiceStarter<V extends JiniServiceProcessHelper> extends
            JavaServiceStarter<V> {
        
        /**
         * The basename of the service configuration file.
         */
        final private String CONFIG_FILE = "service.config";
        
        /**
         * The generated {@link Configuration} file.
         */
        final File configFile = new File(serviceDir, CONFIG_FILE);

        /**
         * @param fed
         * @param listener
         * @param logicalServiceZPath
         *            This is an example of that zpath.
         * <pre>
         * /test/fed0000000014/config/TransactionServer/logicalService0000000000/physicalService0000000000
         * </pre>
         * where <code>/test/fed0000000014/</code> is the zroot for the
         * federation.
         */
        protected JiniServiceStarter(final JiniFederation fed,
                final IServiceListener listener,
                final String logicalServiceZPath) {

            super(fed, listener, logicalServiceZPath);

        }

        /**
         * Imports that will be written into the generated
         * {@link ConfigurationFile}.
         */
        public String[] getImports() {

            return new String[] {
            
                    "net.jini.jeri.BasicILFactory",
                    "net.jini.jeri.BasicJeriExporter",
                    "net.jini.jeri.tcp.TcpServerEndpoint",

                    "net.jini.discovery.LookupDiscovery",
                    "net.jini.core.discovery.LookupLocator",
                    "net.jini.core.entry.Entry",
                    "net.jini.lookup.entry.Name",
                    "net.jini.lookup.entry.Comment",
                    "net.jini.lookup.entry.Address",
                    "net.jini.lookup.entry.Location",
                    "net.jini.lookup.entry.ServiceInfo",

                    "java.io.File",

                    "com.bigdata.util.NV",

                    "com.bigdata.service.IBigdataClient",
                    "com.bigdata.service.jini.*",

                    "org.apache.zookeeper.ZooDefs",
                    "org.apache.zookeeper.data.ACL",
                    "org.apache.zookeeper.data.Id",

            };
            
        }

        /**
         * Extended to specify the configuration file (in the service directory)
         * as an argument to the java class whose main routine will be invoked
         * and to add the {@link Options#JINI_OPTIONS}.
         */
        @Override
        protected void addServiceOptions(List<String> cmds) {

            // The configuration file goes 1st.
            cmds.add(configFile.toString());

            super.addServiceOptions(cmds);
            
            for (String arg : jiniOptions) {

                cmds.add(arg);

            }

        }

        /**
         * Extended to write the configuration file in the service directory.
         */
        @Override
        protected void setUp() throws Exception {
            
            super.setUp();
            
            writeConfigFile();
            
        }
        
        /**
         * Generates the contents of the configuration file and writes it on the
         * {@link #configFile}.
         */
        protected void writeConfigFile() throws IOException {

                // generate the file contents.
            final String contents;
            {
                
                final StringWriter out = new StringWriter();

                writeConfigFile(out);

                out.flush();

                contents = out.toString();
                
            }

            if (INFO)
                log.info("configFile=" + configFile + "\n" + contents);

            // and write the data onto the file.
            {

                final Writer out2 = new OutputStreamWriter(
                        new BufferedOutputStream(new FileOutputStream(
                                configFile)));

                try {

                    out2.write(contents);

                    out2.flush();

                } finally {

                    out2.close();

                }
                
            }

            if(false) {
                
                /*
                 * Validate the generated configuration by parsing it.
                 * 
                 * Note: There can be different classpath assumptions which
                 * could cause the parse to fail so this should not be enabled
                 * except for debugging.
                 */
                
                try {

                    ConfigurationProvider.getInstance(new String[] { configFile
                            .toString() });
                    
                    if(INFO)
                        log.info("Validated generated configuration");
                    
                } catch (ConfigurationException e) {
                    
                    throw new RuntimeException(
                            "Errors in generated configuration: " + e, e);
                    
                }
                
            }
            
        }

        /**
         * Generates the contents of the configuration file.
         * 
         * @param out
         * 
         * @throws IOException
         */
        protected void writeConfigFile(Writer out) throws IOException {

            // write comments on the file.
            writeComments(out);
            
            out.write("\n");
            
            // write import statements.
            for (String i : getImports()) {

                out.write("import " + i + ";\n");
                
            }
            out.write("\n");

            out.write("\n\n" + JiniClient.class.getName() + " {\n");
            {

                writeEntries(out);
                
                writeGroups(out);
                
                writeLocators(out);
                
            }
            out.write("}\n");

            out.write("\n\n" + className + " {\n");
            writeServiceDescription(out);
            out.write("}\n");

            // configuration for the zookeeper client.
            writeZookeeperClientConfig(out);
            
        }
        
        /**
         * Write comments at the top of the configuration file.
         */
        protected void writeComments(Writer out) throws IOException {
            
            out.write("// className=" + className + "\n");

            out.write("// date=" + new Date() + "\n");

        }

        /**
         * Returns the {@link Entry}[] used to describe the service (allows the
         * override or addition of entries at service creation time).
         * <p>
         * Note: A canonical {@link Name} entry is added. It is formed from the
         * service type and the logical service znode.
         * <p>
         * Note: The {@link Hostname} on which the service is running is added.
         * <p>
         * Note: The {@link ServiceToken} attribute is added.
         */
        protected Entry[] getEntries(Entry[] entries) throws IOException {

            final Name serviceName = new Name(this.serviceName);

            final Hostname hostName = new Hostname(InetAddress.getLocalHost()
                    .getCanonicalHostName().toString());

            final ServiceToken serviceToken = new ServiceToken(
                    this.serviceToken);
            
            return concat(new Entry[] { serviceName, hostName, serviceToken },
                    entries);

        }

        /**
         * @param out
         * @throws IOException
         */
        protected void writeEntries(Writer out) throws IOException {

            final Entry[] entries = getEntries(JiniServiceConfiguration.this.entries);

            out.write("\nentries=new Entry[]{\n");

            for (Entry e : entries) {

                writeEntry(out, e);

                out.write(",\n");
                
            }

            out.write("};\n");

        }

        /**
         * Write out the ctor for an {@link Entry}. For example, generating
         * <code>new net.jini.lookup.entry.Name("foo")</code> when given a
         * {@link Name}.
         * <P>
         * Note: There is no general purpose mechanism for emitting
         * {@link Entry} attributes using reflection, even when the
         * {@link Entry} is an {@link AbstractEntry} (ctors do not declare the
         * relationship between their arguments and the public fields). Further,
         * jini will not let you use the zero arg public ctor and then set the
         * fields from the public fields declared by the Entry class from within
         * the generated configuration file. For example,
         * <code>_entry1.name = "foo";</code> is rejected by the
         * {@link ConfigurationProvider}. This unpleasant situation means that
         * we are hardcoding the ctor calls based on the class. However, you can
         * use this method as a hook to extend the logic to handle {@link Entry}
         * implementations which it does not already understand.
         * 
         * @throws UnsupportedOperationException
         *             if it can not emit an entry of some unknown class.
         */ 
        protected void writeEntry(Writer out, Entry e) throws IOException {

            final Class<? extends Entry> cls = e.getClass();
            
            out.write("new " + e.getClass().getName() + "(");

            if (Name.class.equals(cls)) {
                
                out.write(q(((Name) e).name));

            } else if (Comment.class.equals(cls)) {
                    
                out.write(q(((Comment) e).comment));
                    
            } else if (Hostname.class.equals(cls)) {

                out.write(q(((Hostname) e).hostname));

            } else if (ServiceToken.class.equals(cls)) {
                
                out.write("java.util.UUID.fromString("
                        + q(((ServiceToken) e).serviceToken.toString()) + ")");

            } else {
             
                throw new UnsupportedOperationException(
                        "Can not emit entry: cls=" + cls.getName());
                
            }

            out.write(")");
            
        }
        
        protected void writeGroups(Writer out) throws IOException {

            if (groups == null) {
                
                // Note: Handles the ALL_GROUPS case (a null).
                out.write("\ngroups=null;\n");
                
            } else {
                
                out.write("\ngroups=new String[]{\n");
                
                for (String e : groups) {

                    out.write(e + "\n");

                }
                
                out.write("};\n");
                
            }

        }

        protected void writeLocators(Writer out) throws IOException {

            out.write("\nlocators=new " + LookupLocator.class.getName()
                    + "[]{\n");

            for (LookupLocator e : locators) {

                out.write("new " + LookupLocator.class.getName() + "(\""
                        + e.getHost() + "\"," + e.getPort() + "\n");

            }

            out.write("};\n");

        }
        
        /**
         * Writes the ServiceDescription. This section contains the
         * <code>exporter</code>, <code>serviceIdFile</code>, and
         * <code>logicalServiceZPath</code> entries.
         */
        protected void writeServiceDescription(Writer out) throws IOException {

            writeExporter(out);

            writeServiceIdFile(out);
            
            writeLogicalServiceZPath(out);

            writeProperties(out);
            
        }

        /**
         * Writes the properties for the specificed class namespace into the
         * generated service configuration file.
         * 
         * @param writer
         * 
         * @throws IOException
         */
        protected void writeProperties(final Writer out) throws IOException {

            // extension hook.
            final Properties properties = getProperties(JiniServiceConfiguration.this.properties);
            
            out.write("\nproperties = new NV[]{\n");

            Enumeration e = properties.propertyNames();

            while(e.hasMoreElements()) {

                final String k = e.nextElement().toString();
                
                final String v = properties.getProperty(k);
                
                out.write("new NV( " + q(k) + ", " + q(v) + "),\n");
                
            }
            
            out.write("};\n");

        }

        /**
         * Extension hook for adding or overriding properties.
         * 
         * @param properties
         *            The configured properties.
         *            
         * @return The properties that will be written using
         *         {@link #writeProperties(Writer)}
         */
        protected Properties getProperties(Properties properties) {
            
            return properties;
            
        }
        
        /**
         * Writes the <code>exporter</code> entry. This object is used to
         * export the service proxy. The choice here effects the protocol that
         * will be used for communications between the clients and the service.
         * <p>
         * Note: specify the JVM property [-Dcom.sun.jini.jeri.tcp.useNIO=true]
         * to enable NIO.
         * 
         * FIXME the [exporter] is hardwired. Its value is used by the
         * {@link AbstractServer} to export the proxy for the service.
         * <p>
         * Note: There are also hardwired exporters used by the
         * {@link JiniFederation}. The whole issue needs to be resolved. The
         * exported is a chunk of code, so it would have to be quoted to get
         * passed along, which is why I am doing it this way.
         */
        protected void writeExporter(Writer out) throws IOException {
            
            out.write("\nexporter = new " + BasicJeriExporter.class.getName()
                    + "(" + TcpServerEndpoint.class.getName()
                    + ".getInstance(0)," + "new "
                    + BasicILFactory.class.getName() + "());\n");

        }
        
        /**
         * Writes the <code>serviceIdFile</code> entry. The value of that
         * entry is the {@link File} on which the {@link ServiceId} will be
         * written by the service once it is assigned by jini.
         * <p>
         * Note: the serviceUUID (a conversion of the {@link ServiceId} to a
         * normal {@link UUID}) is also put into the znode data for the service
         * once it has been assigned by jini. That action is performed by the
         * service itself.
         */
        protected void writeServiceIdFile(Writer out) throws IOException {

            final File serviceIdFile = new File(serviceDir, "service.id");

            out.write("\nserviceIdFile = new File("
                    + q(serviceIdFile.toString()) + ");\n");

        }

        /**
         * Writes the <code>zpath</code> for the logical service. The service
         * must use {@link CreateMode#EPHEMERAL_SEQUENTIAL} to create a child of
         * this zpath to represent itself.
         * 
         * @throws IOException
         */
        protected void writeLogicalServiceZPath(Writer out) throws IOException {

            out
                    .write("\nlogicalServiceZPath=" + q(logicalServiceZPath)
                            + ";\n");
               
        }

        /**
         * Writes the {@link ZookeeperClientConfig} into the configuration file.
         * 
         * @throws IOException
         */
        protected void writeZookeeperClientConfig(Writer out)
                throws IOException {

            out.write("\n");
            
            fed.getZooConfig().writeConfiguration(out);
            
        }
        
        @Override
        protected V newProcessHelper(String className,
                ProcessBuilder processBuilder, IServiceListener listener)
                throws IOException {

            return (V) new JiniServiceProcessHelper(className, processBuilder, listener);

        }
        
        /**
         * Overriden to monitor for the jini join of the service and the
         * creation of the znode corresponding to the physical service instance.
         * 
         * @todo we could also verify the service using its proxy, e.g., by
         *       testing for a normal run state.
         */
        @Override
        protected void awaitServiceStart(final V processHelper,
                final long timeout, final TimeUnit unit) throws Exception,
                TimeoutException, InterruptedException {

            final long begin = System.nanoTime();
            
            long nanos = unit.toNanos(timeout);
            
            // wait for the service to be discovered
            final ServiceItem serviceItem = awaitServiceDiscoveryOrDeath(
                    processHelper, nanos, TimeUnit.NANOSECONDS);

            // proxy will be used for destroy().
            processHelper.setServiceItem(serviceItem);
            
            // subtract out the time we already waited.
            nanos -= (System.nanoTime() - begin);

            // wait for the ephemeral znode for the service to be created
            awaitZNodeCreatedOrDeath(serviceItem, processHelper,
                    nanos, TimeUnit.NANOSECONDS);

        }

        /**
         * Waits up to timeout units for the service to either by discovered by
         * jini or to die.
         * <p>
         * Note: We recognize the service by the present of the assigned
         * {@link ServiceToken} attribute. If a service with that
         * {@link ServiceToken} can not be discovered by jini after a timeout,
         * then we presume that the service could not start and throw an
         * exception. The {@link ServiceToken} provides an attribute which is
         * assigned by the service starter while the {@link ServiceId} is
         * assigned by jini only after the service has joined with a jini
         * registrar.
         * 
         * @param processHelper
         * @param timeout
         * @param unit
         * @return The {@link ServiceItem} for the discovered service.
         * @throws Exception
         */
        protected ServiceItem awaitServiceDiscoveryOrDeath(
                final ProcessHelper processHelper, long timeout,
                final TimeUnit unit) throws Exception, TimeoutException,
                InterruptedException {
            
            // convert to ms for jini lookup() waitDur.
            timeout = unit.toMillis(timeout);
            
            final long begin = System.currentTimeMillis();
            
            ServiceDiscoveryManager serviceDiscoveryManager = null;
            try {

                serviceDiscoveryManager = new ServiceDiscoveryManager(fed
                        .getDiscoveryManagement(), new LeaseRenewalManager());

                if(INFO)
                    log.info("Awaiting service discovery: "
                            + processHelper.name);
                
                final ServiceItem[] items = serviceDiscoveryManager
                        .lookup(
                                new ServiceTemplate(null/* serviceID */,
                                        null/* iface[] */,
                                        new Entry[] { new ServiceToken(
                                                serviceToken)
                        }), // template
                        1, // minMatches
                        1, // maxMatches
                        null, // filter
                        timeout//
                        );

                final long elapsed = System.currentTimeMillis() - begin;

                if (items.length == 0) {

                    throw new Exception("Service did not start: elapsed="
                            + elapsed + ", name=" + serviceName);

                }

                if (items.length != 1) {

                    throw new Exception("Duplicate ServiceTokens? name="
                            + serviceName + ", found=" + Arrays.toString(items));

                }

                if (INFO)
                    log.info("Discovered service: elapsed=" + elapsed
                            + ", name=" + processHelper.name + ", item="
                            + items[0]);

                return items[0];
                
            } finally {

                if (serviceDiscoveryManager != null) {

                    serviceDiscoveryManager.terminate();
                    
                }

            }

        }
        
        /**
         * Waits up to timeout units for the znode for the phsyical service to
         * be created or the process to die.
         * 
         * @param processHelper
         * @param timeout
         * @param unit
         * 
         * @throws TimeoutException
         * @throws InterruptedException
         * @throws KeeperException
         */
        public void awaitZNodeCreatedOrDeath(final ServiceItem serviceItem,
                final ProcessHelper processHelper, final long timeout,
                final TimeUnit unit) throws KeeperException,
                InterruptedException, TimeoutException {

            // convert to a standard UUID.
            final UUID serviceUUID = JiniUtil.serviceID2UUID(serviceItem.serviceID);
            
            // this is the zpath that the service will create.
            final String physicalServiceZPath = logicalServiceZPath + "/"
                    + BigdataZooDefs.PHYSICAL_SERVICES_CONTAINER + "/" + serviceUUID;

            if (!ZNodeCreatedWatcher.awaitCreate(zookeeper,
                    physicalServiceZPath, timeout, unit)) {

                throw new TimeoutException("zpath does not exist: "
                        + physicalServiceZPath);

            }

            if (INFO)
                log.info("znode exists: zpath=" + physicalServiceZPath);

            // success.
            return;
            
        }

    }

    public static String[] getJiniOptions(String className, Configuration config)
            throws ConfigurationException {

        return getStringArray(Options.JINI_OPTIONS, className, config,
                new String[0]);

    }
}

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

package com.bigdata.jini.start;

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
import java.util.List;
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
import net.jini.discovery.LookupDiscovery;
import net.jini.entry.AbstractEntry;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.entry.Comment;
import net.jini.lookup.entry.Name;

import org.apache.zookeeper.CreateMode;

import sun.security.jca.ServiceId;

import com.bigdata.service.jini.JiniFederation;
import com.bigdata.zookeeper.ZookeeperClientConfig;

/**
 * Abstract implementation for jini-based services.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractJiniServiceConfiguration extends
        JavaServiceConfiguration {

    /**
     * Additional options understood by the
     * {@link AbstractJiniServiceConfiguration}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options extends JavaServiceConfiguration.Options {
     
        /**
         * {@link Entry}[] attributes used to describe the service.
         * <p>
         * Note: A {@link Name} and {@link Hostname} will be automatically added
         * to this array using the class and znode of the service instance. The
         * {@link Name} will be a <strong>canonical</strong> service name. It
         * is a good idea not to specify additional service names as something
         * might break :-).
         * 
         * @todo this will have to be handled specially for the
         *       {@link ServicesManager} instances.
         */
        String ENTRIES = "entries";

        /**
         * A {@link String}[] whose values are the group(s) to be used for
         * discovery (no default). Note that multicast discovery is always used
         * if {@link LookupDiscovery#ALL_GROUPS} (a <code>null</code>) is
         * specified.
         */
        String GROUPS = "groups";

        /**
         * One or more {@link LookupLocator}s specifying unicast URIs of the
         * form <code>jini://host/</code> or <code>jini://host:port/</code>
         * (no default). This MAY be an empty array if you want to use multicast
         * discovery <strong>and</strong> you have specified {@link #GROUPS} as
         * {@link LookupDiscovery#ALL_GROUPS} (a <code>null</code>). .
         */
        String LOCATORS = "locators";

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
    public final String[] jiniOptions;
    
    protected void toString(StringBuilder sb) {

        super.toString(sb);

        sb.append(", entries=" + Arrays.toString(entries));

        sb.append(", groups=" + Arrays.toString(groups));
        
        sb.append(", locators=" + Arrays.toString(locators));

        sb.append(", jiniOptions=" + Arrays.toString(jiniOptions));

    }
    
    /**
     * @param cls
     * @param config
     * @throws ConfigurationException
     */
    public AbstractJiniServiceConfiguration(Class cls, Configuration config)
            throws ConfigurationException {
 
        super(cls, config);
        
        entries = getEntries(cls.getName(), config);

        groups = getGroups(cls.getName(), config);
        
        locators = getLocators(cls.getName(), config);
        
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
    protected class JiniServiceStarter<V> extends JavaServiceStarter<V> {
        
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
         * as an argument to the java class whose main routine will be invoked.
         */
        protected List<String> getCommandLine() {
            
            final List<String> cmds = super.getCommandLine();

            cmds.add(configFile.toString());

            return cmds;
            
        }
        
        /**
         * Extended to add the {@link Options#JINI_OPTIONS}.
         */
        protected void addServiceOptions(List<String> cmds) {

            super.addServiceOptions(cmds);
            
            for (String arg : jiniOptions) {

                cmds.add(arg);

            }

        }

        /**
         * Extended to write the configuration file in the service directory.
         */
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

            // AdvertDescription.
            out.write("\n\nAdvertDescription {\n");
            {

                writeEntries(out);
                
                writeGroups(out);
                
                writeLocators(out);
                
            }
            out.write("}\n");

            // @todo use the className for the component?
            out.write("\n\nServiceDescription {\n");
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

            final Entry[] entries = getEntries(AbstractJiniServiceConfiguration.this.entries);

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

            out.write("\nlocators=new String[]{\n");

            for (LookupLocator e : locators) {

                out.write("new LookupLocator(\"" + e.getHost() + "\","
                        + e.getPort() + "\n");

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

        }

        /**
         * Writes the <code>exporter</code> entry. This object is used to
         * export the service proxy. The choice here effects the protocol that
         * will be used for communications between the clients and the service.
         * <p>
         * Note: specify the JVM property [-Dcom.sun.jini.jeri.tcp.useNIO=true]
         * to enable NIO.
         */
        protected void writeExporter(Writer out) throws IOException {
            
            out
                    .write("\nexporter = new BasicJeriExporter(TcpServerEndpoint.getInstance(0),\n"
                            + "new BasicILFactory());\n");

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
        
        /**
         * Overriden to monitor for the join of the service. If the services
         * DOES NOT join after a timeout then we kill the process. We recognize
         * the service by the present of the assigned {@link ServiceToken}
         * attribute. If a service with that {@link ServiceToken} attribute can
         * not be discovered by jini after a timeout, then we presume that the
         * service could not start and throw an exception.
         * 
         * @todo Also monitor for the create of the physicalServiceZNode, which
         *       should include the serviceUUID and perhaps the serviceToken?
         */
        protected void awaitServiceStart(final ProcessHelper processHelper)
                throws Exception {
           
            // @todo config timeout. 60s?
            final long timeout = TimeUnit.SECONDS.toNanos(60L);
            
            final long begin = System.currentTimeMillis();
            
            /*
             * Set a thread that will interrupt the service discovery if it
             * notices that the process has died. This keeps us from waiting up
             * to the timeout for a process which does not start normally.
             */
            processHelper.interruptWhenProcessDies(timeout,
                    TimeUnit.NANOSECONDS);
            
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

            } catch(InterruptedException ex) {
                
//                if (!processHelper.isRunning()) {
//                 
//                    throw new IOException("Process is dead: exitValue="
//                            + processHelper.exitValue());
//                    
//                }
                
                /*
                 * If we were interrupted because the process is dead then
                 * add that information to the exception.
                 */
                try {
                
                    final int exitValue = processHelper.exitValue(10,
                            TimeUnit.MILLISECONDS);
                    
                    throw new IOException("Process is dead: exitValue="
                            + exitValue);
                
                } catch(TimeoutException ex2) {
                    
                    // ignore.
                    
                }
                
                // otherwise just rethrow the exception.
                throw ex;
                
            } finally {

                if (serviceDiscoveryManager != null) {

                    serviceDiscoveryManager.terminate();
                    
                }

            }

        }
        
    }

    public static Entry[] getEntries(String className, Configuration config)
            throws ConfigurationException {

        final Entry[] a = (Entry[]) config.getEntry(Options.NAMESPACE,
                Options.ENTRIES, Entry[].class, new Entry[] {});
        
        final Entry[] b = (Entry[]) config.getEntry(className, Options.ENTRIES,
                Entry[].class, new Entry[] {});
        
        return concat(a,b);
                
    }

    // @todo default ALL_GROUPS?
    public static String[] getGroups(String className, Configuration config)
            throws ConfigurationException {

        return (String[]) config.getEntry(Options.NAMESPACE, Options.GROUPS,
                String[].class);

    }

    // @todo default empty LookupLocator[]?
    public static LookupLocator[] getLocators(String className,
            Configuration config) throws ConfigurationException {

        return (LookupLocator[]) config.getEntry(Options.NAMESPACE,
                Options.LOCATORS, LookupLocator[].class);

    }

    public static String[] getJiniOptions(String className, Configuration config)
            throws ConfigurationException {

        return getStringArray(Options.JINI_OPTIONS, className, config,
                new String[0]);

    }
}

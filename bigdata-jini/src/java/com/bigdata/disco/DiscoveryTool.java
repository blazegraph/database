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
package com.bigdata.disco;

import com.bigdata.util.Util;

import com.sun.jini.config.Config;
import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;
import net.jini.core.constraint.MethodConstraints;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.discovery.ConstrainableLookupLocator;
import net.jini.discovery.DiscoveryManagement;
import net.jini.discovery.DiscoveryGroupManagement;
import net.jini.discovery.DiscoveryLocatorManagement;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryEvent;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.ServiceItemFilter;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.synchronizedMap;
import static net.jini.discovery.Constants.discoveryPort;
import static net.jini.discovery.DiscoveryGroupManagement.ALL_GROUPS;

import java.io.IOException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DiscoveryTool {

    private static final String COMPONENT_NAME = DiscoveryTool.class.getName();
    private static final String CONFIG_ARG = "config";
    private static final String GROUP_ARG_PREFIX = "group";
    private static final String LOCATOR_ARG_PREFIX = "locator";
    private static final String SERVICE_ARG_PREFIX = "service";
    private static final String ALL_GROUPS_ARG = "allGroups";
    private static final String TIMEOUT_ARG = "timeout";
    private static final String OUTPUT_ARG = "output";
    private static final String VERBOSE_ARG = "verbose";
    private static final Pattern hostPortPattern =
        Pattern.compile("(.+):(\\d+)");

    public static void main(String[] args) throws Exception {
        new DiscoveryTool(args).run();
    }

    private final Map<Class, Collection<Class>> typeCache =
        synchronizedMap(new WeakHashMap<Class, Collection<Class>>());
    private final Configuration config;
    private final Collection<String> groups = new ArrayList<String>();
    private final Collection<LookupLocator> locators =
        new ArrayList<LookupLocator>();
    private final Collection<Pattern> serviceTypePatterns =
        new ArrayList<Pattern>();
    private final boolean allGroups;
    private final Long timeout;
    private final PrintStream out;
    private final boolean verbose;
    private final DateFormat dateFormat;
    private final Set<String> uninterestingInterfaces;

    private DiscoveryTool(String[] rawArgs) 
        throws ConfigurationException, IOException
    {
        Arguments args = new Arguments(rawArgs);

        config = ConfigurationProvider.getInstance
                     ( new String[]{ args.get(CONFIG_ARG) },
                       (this.getClass()).getClassLoader() );

        for (int i = 0; ; i++) {
            String argName = GROUP_ARG_PREFIX + i;
            if (!args.contains(argName)) {
                break;
            }
            groups.add(args.get(argName));
        }

        MethodConstraints locatorConstraints = 
            (MethodConstraints)config.getEntry(COMPONENT_NAME,
                                               "lookupLocatorConstraints",
                                               MethodConstraints.class,
                                               null);
        for (int i = 0; ; i++) {
            String argName = LOCATOR_ARG_PREFIX + i;
            if (!args.contains(argName)) {
                break;
            }
            String val = args.get(argName);
            Matcher m = hostPortPattern.matcher(val);
            locators.add(m.matches() ?
                new ConstrainableLookupLocator(
                    m.group(1), parseInt(m.group(2)), locatorConstraints) :
                new ConstrainableLookupLocator(
                    val, discoveryPort, locatorConstraints));
        }

        for (int i = 0; ; i++) {
            String argName = SERVICE_ARG_PREFIX + i;
            if (!args.contains(argName)) {
                break;
            }
            serviceTypePatterns.add(Pattern.compile(args.get(argName)));
        }

        timeout = args.contains(TIMEOUT_ARG) ?
            parseLong(args.get(TIMEOUT_ARG)) : null;

        if (args.contains(OUTPUT_ARG)) {
            String val = args.get(OUTPUT_ARG);
            out = val.equals("-") ? System.out : new PrintStream(val);
        } else {
            out = System.out;
        }

        allGroups = parseBoolean(args.get(ALL_GROUPS_ARG));
        verbose = parseBoolean(args.get(VERBOSE_ARG));

        dateFormat = (DateFormat)Config.getNonNullEntry
                         (config, COMPONENT_NAME, "dateFormat",
                          DateFormat.class, DateFormat.getInstance());

        uninterestingInterfaces = 
            new HashSet<String>(asList((String[])Config.getNonNullEntry
                                                    (config, COMPONENT_NAME, 
                                                     "uninterestingInterfaces",
                                                     String[].class, 
                                                     new String[0])));
    }

    void run()
        throws ConfigurationException, IOException, InterruptedException
    {
        DiscoveryManagement discoveryManager = null;
        ServiceDiscoveryManager serviceDiscovery = null;

        try {
            println(getDateString(), ": starting discovery");

            discoveryManager = 
                Util.getDiscoveryManager(config, COMPONENT_NAME);

            verbose("created lookup discovery manager: ", discoveryManager);

            verbose("groups: ", allGroups ? "<all>" : groups.toString());
            ((DiscoveryGroupManagement) discoveryManager).setGroups(
                allGroups ? 
                    ALL_GROUPS : groups.toArray(new String[groups.size()]));
            verbose("locators: ", locators);
            ((DiscoveryLocatorManagement) discoveryManager).setLocators(
                locators.toArray(new LookupLocator[locators.size()]));

            serviceDiscovery = 
                new ServiceDiscoveryManager(discoveryManager, null, config);
            verbose("created service discovery manager: ", serviceDiscovery);

            verbose("service type patterns: ", serviceTypePatterns);
            ServiceItemFilter serviceFilter = serviceTypePatterns.isEmpty() ?
                null : new ServiceTypeFilter(serviceTypePatterns);
            LookupCache cache = serviceDiscovery.createLookupCache(
                null, serviceFilter, new Listener());
            verbose("created lookup cache: ", cache);

            verbose(
                "timeout: ", (timeout != null) ? (timeout + " ms") : "<none>");
            if (timeout != null) {
                sleep(timeout);
            } else {
                while (true) {
                    sleep(Long.MAX_VALUE);
                }
            }

            verbose("shutting down");

        } finally {
            if (serviceDiscovery != null) {
                serviceDiscovery.terminate();
            }
            if (discoveryManager != null) {
                discoveryManager.terminate();
            }
        }
    }

    void println(Object... args) {
        StringBuilder sb = new StringBuilder();
        for (Object arg : args) {
            sb.append(arg);
        }
        out.println(sb);
    }

    void verbose(Object... args) {
        if (verbose) {
            println(args);
        }
    }

    String getDateString() {
        return dateFormat.format(new Date());
    }

    Collection<Class> getTypes(Object obj) {
        if (obj == null) {
            return emptyList();
        }
        Class cl = obj.getClass();
        Collection<Class> types = typeCache.get(cl);
        if (types == null) {
            Set<Class> s = new HashSet<Class>();
            accumulateTypes(cl, s);
            types = new ArrayList<Class>(s);
            typeCache.put(cl, types);
        }
        return types;
    }

    private static void accumulateTypes(Class cl, Set<Class> types) {
        if (cl != null && types.add(cl)) {
            accumulateTypes(cl.getSuperclass(), types);
            for (Class intf : cl.getInterfaces()) {
                accumulateTypes(intf, types);
            }
        }
    }

    private static String join(Iterable<?> i, String delim) {
        Iterator<?> iter = i.iterator();
        if (!iter.hasNext()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(iter.next());
        while (iter.hasNext()) {
            sb.append(delim);
            sb.append(iter.next());
        }
        return sb.toString();
    }

    private class Listener implements ServiceDiscoveryListener {

        Listener() {
        }

        public synchronized void serviceAdded(ServiceDiscoveryEvent event) {
            println(getDateString(), ": service added");
            printServiceItem(event.getPostEventServiceItem());
        }

        public synchronized void serviceChanged(ServiceDiscoveryEvent event) {
            println(getDateString(), ": service changed");
            printServiceItem(event.getPostEventServiceItem());
        }

        public synchronized void serviceRemoved(ServiceDiscoveryEvent event) {
            println(getDateString(), ": service removed");
            printServiceItem(event.getPreEventServiceItem());
        }

        private void printServiceItem(ServiceItem item) {
            Collection<String> typeNames = new ArrayList<String>();
            for (Class type : getTypes(item.service)) {
                if (type.isInterface() &&
                    !uninterestingInterfaces.contains(type.getName()))
                {
                    typeNames.add(type.getName());
                }
            }

            println("  Service ID: ", item.serviceID);
            println("  Types: ", join(typeNames, ", "));
            println("  Attributes:");
            for (Entry e : item.attributeSets) {
                println("    ", e);
            }
            println();
        }
    }

    private class ServiceTypeFilter implements ServiceItemFilter {

        private final Collection<Pattern> serviceTypePatterns;

        ServiceTypeFilter(Collection<Pattern> serviceTypePatterns) {
            this.serviceTypePatterns = serviceTypePatterns;
        }

        public boolean check(ServiceItem item) {
            for (Class type : getTypes(item.service)) {
                for (Pattern pattern : serviceTypePatterns) {
                    if (pattern.matcher(type.getName()).find()) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private static class Arguments {
        
        private static final Pattern pattern = Pattern.compile("(.+)=(.+)");
        private final Map<String, String> map = new HashMap<String, String>();
        
        Arguments(String[] rawArgs) {
            for (String arg : rawArgs) {
                Matcher m = pattern.matcher(arg);
                if (!m.matches()) {
                    throw new IllegalArgumentException(
                        "invalid argument: " + arg);
                }
                map.put(m.group(1), m.group(2));
            }
        }

        boolean contains(String name) {
            return map.containsKey(name);
        }
        
        String get(String name) {
            String val = map.get(name);
            if (val != null) {
                return val;
            } else {
                throw new IllegalArgumentException("no value for " + name);
            }
        }
    }
}

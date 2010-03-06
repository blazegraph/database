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

package com.bigdata.service.jini.util;

import com.bigdata.util.config.ConfigurationUtil;
import com.bigdata.util.config.LogUtil;
import com.bigdata.util.config.NicUtil;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.sun.jini.admin.DestroyAdmin;
import com.sun.jini.start.NonActivatableServiceDescriptor;
import com.sun.jini.start.NonActivatableServiceDescriptor.Created;
import net.jini.admin.Administrable;
import net.jini.config.AbstractConfiguration;
import net.jini.config.ConfigurationException;
import net.jini.discovery.DiscoveryManagement;
import net.jini.discovery.DiscoveryListener;
import net.jini.discovery.DiscoveryEvent;
import net.jini.discovery.LookupDiscoveryManager;
import net.jini.security.BasicProxyPreparer;
import net.jini.security.ProxyPreparer;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceRegistrar;

import java.rmi.RMISecurityManager;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Class that can be used to start a Jini lookup service from the command
 * line, from a shell or ant script, or from within program control.
 */
public class LookupStarter extends Thread {

    private static final org.apache.log4j.Logger logger = 
        LogUtil.getLog4jLogger( LookupStarter.class );

    private String pSep = System.getProperty("path.separator");
    private String fSep = System.getProperty("file.separator");
    private String userDir = System.getProperty("user.dir");

    //set on command line
    private String appHome = System.getProperty("app.home");
    private String jiniLib = System.getProperty("jini.lib");
    private String jiniLibDl = System.getProperty("jini.lib.dl");
    private String localPolicy = System.getProperty("java.security.policy");

    private static String thisHost = NicUtil.getIpAddress("eth0");
    private static String defaultGroup = "bigdata.fedname-"+thisHost;
    private static String defaultCodebasePort = "23333";

    private static String group = 
        System.getProperty("bigdata.fedname", defaultGroup);
    private static String codebasePortStr = 
        System.getProperty("codebase.port", defaultCodebasePort);
    private static int codebasePort = Integer.parseInt(codebasePortStr);

    private static String[] groups = new String[] { group };
    private static String overrideGroups;
    private static LookupLocator[] locators = new LookupLocator[0];
    private static DiscoveryManagement ldm;

    private static String jskCodebase;
    private static String lookupServerCodebase;
    private static String lookupCodebase;
    private String lookupClasspath = jiniLib+fSep+"reggie.jar";
    private String lookupImplName =
                                "com.sun.jini.reggie.TransientRegistrarImpl";
    private String lookupConfig = appHome
                                  +fSep+"bigdata-jini"+fSep+"src"+fSep+"java"
                                  +fSep+"com"+fSep+"bigdata"+fSep+"service"
                                  +fSep+"jini"+fSep+"util"+fSep+"config"
                                  +fSep+"lookup.config";
    private static HashSet<ServiceRegistrar> proxySet = 
        new HashSet<ServiceRegistrar>();
    private static HashSet<Created> implRefSet = new HashSet<Created>();

    public static void main( String[] args ) {
        boolean stop = false;
        for (int i = 0; i < args.length ; i++ ) {
            String arg = args[i];
            if (arg.equals("-help")) {
                System.out.println
                          ("usage: java -jar lookupstarter.jar [-help|-stop]");
                return;
            } else if (arg.equals("-stop")) {
                stop = true;
            }
        }
        try {
            if (stop) {
                stopLookupService();
            } else {
                new LookupStarter().start();
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public LookupStarter() {
    }

    public void run() {
        (new LookupStartThread()).start();
    }

    private class LookupStartThread extends Thread {
        public LookupStartThread() {
            super("LookupStartThread");
            setDaemon(false);//stay up past main thread
        }
        public void run() {
            try {
                setupDiscovery();
                startLookupService();
                boolean lookupDiscovered = waitForLookupServiceDiscovery();
                if(!lookupDiscovered) {
                    System.out.println("FAILED to start lookup service");
                    System.exit(-1);//failure
                }
            } catch(Throwable t) {
                t.printStackTrace();
                System.exit(-2);//exception
            }
            synchronized(proxySet) {
                if (proxySet.size() > 0) {
                    System.out.println("lookup service started");
                }
            }
            // Wait for lookup service shutdown before exiting.
            while(true) {
                if( waitForLookupServiceShutdown() ) break;
                delayMS(3*1000);
            }//end loop
            System.exit(0);//successful completion
        }
    }



    private static void setupDiscovery() throws Exception {
        StringBuffer strBuf = null;
        if(groups[0].compareTo("") == 0) {
            strBuf = new StringBuffer("{"+"\"\"");
        } else {
            strBuf = new StringBuffer("{"+"\""+groups[0]+"\"");
        }//endif
        for(int i=1;i<groups.length;i++) {
            if(groups[i].compareTo("") == 0) {
                strBuf.append(", "+"\"\"");
            } else {
                strBuf.append(", ").append("\""+groups[i]+"\"");
            }//endif
        }//end loop
        strBuf.append("}");
        overrideGroups = strBuf.toString();

        ldm = new LookupDiscoveryManager(groups, locators, null);
        logger.log(Level.INFO, "groups="+writeGroupArrayToString(groups)
                   +", locators="+writeArrayElementsToString(locators));
        jskCodebase = ConfigurationUtil.computeCodebase
                          (thisHost, "jsk-dl.jar", codebasePort);
        lookupServerCodebase = ConfigurationUtil.computeCodebase
                                   (thisHost, "reggie-dl.jar", codebasePort);
        lookupCodebase = lookupServerCodebase+" "+jskCodebase;

        // Turn on Lookup discovery in ldm
        ldm.addDiscoveryListener(new LookupDiscoveryListener());
    }//end setupDiscovery

    private void startLookupService() throws Exception {
        String joinGroupsOverride = 
        "com.sun.jini.reggie.initialLookupGroups=new String[] "+overrideGroups;
        String memberGroupsOverride = 
        "com.sun.jini.reggie.initialMemberGroups=new String[] "+overrideGroups;

        logger.log(Level.INFO,"Start lookup service "
                   +"["+memberGroupsOverride+"]");
        String[] lookupArgsArray = new String[] { lookupConfig, 
                                                  joinGroupsOverride,
                                                  memberGroupsOverride };
        NonActivatableServiceDescriptor lookupDescriptor =
            new NonActivatableServiceDescriptor(lookupCodebase,
                                                localPolicy,
                                                lookupClasspath,
                                                lookupImplName,
                                                lookupArgsArray);
        ServiceStarterConfig starterConfig = 
            new ServiceStarterConfig(new BasicProxyPreparer());

        implRefSet.add( (Created)lookupDescriptor.create(starterConfig) );
    }

    private static void stopLookupService() throws Exception {
        if(System.getSecurityManager() == null) {
            System.setSecurityManager(new RMISecurityManager());
        }
        setupDiscovery();
        if(!waitForLookupServiceDiscovery()) {
            System.out.println("FAILED to discover lookup service");
            System.exit(0);
        }
        HashSet<ServiceRegistrar> proxySetClone;
        synchronized(proxySet) {
            proxySetClone = (HashSet<ServiceRegistrar>)(proxySet.clone());
        }
        for (Iterator<ServiceRegistrar> itr = proxySetClone.iterator();
                                                             itr.hasNext(); )
        {
            ServiceRegistrar srvcReg = itr.next();
            if(srvcReg == null) continue;
            if(srvcReg instanceof Administrable) {
                try {
                    Object srvcRegAdmin  = ((Administrable)srvcReg).getAdmin();
                    if(srvcRegAdmin instanceof DestroyAdmin) {
                        try {
                            ((DestroyAdmin)srvcRegAdmin).destroy();
                            logger.log(Level.INFO,"destroyed lookup service");
                        } catch(Throwable t) { 
                            logger.log(Level.WARN, "exception on lookup "
                                       +"service destroy", t);
                        }
                    } else {
                        logger.log(Level.WARN, "on shutdown - lookup service "
                               +"admin not instance of DestroyAdmin");
                    }
                } catch(Throwable t) { 
                    logger.log(Level.WARN, "getAdmin exception on lookup "
                               +"service shutdown ["+t+"]", t);
                }
            } else {
                logger.log(Level.WARN, "on shutdown - lookup service not "
                           +"instance of Administrable");
            }
        }
    }

    private static boolean waitForLookupServiceDiscovery() throws Exception {

        ServiceRegistrar[] regs = ldm.getRegistrars();//initial query for lus
        if (regs.length > 0) {
            for(int i=0; i<regs.length; i++) {
                synchronized(proxySet) {
                    proxySet.add(regs[i]);
                }
            }
        }
        // Wait for lookup service to be discovered
        int nSecs = 30;
        int nExpected = 1;
        logger.log(Level.INFO, "waiting "+nSecs+" seconds for lookup "
                   +"service discovery");
        for (int i=0; i<nSecs; i++) {
            delayMS(1*1000);
            synchronized(proxySet) {
                if(proxySet.size() < nExpected) continue;
            }
            return true;
        }
        // If at least one, but less than the expected number of lookup
        // services were started, still return true, but log a WARNING,
        // so that this program will enter its wait-for-shutdown-loop;
        // and so that when this program is run again with the '-stop'
        // argument, it will not time out.
        int nLus = 0;
        synchronized(proxySet) {
            nLus = proxySet.size();
        }
        boolean retVal = true;
        if(nLus >= nExpected) {
            logger.log(Level.INFO, "discovered = "+nLus+" lookup service(s)");
        } else if (nLus > 0) {
            logger.log(Level.WARN, "discovered less than expected number of "
                       +"lookup services [discovered="+nLus+", "
                       +"expected="+nExpected+"]");
            retVal = false;
        } else {
            logger.log(Level.WARN, "NO lookup services discovered");
            retVal = false;
        }//endif
        return retVal;
    }

    private boolean waitForLookupServiceShutdown() {
        int nSecs = 30;
        for (int i=0; i<nSecs; i++) {
            delayMS(1*1000);
            synchronized(proxySet) {
                if(proxySet.size() > 0) continue;
            }
            return true;
        }
        return false;
    }

    private static void delayMS(long nMS) {
        try {
            Thread.sleep(nMS);
        } catch (InterruptedException e) { }
    }

    public static String writeGroupArrayToString(String[] groups) {
        if(groups == null) return new String("[ALL_GROUPS]");
        if(groups.length <= 0) return new String("[]");
        StringBuffer strBuf = null;
        if(groups[0].compareTo("") == 0) {
            strBuf = new StringBuffer("[The PUBLIC Group");
        } else {
            strBuf = new StringBuffer("["+groups[0]);
        }
        for(int i=1;i<groups.length;i++) {
            if( groups[i].compareTo("") == 0 ) {
                strBuf.append(", The PUBLIC Group");
            } else {
                strBuf.append(", ").append(groups[i]);
            }
        }
        strBuf.append("]");
        return strBuf.toString();
    }

    public static String writeArrayElementsToString(Object[] arr) {
        if(arr == null) return new String("[]");
        if(arr.length <= 0) return new String("[]");
        StringBuffer strBuf = new StringBuffer("["+arr[0]);
        for (int i=1;i<arr.length;i++) {
            strBuf.append(", ").append(arr[i]);
        }
        strBuf.append("]");
        return strBuf.toString();
    }


    private static class LookupDiscoveryListener implements DiscoveryListener {
        public void discovered(DiscoveryEvent evnt) {
            logger.log(Level.INFO, "discovery event ...");
            ServiceRegistrar[] regs = evnt.getRegistrars();
            for (int i=0;i<regs.length;i++) {
                try {
                    LookupLocator loc = regs[i].getLocator();
                    String[] groups = regs[i].getGroups();
                    logger.log(Level.INFO, "discovered locator   = "+loc);
                    for (int j=0;j<groups.length;j++) {
                        logger.log(Level.INFO, "discovered groups["
                                   +j+"] = "+groups[j]);
                    }
                    synchronized(proxySet) {
                        proxySet.add(regs[i]);
                    }
                } catch(Throwable e) {
                    e.printStackTrace();
                }
            }
        }

        public void discarded(DiscoveryEvent evnt) {
            logger.log(Level.INFO, "discarded event ...");
            ServiceRegistrar[] regs = evnt.getRegistrars();
            for (int i=0;i<regs.length;i++) {
                synchronized(proxySet) {
                    proxySet.remove(regs[i]);
                }
            }
        }
    }

    private class ServiceStarterConfig extends AbstractConfiguration {

        private String sdComponent = "com.sun.jini.start";
        private String sdEntryName = "servicePreparer";
        private Class  sdType      = ProxyPreparer.class;
        private ProxyPreparer proxyPreparer;

        ServiceStarterConfig(ProxyPreparer proxyPreparer) {
            this.proxyPreparer = proxyPreparer;
        }

        protected Object getEntryInternal(String component,
                                          String name,
                                          Class  type,
                                          Object data)
                                                 throws ConfigurationException
        {
            if( (component == null) || (name == null) || (type == null) ) {
                throw new NullPointerException("component, name and type "
                                               +"cannot be null");
            }
            if(    (sdComponent.equals(component))
                && (sdEntryName.equals(name))
                && (sdType.isAssignableFrom(type)) )
            {
                return proxyPreparer;
            } else {
                throw new ConfigurationException("entry not found for "
                                                 +"component "+component
                                                 +", name " + name);
            }
        }
    }

}

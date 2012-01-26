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
 * Created on Mar 24, 2008
 */

package com.bigdata;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Date;
import java.util.Formatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.system.SystemUtil;

import com.bigdata.Depends.Dependency;
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.util.InnerCause;

/**
 * Class has a static method which writes a copyright banner on stdout once per
 * JVM. This method is invoked from several core classes in order to ensure that
 * the copyright banner is always written out on bigdata startup.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Banner {

    /**
     * The logger for <em>this</em> class.
     */
    private static final Logger log = Logger.getLogger("com.bigdata.Banner");

    private final static AtomicBoolean didBanner = new AtomicBoolean(false);

    /**
     * Environment variables understood by the {@link Banner} class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options {

        /**
         * This may be used to suppress the banner text. 
         */
        String QUIET = "com.bigdata.Banner.quiet";

        /**
         * This may be used to disable JMX MBeans which self-report on the log4j
         * properties.
         */
        String LOG4J_MBEANS_DISABLE = "com.bigdata.jmx.log4j.disable";

    }
    
    static public void banner() {
        
        if(didBanner.compareAndSet(false/*expect*/, true/*update*/)) {
        
            final boolean quiet = Boolean.getBoolean(Options.QUIET);

            if (!quiet) {

                final StringBuilder sb = new StringBuilder(banner);
         
                // Add in the dependencies.
                {
                    int maxNameLen = 0, maxProjectLen = 0, maxLicenseLen = 0;
                    for (Dependency dep : com.bigdata.Depends.depends()) {
                        if (dep.getName().length() > maxNameLen)
                            maxNameLen = dep.getName().length();
                        if (dep.projectURL().length() > maxProjectLen)
                            maxProjectLen = dep.projectURL().length();
                        if (dep.licenseURL().length() > maxLicenseLen)
                            maxLicenseLen = dep.licenseURL().length();
                    }
                    maxNameLen = Math.min(80, maxNameLen);
                    maxProjectLen = Math.min(80, maxProjectLen);
                    maxLicenseLen = Math.min(80, maxLicenseLen);

                    final Formatter f = new Formatter(sb);

                    final String fmt1 = "" //
                            + "%-" + maxNameLen + "s"//
//                            + " %-" + maxProjectLen + "s" //
                            + " %-" + maxLicenseLen + "s"//
                            + "\n";

                    f.format(fmt1, "Dependency", "License");
                    
                    for (Dependency dep : com.bigdata.Depends.depends()) {

                        f.format(fmt1, //
                                dep.getName(),// 
//                                dep.projectURL(),//
                                dep.licenseURL()//
                                );

                    }
                }
                
                System.out.println(sb);
                
            }

            /*
             * If logging is not configured for [com.bigdata] then we set a
             * default log level @ WARN.  This is critical for good performance.
             */
            setDefaultLogLevel(quiet);
            
            /*
             * Note: I have modified this to test for disabled registration and
             * to use reflection in order to decouple the JMX dependency for
             * anzo.
             */
            if (!Boolean.getBoolean(Options.LOG4J_MBEANS_DISABLE)) {

                try {

                    final Class<?> cls = Class
                            .forName("com.bigdata.jmx.JMXLog4jMBeanUtil");

                    final Method m = cls.getMethod("registerLog4jMBeans",
                            new Class[] {});

                    // Optionally register a log4j MBean.
                    m.invoke(null/* obj */);

                    // JMXLog4jMBeanUtil.registerLog4jMBeans();

                } catch (Throwable t) {

                    log.info("Problem registering log4j mbean?", t);

                }

            }
            
        }
        
    }
    
    /**
     * If logging is not configured for [com.bigdata] then we set a default log
     * level @ WARN. This is critical for good performance.
     */
    private static void setDefaultLogLevel(final boolean quiet) {

        final Logger defaultLog = Logger.getLogger("com.bigdata");

        if (defaultLog.getLevel() == null) {

            /*
             * Since there is no default for com.bigdata, default to WARN.
             */
            try {

                defaultLog.setLevel(Level.WARN);

                if (!quiet)
                    log.warn("Defaulting log level to WARN: "
                            + defaultLog.getName());

            } catch (Throwable t) {

                /*
                 * Note: The SLF4J bridge can cause a NoSuchMethodException to
                 * be thrown out of Logger.setLevel(). We trap this exception
                 * and log a message @ ERROR. It is critical that bigdata
                 * logging is properly configured as logging at INFO for
                 * com.bigdata will cause a tremendous loss of performance.
                 * 
                 * @see https://sourceforge.net/apps/trac/bigdata/ticket/362
                 */
                if (InnerCause.isInnerCause(t, NoSuchMethodError.class)) {

                    log.error("Unable to raise the default log level to WARN."
                            + " Logging is NOT properly configured."
                            + " Severe performance penalty will result.");

                } else {

                    // Something else that we are not expecting.
                    throw new RuntimeException(t);

                }

            }

        } // if(log.getLevel() == null)

    }

    /**
     * Use reflection to discover and report on the bigdata build information. A
     * <code>com.bigdata.BuildInfo</code> is built when the JAR is created.
     * However, it may not be present when running under an IDE from the source
     * code and, therefore, there MUST NOT be any compile time references to the
     * <code>com.bigdata.BuildInfo</code> class. This method uses reflection to
     * avoid a compile time dependency.
     * <p>
     * Note: This method works fine. However, the problem with exposing the
     * information is that people running from an IDE can observe <em>stale</em>
     * data from old <code>com.bigdata.BuildInfo</code> class files left from a
     * previous build of a JAR. This makes the information good for deployed
     * versions of the JAR but potentially misleading when people are running
     * under an IDE.
     * 
     * @return Build info metadata iff available.
     */
    private synchronized static Map<String,String> getBuildInfo() {

        if (buildInfoRef.get() == null) {

            final Map<String,String> map = new LinkedHashMap<String, String>();
            
            try {
            
                final Class<?> cls = Class.forName("com.bigdata.BuildInfo");
                
                for (Field f : cls.getFields()) {

                    final String name = f.getName();

                    final int mod = f.getModifiers();

                    if (!Modifier.isStatic(mod))
                        continue;

                    if (!Modifier.isPublic(mod))
                        continue;

                    if (!Modifier.isFinal(mod))
                        continue;

                    try {

                        final Object obj = f.get(null/* staticField */);

                        if (obj != null) {

                            map.put(name, "" + obj);
                            
                        }
                        
                    } catch (IllegalArgumentException e) {

                        log.warn("Field: " + name + " : " + e);
                        
                    } catch (IllegalAccessException e) {
                        
                        log.warn("Field: " + name + " : " + e);
                        
                    }

                }

            } catch (ClassNotFoundException e) {

                log.warn("Not found: " + "com.bigdata.BuildInfo");

            } catch (Throwable t) {

                log.error(t, t);

            }

            // set at most once.
            buildInfoRef.compareAndSet(null/* expect */,
                    Collections.unmodifiableMap(map)/* update */);

        }
        
        return buildInfoRef.get();
        
    }

    private final static AtomicReference<Map<String, String>> buildInfoRef = new AtomicReference<Map<String, String>>();
    
    private final static String getBuildString() {

        if (getBuildInfo().isEmpty())
            return "";

        final StringBuilder s = new StringBuilder();

        s.append("\nbuildVersion=" + getBuildInfo().get("buildVersion"));

//        s.append("\nsvnRevision =" + getBuildInfo().get("svnRevision"));
        
        return s.toString();

    }
    
    /**
     * Outputs the banner and exits.
     * 
     * @param args
     *            Ignored.
     */
    public static void main(final String[] args) {
        
        banner();

    }
    
    private static final String banner =//
        "\nBIGDATA(R)"+//
        "\n"+//
        "\n                   Flexible"+//
        "\n                   Reliable"+//
        "\n                  Affordable"+//
        "\n      Web-Scale Computing for the Enterprise"+//
        "\n"+//
        "\nCopyright SYSTAP, LLC 2006-2010.  All rights reserved."+//
        "\n"+//
        "\n"+AbstractStatisticsCollector.fullyQualifiedHostName+//
        "\n"+new Date()+//
        "\n"+SystemUtil.operatingSystem() + "/" + SystemUtil.osVersion()
                + " " + SystemUtil.architecture() + //
        "\n"+SystemUtil.cpuInfo() + " #CPU="+SystemUtil.numProcessors() +//
        "\n"+System.getProperty("java.vendor")+" "+System.getProperty("java.version")+
        getBuildString()+ // Note: Will add its own newline if non-empty.
        "\n\n"
        ;
    
}

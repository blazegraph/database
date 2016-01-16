/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
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

import com.bigdata.util.Depends;
import com.bigdata.util.InnerCause;
import com.bigdata.util.Depends.Dependency;
import com.bigdata.util.config.LogUtil;
import com.bigdata.util.config.NicUtil;

/**
 * Class has a static method which writes a copyright banner on stdout once per
 * JVM. This method is invoked from several core classes in order to ensure that
 * the copyright banner is always written out on bigdata startup.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class Banner {

    /**
     * The logger for <em>this</em> class.
     */
    private static final Logger log = Logger.getLogger("com.bigdata.Banner");

    private final static AtomicBoolean didBanner = new AtomicBoolean(false);
    
    private final static String fullyQualifiedHostName;

    /**
     * Returns fully qualified host name from static initialization.
     * 
     * Moved from AbstractStatisticsCollector for BLZG-1497.
     * 
     * @return
     */
    public static String getFullyqualifiedhostname() {
		return fullyQualifiedHostName;
	}

	/**
     * Environment variables understood by the {@link Banner} class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public interface Options {

        /**
         * This may be used to suppress the banner text. 
         */
        String QUIET = "com.bigdata.Banner.quiet";

        /**
         * Suppress the installation of a default
         * {@link UncaughtExceptionHandler}.
         */
        String NOCATCH = "com.bigdata.Banner.nocatch";

        /**
         * This may be used to disable JMX MBeans which self-report on the log4j
         * properties.
         */
        String LOG4J_MBEANS_DISABLE = "com.bigdata.jmx.log4j.disable";

    }
    
    /**
     * This static code block is responsible obtaining the canonical hostname.
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/886" >Provide workaround for
     *      bad reverse DNS setups</a>
     */
    static {
    
		String s = System.getProperty(BigdataStatics.HOSTNAME);
        if (s != null) {
            // Trim whitespace.
            s = s.trim();
        }
        if (s != null && s.length() != 0) {
            log.warn("Hostname override: hostname=" + s);
        } else {
		try {
			/*
			 * Note: This should be the host *name* NOT an IP address of a
			 * preferred Ethernet adaptor.
			 */
			s = InetAddress.getLocalHost().getCanonicalHostName();
			// s = NicUtil.getIpAddress("default.nic", "default", false);
		} catch (Throwable t) {
			// fall back
			log.error("Could not resolve name for host: " + t);
			s = NicUtil.getIpAddressByLocalHost();
			log.warn("Falling back to " + s);
		}
		}
       
        fullyQualifiedHostName = s;
        
    }
    
    /**
     * Display the banner, dependencies, etc.
     */
    static public void banner() {
        
        if(didBanner.compareAndSet(false/*expect*/, true/*update*/)) {

            final boolean nocatch = Boolean.getBoolean(Options.NOCATCH);

            if (!nocatch) {
                /*
                 * Set a logger for any uncaught exceptions.
                 */
                Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
                    public void uncaughtException(final Thread t,
                            final Throwable e) {
                        log.error("Uncaught exception in thread", e);
                    }
                });
            }

            if (!quiet) {

                final StringBuilder sb = new StringBuilder(banner);
         
                // Add in the dependencies.
                {
                    int maxNameLen = 0, maxProjectLen = 0, maxLicenseLen = 0;
                    for (Dependency dep : com.bigdata.util.Depends.depends()) {
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
                    try {
                    final String fmt1 = "" //
                            + "%-" + maxNameLen + "s"//
//                            + " %-" + maxProjectLen + "s" //
                            + " %-" + maxLicenseLen + "s"//
                            + "\n";

                    f.format(fmt1, "Dependency", "License");
                    
                    for (Dependency dep : com.bigdata.util.Depends.depends()) {

                        f.format(fmt1, //
                                dep.getName(),// 
//                                dep.projectURL(),//
                                dep.licenseURL()//
                                );

                    }
                    } finally {
                    	f.close();
                    }
                }
                
                System.out.println(sb);
                
            }

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

        final Logger defaultLog = 
                LogUtil.getLog4jLogger("com.bigdata");
//                Logger.getLogger("com.bigdata");

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
     * An interface which declares the keys for the map returned by
     * {@link Banner#getBuildInfo()} .
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public interface BuildInfoMeta {
        /** The bigdata release version. */
        static final String buildVersion = "buildVersion";
        /** The source code revision. */
        static final String svnRevision = "svnRevision";
        /** The source code repository URL for the branch. */
        static final String svnURL = "svnURL";
        /** The timestamp of the build. */
        static final String buildTimestamp = "buildTimestamp";
        /** The username that performed the build. */
        static final String buildUser = "buildUser";
        /** The hostname on which the build was performed. */
        static final String buildHost = "buildHost";
        /** The OS architecture on which the build was performed. */
        static final String osArch = "osArch";
        /** The OS name on which the build was performed. */
        static final String osName = "osName";
        /** The OS version on which the build was performed. */
        static final String osVersion = "osVersion";
        /** The string representing the git build branch. */
        static final String gitBranch = "gitBranch"; 
        /** 
         * The string representing the git build commit. 
         * 
         * This is the output of git  git rev-parse --verify HEAD
         * 
         * See BLZG-1688
         * 
         */
        static final String gitCommit = "gitCommit"; 
    }
    
    /**
     * Method used to discover and report on the bigdata build information. A
     * <code>com.bigdata.BuildInfo</code> class is built when the JAR is
     * created. However, it may not be present when running under an IDE from
     * the source code and, therefore, there MUST NOT be any compile time
     * references to the <code>com.bigdata.BuildInfo</code> class. This method
     * uses reflection to avoid a compile time dependency.
     * <p>
     * Note: This method works fine. However, people running from an IDE will
     * observe <em>stale</em> data from old <code>com.bigdata.BuildInfo</code>
     * class files left from a previous build of a JAR. This makes the
     * information good for deployed versions of the JAR but potentially
     * misleading when people are running under an IDE.
     * 
     * @return Build info metadata iff available.
     * 
     * @see BuildInfoMeta
     */
    public synchronized static Map<String,String> getBuildInfo() {

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

        s.append("\nbuildVersion=" + getBuildInfo().get(BuildInfoMeta.buildVersion));

        //BLZG-1688
        if(getBuildInfo().get(BuildInfoMeta.gitCommit) != null) {
        	s.append("\ngitCommit=" + getBuildInfo().get(BuildInfoMeta.gitCommit));
        }
        
        return s.toString();

    }
    
   /**
    * Attempts to return the build version (aka the release version) from the
    * <code>com.bigdata.BuildInfo</code> class. This class is generated by
    * <code>build.xml</code> and is NOT available from the IDE. It is correct
    * discovered using reflection.
    * 
    * @return Build version if available and <code>null</code> otherwise.
    * 
    * @see #getBuildInfo()
    */
   public final static String getVersion() {

      if (getBuildInfo().isEmpty()) {

         return null;

      }

      return getBuildInfo().get(BuildInfoMeta.buildVersion);

   }
   
   /**
    * Return the banner.
    */
   public static String getBanner() {
	   
	   return banner;
	   
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
    
    private static final String banner;
    private static final boolean quiet;
    
    static {

    	 quiet = Boolean.getBoolean(Options.QUIET);

    	 /*
         * If logging is not configured for [com.bigdata] then we set a
         * default log level @ WARN.  This is critical for good performance.
         */
        setDefaultLogLevel(quiet);
        
    	banner = 
        "\nBlazeGraph(TM) Graph Engine"+//
        "\n"+//
        "\n                   Flexible"+//
        "\n                   Reliable"+//
        "\n                  Affordable"+//
        "\n      Web-Scale Computing for the Enterprise"+//
        "\n"+//
        "\nCopyright SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved."+//
        "\n"+//
        "\n"+fullyQualifiedHostName+//
        "\n"+new Date()+//
        "\n"+SystemUtil.operatingSystem() + "/" + SystemUtil.osVersion()
                + " " + SystemUtil.architecture() + //
        "\n"+SystemUtil.cpuInfo() + " #CPU="+SystemUtil.numProcessors() +//
        "\n"+System.getProperty("java.vendor")+" "+System.getProperty("java.version")+
        "\nfreeMemory="+Runtime.getRuntime().freeMemory()+//
        getBuildString()+ // Note: Will add its own newline if non-empty.
        "\n\n"
        ;
    }
    
}

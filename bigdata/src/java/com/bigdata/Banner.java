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

import java.lang.reflect.Method;
import java.util.Date;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.system.SystemUtil;

import com.bigdata.counters.AbstractStatisticsCollector;

/**
 * Class has a static method which writes a copyright banner on stdout once per
 * JVM. This method is invoked from several core classes in order to ensure that
 * the copyright banner is always written out on bigdata startup.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Banner {

    private static boolean didBanner;

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
    
    synchronized static public void banner() {
        
        if(!didBanner) {
        
            final boolean quiet = Boolean.getBoolean(Options.QUIET);

            if (!quiet) {

                System.out.println(banner);

            }
         
            didBanner = true;
            
            final Logger log = Logger.getLogger("com.bigdata");

            if (log.getLevel() == null) {

                /*
                 * Since there is no default for com.bigdata, default to WARN.
                 */
                
                log.setLevel(Level.WARN);

                if (!quiet)
                    log.warn("Defaulting log level to WARN: " + log.getName());

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
     * Outputs the banner and exits.
     * 
     * @param args
     *            Ignored.
     */
    public static void main(final String[] args) {
        
        System.out.println(banner);

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
        "\n"
        ;
    
}

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

package com.bigdata.util.config;

import java.net.URL;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;

/**
 * Utility class that provides a set of static convenience methods related to
 * the initialization and configuration of the logging mechanism(s) employed by
 * the components of the system. The methods of this class can be useful both in
 * Jini configuration files, as well as in the system components themselves.
 * <p>
 * This class relies on the presence of either the
 * <code>log4j.configuration</code> or the
 * <code>log4j.primary.configuration</code> property.
 * <p>
 * If neither of those properties is found, then this class searches the
 * CLASSPATH for a log4j configuration. While this is a change from the
 * historical, searching the CLASSPATH is necessary for webapp deployments.
 * <p>
 * This class understands files with any of the following extensions {
 * <code>.properties</code>, <code>.logging</code>, <code>.xml</code> . If
 * neither configuration property is defined, if the resource identified by the
 * property can not be located, or if a log4j configuration resource can not be
 * located in the default location along the class path then this class will
 * will a message on <em>stderr</em>.
 * <p>
 * A watcher is setup on the log4j configuration if one is found.
 * <p>
 * This class cannot be instantiated.
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/394
 */
public class LogUtil {

    /**
     * Examine the various log4j configuration properties and return the name of
     * the log4j configuration resource if one was configured.
     * 
     * @return The log4j configuration resource -or- <code>null</code> if the
     *         resource was not configured properly.
     */
    static String getConfigPropertyValue() {

        final String log4jConfig = System
                .getProperty("log4j.primary.configuration");
        
        if (log4jConfig != null)
            return log4jConfig;
        
        final String log4jDefaultConfig = System
                .getProperty("log4j.configuration");
        
        if (log4jDefaultConfig != null)
            return log4jDefaultConfig;
        
        return null;
        
    }
    
    /**
     * Attempt to resolve the resources with the following names in the given
     * order and return the {@link URL} of the first such resource which is
     * found and <code>null</code> if none of the resources are found:
     * <ol>
     * <li>log4j.properties</li>
     * <li>log4j.logging</li>
     * <li>log4j.xml</li>
     * </ol>
     * 
     * @return The {@link URL} of the first such resource which was found.
     */
    static URL getConfigPropertyValueUrl() {

        URL url = LogUtil.class.getResource("/log4j.properties");

        if (url == null)
            url = LogUtil.class.getResource("/log4j.logging");

        if (url == null)
            url = LogUtil.class.getResource("/log4j.xml");

        return url;

    }
    
    public interface Options {

        /**
         * This may be used to suppress the banner text. 
         */
        String QUIET = "com.bigdata.util.config.LogUtil.quiet";
        
    }
    
    // Static initialization block that retrieves and initializes 
    // the log4j logger configuration for the given VM in which this
    // class resides. Note that this block is executed only once
    // during the life of the associated VM.
    static {

        final boolean quiet = Boolean.getBoolean(Options.QUIET);
        
        /*
         * First, attempt to resolve the configuration property.
         */
        final String log4jConfig = getConfigPropertyValue();
        
        if( log4jConfig != null && (log4jConfig.endsWith(".properties") ||
                                    log4jConfig.endsWith(".logging"))) {

            PropertyConfigurator.configureAndWatch(log4jConfig);
            if (!quiet) System.out.println("INFO: " + LogUtil.class.getName()
                    + ": Configure and watch: " + log4jConfig);

        } else if (log4jConfig != null && log4jConfig.endsWith(".xml")) {

            DOMConfigurator.configureAndWatch(log4jConfig);
            if (!quiet) System.out.println("INFO: " + LogUtil.class.getName()
                    + ": Configure and watch: " + log4jConfig);

        } else {
            
            /*
             * Then attempt to resolve the resource to a URL.
             */
            
            final URL log4jUrl = getConfigPropertyValueUrl();
            
            if (log4jUrl != null &&//
                    (log4jUrl.getFile().endsWith(".properties") || //
                     log4jUrl.getFile().endsWith(".logging")//
                    )) {

                PropertyConfigurator.configure(log4jUrl);
                if (!quiet) System.out.println("INFO: " + LogUtil.class.getName()
                        + ": Configure: " + log4jUrl);

            } else if (log4jUrl != null && log4jUrl.getFile().endsWith(".xml")) {

                DOMConfigurator.configure(log4jUrl);
                if (!quiet) System.out.println("INFO: " + LogUtil.class.getName()
                        + ": Configure: " + log4jUrl);

            } else {
                
                /*
                 * log4j was not explicitly configured and the log4j resource
                 * could not be located on the CLASSPATH.
                 */

                System.err.println("ERROR: " + LogUtil.class.getName()
                     + " : Could not initialize Log4J logging utility.\n"
                     + "Set system property "
                     +"'-Dlog4j.configuration="
                     +"file:bigdata/src/resources/logging/log4j.properties'"
                     +"\n  and / or \n"
                     +"Set system property "
                     +"'-Dlog4j.primary.configuration="
                     +"file:<installDir>/"
                     +"bigdata/src/resources/logging/log4j.properties'");
        
            }
            
        }
        
    }

    public static Logger getLog4jLogger(String componentName) {
        return Logger.getLogger(componentName);
    }

    public static Logger getLog4jLogger(Class componentClass) {
        return Logger.getLogger(componentClass);
    }

    public static Logger getLog4jRootLogger() {
        return Logger.getRootLogger();
    }
    
}

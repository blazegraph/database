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

package com.bigdata.util.config;

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
 * <code>log4j.primary.configuration</code> property and understands files with
 * any of the following extensions {<code>.properties</code>,
 * <code>.logging</code>, <code>.xml</code> . It will log a message on
 * <em>stderr</em> if neither of those properties is defined. The class
 * deliberately does not search the CLASSPATH for a log4j configuration in an
 * effort to discourage the inadvertent use of hidden configuration files when
 * deploying bigdata.
 * <p>
 * A watcher is setup on the log4j configuration if one is found.
 * <p>
 * This class cannot be instantiated.
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
    
    // Static initialization block that retrieves and initializes 
    // the log4j logger configuration for the given VM in which this
    // class resides. Note that this block is executed only once
    // during the life of the associated VM.
    static {

        final String log4jConfig = getConfigPropertyValue();
        
        if( log4jConfig != null && (log4jConfig.endsWith(".properties") ||
                                    log4jConfig.endsWith(".logging"))) {

            PropertyConfigurator.configureAndWatch(log4jConfig);
        
        } else if ( log4jConfig != null && log4jConfig.endsWith(".xml") ) {
        
            DOMConfigurator.configureAndWatch(log4jConfig);
            
        } else {
        
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
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
/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
/*
 * Created on Mar 30, 2009
 */

package com.bigdata.jmx;

import java.lang.management.ManagementFactory;
import java.util.Enumeration;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.jmx.HierarchyDynamicMBean;
import org.apache.log4j.spi.LoggerRepository;

/**
 * Utility method to register a log4j MBean.
 * <p>
 * Note: This class is based on ManagedUtil in the Apache zookeeper project and
 * is therefore under the Apache License.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see JMXTest
 */
public class JMXLog4jMBeanUtil {

    protected static final Logger log = Logger.getLogger(JMXLog4jMBeanUtil.class);
    
    /**
     * Register the log4j JMX mbeans. Set environment variable
     * <code>com.bigdata.jmx.log4j.disable</code> to true to disable
     * registration.
     * 
     * @throws JMException
     *             if registration fails
     * 
     * @see http://logging.apache.org/log4j/1.2/apidocs/index.html?org/apache/log4j/
     *      jmx/package-summary.html
     */
    public static void registerLog4jMBeans() throws JMException {
        
        if (Boolean.getBoolean("com.bigdata.jmx.log4j.disable") == true) {

            return;

        }

        // Log4J MBean
        final HierarchyDynamicMBean hdm = new HierarchyDynamicMBean();

        // Add the root logger to the Hierarchy MBean
        {

            final Logger rootLogger = Logger.getRootLogger();

            hdm.addLoggerMBean(rootLogger.getName());

        }

        /*
         * Get each logger from the Log4J Repository and add it to the Hierarchy
         * MBean created above.
         */
        {

            final LoggerRepository r = LogManager.getLoggerRepository();

            final Enumeration enumer = r.getCurrentLoggers();

            while (enumer.hasMoreElements()) {

                final Logger logger = (Logger) enumer.nextElement();

                try {
                    hdm.addLoggerMBean(logger.getName());
                } catch (Throwable t) {
                    log.error("Could not add logger: " + logger.getName(), t);
                }

            }

        }

        /*
         * Register the hierarchy MBean with the platform's mbean
         server.
         */
        {

            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

            final ObjectName mbo = new ObjectName("log4j:hierarchy=default");

            mbs.registerMBean(hdm, mbo);

        }

    }

}

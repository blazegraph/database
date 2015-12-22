/* 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed  under the  License is distributed on an "AS IS" BASIS,
 * WITHOUT  WARRANTIES OR CONDITIONS  OF ANY KIND, either  express  or
 * implied.
 * 
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.system;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

/**
 * Generic version based on {@link OperatingSystemMXBean}.
 *
 * @author <a href="mailto:dev@avalon.apache.org">Avalon Development Team</a>
 * @version $Id: Linux.java 2265 2009-10-26 12:51:06Z thompsonbry $
 */
public final class MacOSX implements CPUParser
{
    private final int m_processors;
    private final String m_cpuInfo;

    public MacOSX()
    {
    	final OperatingSystemMXBean b = ManagementFactory.getOperatingSystemMXBean();

    	m_processors = b.getAvailableProcessors();
    	
    	m_cpuInfo = b.getName()+" "+b.getVersion()+" "+b.getArch();
    
    }

    public int numProcessors()
    {
        return m_processors;
    }

    public String cpuInfo()
    {
        return m_cpuInfo;
    }

}


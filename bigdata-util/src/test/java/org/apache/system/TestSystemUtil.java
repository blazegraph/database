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

import junit.framework.TestCase;

/**
 * This is used to test SystemUtil for correctness.
 *
 * @author <a href="mailto:dev@avalon.apache.org">Avalon Development Team</a>
 * @version $Id$
 */
public final class TestSystemUtil
    extends TestCase
{
    public TestSystemUtil( String name )
    {
        super( name );
    }

    public void testSystemUtil()
    {
        System.out.println( "Number of Processors: " + SystemUtil.numProcessors() );
        System.out.println( "CPU Info:             " + SystemUtil.cpuInfo() );
        System.out.println( "Architecture:         " + SystemUtil.architecture() );
        System.out.println( "Operating System:     " + SystemUtil.operatingSystem() );
        System.out.println( "OS Version:           " + SystemUtil.osVersion() );

        assertEquals( SystemUtil.architecture(), System.getProperty( "os.arch" ) );
        assertEquals( SystemUtil.operatingSystem(), System.getProperty( "os.name" ) );
        assertEquals( SystemUtil.osVersion(), System.getProperty( "os.version" ) );
    }
}

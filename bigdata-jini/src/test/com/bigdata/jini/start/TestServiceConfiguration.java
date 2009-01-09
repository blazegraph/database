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
 * Created on Jan 6, 2009
 */

package com.bigdata.jini.start;

import java.io.File;
import java.io.FileNotFoundException;

import junit.framework.TestCase2;
import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;

import com.bigdata.jini.start.config.BigdataServiceConfiguration;
import com.bigdata.jini.start.config.IServiceConstraint;
import com.bigdata.jini.start.config.ServiceConfiguration;
import com.bigdata.jini.start.config.TransactionServiceConfiguration;
import com.bigdata.service.jini.TransactionServer;

/**
 * Some unit tests for {@link ServiceConfiguration} and friends focused on
 * verifying correct extraction of properties and the correct generation of
 * command lines and configuration files.
 * <p>
 * Note: all of this can be tested directly since we can parse the generated
 * configuration files.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo not testing correct generation of command lines
 * 
 * @todo not testing correct generation of configuration files.
 */
public class TestServiceConfiguration extends TestCase2 {

    /**
     * 
     */
    public TestServiceConfiguration() {
    }

    /**
     * @param arg0
     */
    public TestServiceConfiguration(String arg0) {
        super(arg0);
    }

    /**
     * A configuration file used by some of the unit tests in this package.
     */
    private final String configFile = "file:src/test/com/bigdata/jini/start/testfed.config";

    /**
     * 
     * 
     * @throws FileNotFoundException
     * @throws ConfigurationException
     */
    public void test01() throws FileNotFoundException, ConfigurationException {

        // Note: reads from a URI.
        final Configuration config = ConfigurationProvider
                .getInstance(new String[] { configFile });

        final BigdataServiceConfiguration serviceConfig = new TransactionServiceConfiguration(
                config);

        assertEquals(TransactionServer.class.getName(), serviceConfig.className);

        assertEquals(new String[] {"-Xmx1G", "-server"}, serviceConfig.args);

        assertEquals(
                new String[] { "com.bigdata.service.jini.TransactionServer.Options.SNAPSHOT_INTERVAL=60000" },
                serviceConfig.options);

        assertEquals(new File("test-fed"), serviceConfig.serviceDir);

        assertEquals(1, serviceConfig.serviceCount);

        assertEquals(1, serviceConfig.replicationCount);

        assertEquals(new IServiceConstraint[0], serviceConfig.constraints);
        
    }

}

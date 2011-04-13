/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Mar 9, 2011
 */

package com.bigdata.rdf.sail.bench;

import java.io.IOException;
import java.util.Properties;

import junit.framework.TestCase2;

import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.sail.bench.NanoSparqlServer.Config;
import com.bigdata.rdf.store.LocalTripleStore;

/**
 * Test suite for the {@link NanoSparqlServer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated along with {@link NanoSparqlServer}
 */
public class TestNanoSparqlServer_StartStop extends TestCase2 {

    /**
     * 
     */
    public TestNanoSparqlServer_StartStop() {
    }

    /**
     * @param name
     */
    public TestNanoSparqlServer_StartStop(String name) {
        super(name);
    }

    public void test_startStop() throws SailException, RepositoryException,
            IOException {

        final Properties properties = new Properties();

        properties.setProperty(com.bigdata.journal.Options.BUFFER_MODE,
                BufferMode.Transient.toString());

        final String namespace = getName();

        NanoSparqlServer fixture = null;

        final Journal jnl = new Journal(properties);
        try {

            // Create the kb instance.
            new LocalTripleStore(jnl, namespace, ITx.UNISOLATED, properties)
                    .create();

            final Config config = new Config();

            config.namespace = namespace;
            config.port = 0; // any open port.
            config.timestamp = ITx.UNISOLATED; // No read lock.

            // Start server for that kb instance.
            fixture = new NanoSparqlServer(config, jnl);
        
            assertTrue("open", fixture.isOpen());

        } finally {

            if (fixture != null) {

                fixture.shutdownNow();

            }
            
            jnl.destroy();
            
        }

        assertFalse("open", fixture.isOpen());
        
    }
    
}

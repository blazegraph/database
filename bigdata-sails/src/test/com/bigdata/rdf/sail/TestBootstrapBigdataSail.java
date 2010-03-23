/**
Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Jan 3, 2008
 */

package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.io.File;
import java.util.Properties;

import junit.framework.TestCase;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.sail.BigdataSail.Options;

/**
 * Bootstrap test case for bringing up the {@link BigdataSail}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBootstrapBigdataSail extends TestCase {

    /**
     * 
     */
    public TestBootstrapBigdataSail() {
    }

    /**
     * @param arg0
     */
    public TestBootstrapBigdataSail(String arg0) {
        super(arg0);
    }

    /**
     * Test create and shutdown of the default store.
     * 
     * @throws SailException
     */
    public void test_ctor_1() throws SailException {
        
        final BigdataSail sail = new BigdataSail();
        
        sail.initialize();
        
        try {

            sail.shutDown();

        }

        finally {

            String filename = sail.properties.getProperty(Options.FILE);

            if (filename != null) {

                File file = new File(filename);
                
                if(file.exists() && ! file.delete()) {
                    
                    fail("Could not delete file after test: "+filename);
                    
                }
                
            }

        }

    }

    /**
     * Test create and shutdown of a named store.
     * 
     * @throws SailException
     */
    public void test_ctor_2() throws SailException {

        final File file = new File(getName() + Options.JNL);
        
        if(file.exists()) {
            
            if(!file.delete()) {
                
                fail("Could not delete file before test: " + file);

            }
            
        }
        
        Properties properties = new Properties();

        properties.setProperty(Options.FILE, file.toString());

        BigdataSail sail = new BigdataSail(properties);

        sail.initialize();
        
        try {

            sail.shutDown();

        }

        finally {

            if (!file.exists()) {

                fail("Could not locate store: " + file);

                if (!file.delete()) {

                    fail("Could not delete file after test: " + file);

                }

            }

        }

    }

    /**
     * Test creates a database, obtains a writable connection on the database,
     * and then closes the connection and shutdown the database.
     * 
     * @throws SailException
     */
    public void test_getConnection() throws SailException {

        final File file = new File(getName() + Options.JNL);
        
        if(file.exists()) {
            
            if(!file.delete()) {
                
                fail("Could not delete file before test: " + file);

            }
            
        }

        final Properties properties = new Properties();

        properties.setProperty(Options.FILE, file.toString());

        final BigdataSail sail = new BigdataSail(properties);

        sail.initialize();
        
        try {

            final SailConnection conn = sail.getConnection();
            
            conn.close();
            
            sail.shutDown();

        }

        finally {

            if (!file.exists()) {

                fail("Could not locate store: " + file);

                if (!file.delete()) {

                    fail("Could not delete file after test: " + file);

                }

            }

        }

    }

    /**
     * Test creates a database, obtains a writable connection, writes some data
     * on the store, verifies that the data can be read back from within the
     * connection but that it is not visible in a read-committed view, commits
     * the write set, and verifies that the data is now visible in a
     * read-committed view.
     * 
     * @todo variant that writes, aborts the write, and verifies that the data
     *       was not made restart safe.
     * 
     * @throws SailException
     */
    public void test_isolation() throws SailException {

        final File file = new File(getName() + Options.JNL);
        
        if(file.exists()) {
            
            if(!file.delete()) {
                
                fail("Could not delete file before test: " + file);

            }
            
        }

        final Properties properties = new Properties();

        properties.setProperty(Options.FILE, file.toString());

        final BigdataSail sail = new BigdataSail(properties);

        sail.initialize();
        
        final SailConnection conn = sail.getConnection();
        
        final SailConnection readConn = sail.getReadOnlyConnection();
        
        try {

            final URI s = new URIImpl("http://www.bigdata.com/s");

            final URI p = new URIImpl("http://www.bigdata.com/p");

            final Value o = new LiteralImpl("o");

            // add a statement.
            conn.addStatement(s, p, o);

            // verify read back within the connection.
            {

                int n = 0;

                final CloseableIteration<? extends Statement, SailException> itr = conn
                        .getStatements(s, p, o, false/* includeInferred */);

                try {

                    while (itr.hasNext()) {

                        BigdataStatement stmt = (BigdataStatement) itr.next();

                        assertEquals("subject", s, stmt.getSubject());
                        assertEquals("predicate", p, stmt.getPredicate());
                        assertEquals("object", o, stmt.getObject());
                        // // @todo what value should the context have?
                        // assertEquals("context", null, stmt.getContext());

                        n++;

                    }

                } finally {

                    itr.close();

                }

                assertEquals("#statements visited", 1, n);
            }

            // verify NO read back in the read-committed view.
            {
                
                int n = 0;

                CloseableIteration<? extends Statement, SailException> itr = readConn
                        .getStatements(s, p, o, false/* includeInferred */);

                while (itr.hasNext()) {

                    itr.next();
                    
                    n++;

                }

                assertEquals("#statements visited", 0, n);
                
            }

            // commit the connection.
            conn.commit();

            // verify read back in the read-committed view.
            {

                int n = 0;

                final CloseableIteration<? extends Statement, SailException> itr = conn
                        .getStatements(s, p, o, false/* includeInferred */);

                try {

                    while (itr.hasNext()) {

                        BigdataStatement stmt = (BigdataStatement) itr.next();

                        assertEquals("subject", s, stmt.getSubject());
                        assertEquals("predicate", p, stmt.getPredicate());
                        assertEquals("object", o, stmt.getObject());
                        // // @todo what value should the context have?
                        // assertEquals("context", null, stmt.getContext());

                        n++;

                    }

                } finally {

                    itr.close();
                    
                }
                
                assertEquals("#statements visited", 1, n);
                
            }

        }

        finally {

            if (conn != null)
                conn.close();

            if (readConn != null)
                readConn.close();
            
            sail.shutDown();

            if (!file.exists()) {

                fail("Could not locate store: " + file);

                if (!file.delete()) {

                    fail("Could not delete file after test: " + file);

                }

            }

        }

    }

}

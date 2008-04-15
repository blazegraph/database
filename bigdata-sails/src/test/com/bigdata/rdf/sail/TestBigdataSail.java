/*
 * Copyright SYSTAP, LLC 2006-2007.  All rights reserved.
 * 
 * Contact:
 *      SYSTAP, LLC
 *      4501 Tower Road
 *      Greensboro, NC 27410
 *      phone: +1 202 462 9888
 *      email: licenses@bigdata.com
 *
 *      http://www.systap.com/
 *      http://www.bigdata.com/
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
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

import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.store.IStatementWithType;

/**
 * Bootstrap test case for bringing up the {@link BigdataSail}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBigdataSail extends TestCase {

    /**
     * 
     */
    public TestBigdataSail() {
    }

    /**
     * @param arg0
     */
    public TestBigdataSail(String arg0) {
        super(arg0);
    }

    /**
     * Test create and shutdown of the default store.
     * 
     * @throws SailException
     */
    public void test_ctor_1() throws SailException {
        
        BigdataSail sail = new BigdataSail();
        
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
     * Test create a shutdown of a named store.
     * 
     * @throws SailException
     */
    public void test_ctor_2() throws SailException {

        final String filename = getName() + Options.JNL;

        Properties properties = new Properties();

        properties.setProperty(Options.FILE, filename);

        BigdataSail sail = new BigdataSail(properties);

        try {

            sail.shutDown();

        }

        finally {

            File file = new File(filename);

            if (!file.exists()) {

                fail("Could not locate store: " + filename);

                if (!file.delete()) {

                    fail("Could not delete file after test: " + filename);

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

        final String filename = getName() + Options.JNL;

        Properties properties = new Properties();

        properties.setProperty(Options.FILE, filename);

        BigdataSail sail = new BigdataSail(properties);

        try {

            SailConnection conn = sail.getConnection();
            
            conn.close();
            
            sail.shutDown();

        }

        finally {

            File file = new File(filename);

            if (!file.exists()) {

                fail("Could not locate store: " + filename);

                if (!file.delete()) {

                    fail("Could not delete file after test: " + filename);

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

        final String filename = getName() + Options.JNL;

        Properties properties = new Properties();

        properties.setProperty(Options.FILE, filename);

        BigdataSail sail = new BigdataSail(properties);

        SailConnection conn = sail.getConnection();
        
        SailConnection readConn = sail.asReadCommittedView();
        
        try {

            final URI s = new URIImpl("http://www.bigdata.com/s");
            
            final URI p = new URIImpl("http://www.bigdata.com/p");

            final Value o = new LiteralImpl("o");

            // add a statement.
            conn.addStatement(s, p, o);
            
            // verify read back within the connection.
            {
                
                int n = 0;

                CloseableIteration<? extends Statement, SailException> itr = conn
                        .getStatements(s, p, o, false/* includeInferred */);

                while (itr.hasNext()) {

                    IStatementWithType stmt = (IStatementWithType) itr.next();

                    assertEquals("subject", s, stmt.getSubject());
                    assertEquals("predicate", p, stmt.getPredicate());
                    assertEquals("object", o, stmt.getObject());
//                    // @todo what value should the context have?
//                    assertEquals("context", null, stmt.getContext());

                    n++;

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

                CloseableIteration<? extends Statement, SailException> itr = conn
                        .getStatements(s, p, o, false/* includeInferred */);

                while (itr.hasNext()) {

                    IStatementWithType stmt = (IStatementWithType) itr.next();

                    assertEquals("subject", s, stmt.getSubject());
                    assertEquals("predicate", p, stmt.getPredicate());
                    assertEquals("object", o, stmt.getObject());
//                    // @todo what value should the context have?
//                    assertEquals("context", null, stmt.getContext());

                    n++;

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

            final File file = new File(filename);

            if (!file.exists()) {

                fail("Could not locate store: " + filename);

                if (!file.delete()) {

                    fail("Could not delete file after test: " + filename);

                }

            }

        }

    }

}

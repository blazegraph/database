/**
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
/*
 * Created on July 20, 2016
 */

package com.bigdata.rdf.sail.webapp;

import java.util.Arrays;
import java.util.LinkedHashSet;

import junit.framework.Test;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.RepositoryException;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;

/**
 * Test suite for proper UPDATE handling of xsd:date literals
 * with timezone, see https://jira.blazegraph.com/browse/BLZG-2026.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
public class TestTicket2026<S extends IIndexManager> extends
        AbstractTestNanoSparqlClient<S> {
    
    public TestTicket2026() {

    }

	public TestTicket2026(final String name) {

		super(name);

	}

	static public Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(TestTicket2026.class,
		      "test.*",
				new LinkedHashSet<BufferMode>(Arrays.asList(new BufferMode[]{
				BufferMode.DiskRW, 
				})),
				TestMode.triples
				);
	}

    private ValueFactory f = new ValueFactoryImpl();
    private URI a, b;

	@Override
	public void setUp() throws Exception {

		super.setUp();
	    
        a = f.createURI("http://a");
        b = f.createURI("http://b");
	}
	
	@Override
	public void tearDown() throws Exception {
	    
	    a = b = null;
	    f = null;
	    
	    super.tearDown();
	}

    protected boolean hasStatement(final Resource subj, final URI pred,
            final Value obj, final boolean includeInferred,
            final Resource... contexts) throws RepositoryException {

      try {

         return m_repo.hasStatement(subj, pred, obj, includeInferred, contexts);

      } catch (Exception e) {

         throw new RepositoryException(e);

      }

    }
    
    /**
     * Test date with timezone literal handling, asserting that they can be
     * properly deleted through a variable.
     * 
     * @throws Exception
     */
    public void testTicket2026DeleteByVariable() throws Exception
    {
        for (int i=-12;i<14; i++) {
            
            final String dateLiteral = dateLiteralWithTimezoneOffset(i);
            
            // initially, no date statement contained in the database
            assertFalse(hasStatement(a, b, null, false));
    
            // insert statement and assert it has been written
            final StringBuilder insert = new StringBuilder();
            insert.append("INSERT{ <http://a> <http://b> " + dateLiteral.toString() + " } WHERE {}");
    
            m_repo.prepareUpdate(insert.toString()).evaluate();
            assertTrue(hasStatement(a, b, null, false));
    
            // delete statement and assert it has been removed
            final StringBuilder delete = new StringBuilder();
            delete.append("DELETE WHERE { <http://a> <http://b> ?date }");
    
            m_repo.prepareUpdate(delete.toString()).evaluate();
            assertFalse(hasStatement(a, b, null, false));
        }

    }

    /**
     * Test date with timezone literal handling, asserting that they can be
     * properly deleted by specifying the exact literal.
     * 
     * @throws Exception
     */    
    public void testTicket2026DeleteByExactLiteral() throws Exception
    {
        for (int i=-12;i<14; i++) {
            
            final String dateLiteral = dateLiteralWithTimezoneOffset(i);
            
            // initially, no date statement contained in the database
            assertFalse(hasStatement(a, b, null, false));
    
            // insert statement and assert it has been written
            final StringBuilder insert = new StringBuilder();
            insert.append("INSERT{ <http://a> <http://b> " + dateLiteral.toString() + " } WHERE {}");
    
            m_repo.prepareUpdate(insert.toString()).evaluate();
            assertTrue(hasStatement(a, b, null, false));
    
            // delete statement and assert it has been removed
            final StringBuilder delete = new StringBuilder();
            delete.append("DELETE WHERE { <http://a> <http://b> " + dateLiteral + " }");
    
            m_repo.prepareUpdate(delete.toString()).evaluate();
            assertFalse(hasStatement(a, b, null, false));
        }

    }

    private String dateLiteralWithTimezoneOffset(int timezoneOffset) {
        final StringBuilder sb = new StringBuilder();
        sb.append("\"1981-12-31");
        if (timezoneOffset<0) {
            sb.append("-");
            sb.append(timezoneOffset<=10 ? timezoneOffset : "0" + timezoneOffset);
        } else if (timezoneOffset==0) {
            sb.append("+00:00");
        } else { // i>0
            sb.append("+");
            sb.append(timezoneOffset>=10 ? timezoneOffset : "0" + timezoneOffset);
        }
        sb.append("\"^^<http://www.w3.org/2001/XMLSchema#date>");
        
        return sb.toString();
    }
    

}

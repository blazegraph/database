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
 * Created on Jun 19, 2008
 */
package com.bigdata.rdf.sail.tck;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnectionTest;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.sail.tck.BigdataStoreTest.LTSWithNestedSubquery;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.relation.AbstractResource;

public class BigdataConnectionTest extends RepositoryConnectionTest {

	public BigdataConnectionTest(String name) {
		super(name);
	}

    /**
     * Return a test suite using the {@link LocalTripleStore} and nested
     * subquery joins.
     */
    public static class LTSWithNestedSubquery extends BigdataConnectionTest {

        public LTSWithNestedSubquery(String name) {
            super(name);
        }

        @Override
        protected Properties getProperties() {
            
            final Properties p = new Properties(super.getProperties());
            
            p.setProperty(AbstractResource.Options.NESTED_SUBQUERY,"true");
            
            return p;
            
        }

    }
    
    /**
     * Return a test suite using the {@link LocalTripleStore} and pipeline
     * joins.
     */
    public static class LTSWithPipelineJoins extends BigdataConnectionTest {

        public LTSWithPipelineJoins(String name) {
            
            super(name);
            
        }
        
        @Override
        protected Properties getProperties() {
            
            final Properties p = new Properties(super.getProperties());
            
            p.setProperty(AbstractResource.Options.NESTED_SUBQUERY,"false");
            
            return p;
            
        }

    }
    
	protected Properties getProperties() {
	    
        final Properties props = new Properties();
        
        final File journal = BigdataStoreTest.createTempFile();
        
        props.setProperty(BigdataSail.Options.FILE, journal.getAbsolutePath());
        
        props.setProperty(Options.STATEMENT_IDENTIFIERS, "false");
        
        props.setProperty(Options.QUADS, "true");
        
        props.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());

        props.setProperty(Options.ALLOW_AUTO_COMMIT, "true");
        
        props.setProperty(Options.EXACT_SIZE, "true");
        
        return props;
	    
	}
	
	@Override
	protected Repository createRepository()
		throws IOException
	{
        
        final BigdataSail sail = new BigdataSail(getProperties());

        return new BigdataSailRepository(sail);
        
	}
		
    /**
     * Overridden to destroy the backend database and its files on the disk.
     */
    @Override
    protected void tearDown()
        throws Exception
    {

        final IIndexManager backend = testRepository == null ? null
                : ((BigdataSailRepository) testRepository).getDatabase()
                        .getIndexManager();

        super.tearDown();

        if (backend != null)
            backend.destroy();

    }
    
}

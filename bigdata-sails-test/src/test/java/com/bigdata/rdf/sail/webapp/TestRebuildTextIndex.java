/*

 Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

 Contact:
 SYSTAP, LLC
 2501 Calvert ST NW #106
 Washington, DC 20008
 licenses@systap.com

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
package com.bigdata.rdf.sail.webapp;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import javax.servlet.http.HttpServletResponse;

import junit.framework.Test;

import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.TupleQueryResult;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.HttpException;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.AddOp;

/**
 * Proxied test suite for rebuilding full text index.
 * 
 */
public class TestRebuildTextIndex<S extends IIndexManager> extends
        AbstractTestNanoSparqlClient<S> {

    public TestRebuildTextIndex() {

    }

    public TestRebuildTextIndex(final String name) {

        super(name);

    }

    static public Test suite() {

       return ProxySuiteHelper.suiteWhenStandalone(TestRebuildTextIndex.class,
             "test_rebuildTextIndex",
             Collections.singleton(BufferMode.DiskRW)
             );
    }

    /**
     * Test force create full text index.
     */
    public void test_rebuildTextIndex() throws Exception {

        final String namespace = "test" + UUID.randomUUID();

        final Properties properties = new Properties();

        properties.put(Options.TEXT_INDEX, "false");

        m_mgr.createRepository(namespace, properties );

        final RemoteRepository repo = m_mgr.getRepositoryForNamespace(namespace);

        final ValueFactoryImpl vf = ValueFactoryImpl.getInstance();

        final URI s = vf.createURI("s:s1");

        final Literal o = vf.createLiteral("literal");

        final Statement[] a = new Statement[] { 
                vf.createStatement(s, RDFS.LABEL, o),

        };

        final AddOp addOp = new AddOp(Arrays.asList(a));

        repo.add(addOp);

        final String sparql = "select ?s where { ?s ?p ?o . ?o <http://www.bigdata.com/rdf/search#search> \"" + o.stringValue() + "\" .}";

        try {

            repo.prepareTupleQuery(sparql).evaluate().close();

        } catch (HttpException ex) {
            assertEquals(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ex.getStatusCode());
        }

        boolean forceBuildTextIndex = false;

        try {

            m_mgr.rebuildTextIndex(namespace, forceBuildTextIndex);

            fail("Expecting: " + HttpServletResponse.SC_INTERNAL_SERVER_ERROR);

        } catch (HttpException ex) {
            assertEquals(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ex.getStatusCode());
        }

        forceBuildTextIndex = true;

        m_mgr.rebuildTextIndex(namespace, forceBuildTextIndex);


        // Assert
        String expected = s.stringValue();

        TupleQueryResult result = repo.prepareTupleQuery(sparql).evaluate();
        
        String actual = null;
        
        try {
        	
            actual = result.next().getBinding("s").getValue().stringValue();
            
        } finally {
        	
        	result.close();
        	
        }

        assertEquals(expected, actual);

    }
    
}

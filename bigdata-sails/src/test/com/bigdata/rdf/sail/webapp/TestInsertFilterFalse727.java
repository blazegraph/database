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
Portions of this code are:

Copyright Aduna (http://www.aduna-software.com/) � 2001-2007

All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.
    * Neither the name of the copyright holder nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

package com.bigdata.rdf.sail.webapp;

import java.io.File;

import junit.framework.Test;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.parser.sparql.DC;
import org.openrdf.query.parser.sparql.FOAF;
import org.openrdf.query.parser.sparql.SPARQLUpdateTest;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.IPreparedTupleQuery;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.AddOp;

/**
 * Proxied test suite.
 * <p>
 * Note: Also see {@link SPARQLUpdateTest}. These two test suites SHOULD be kept
 * synchronized. {@link SPARQLUpdateTest} runs against a local kb instance while
 * this class runs against the NSS. The two test suites are not exactly the same
 * because one uses the {@link RemoteRepository} to commuicate with the NSS
 * while the other uses the local API.
 * 
 * @param <S>
 * 
 * @see SPARQLUpdateTest
 */
public class TestInsertFilterFalse727<S extends IIndexManager> extends
        AbstractTestNanoSparqlClient<S> {

	static public Test suite() {
		return ProxySuiteHelper.suiteWhenStandalone(TestInsertFilterFalse727.class,"test.*", TestMode.quads,TestMode.sids,TestMode.triples);
	}
    public TestInsertFilterFalse727() {

    }

	public TestInsertFilterFalse727(final String name) {

		super(name);

	}

	private  static final String EX_NS = "http://example.org/";

    private ValueFactory f = new ValueFactoryImpl();
    private  URI bob;
//    protected RemoteRepository m_repo;


	@Override
	public void setUp() throws Exception {
	    
	    super.setUp();

        bob = f.createURI(EX_NS, "bob");
	}
	
	public void tearDown() throws Exception {
	    
	    bob = null;
	    
	    f = null;
	    
	    super.tearDown();
	    
	}



    /**
     * Get a set of useful namespace prefix declarations.
     * 
     * @return namespace prefix declarations for rdf, rdfs, dc, foaf and ex.
     */
    protected String getNamespaceDeclarations() {
        final StringBuilder declarations = new StringBuilder();
        declarations.append("PREFIX rdf: <" + RDF.NAMESPACE + "> \n");
        declarations.append("PREFIX rdfs: <" + RDFS.NAMESPACE + "> \n");
        declarations.append("PREFIX dc: <" + DC.NAMESPACE + "> \n");
        declarations.append("PREFIX foaf: <" + FOAF.NAMESPACE + "> \n");
        declarations.append("PREFIX ex: <" + EX_NS + "> \n");
        declarations.append("PREFIX xsd: <" +  XMLSchema.NAMESPACE + "> \n");
        declarations.append("\n");

        return declarations.toString();
    }

    protected boolean hasStatement(final Resource subj, final URI pred,
            final Value obj, final boolean includeInferred,
            final Resource... contexts) throws RepositoryException {

        try {

            return m_repo.getStatements(subj, pred, obj, includeInferred,
                    contexts).hasNext();
            
        } catch (Exception e) {
            
            throw new RepositoryException(e);
            
        }

    }
    
    public void testInsertWhereTrue()
            throws Exception
    {
        executeInsert("FILTER ( true )", true);
    }
	private void executeInsert(String where, boolean expected) throws RepositoryException, Exception {
		final StringBuilder update = new StringBuilder();
        update.append(getNamespaceDeclarations());
		update.append("INSERT { ex:bob rdfs:label \"Bob\" . } WHERE { " + where +" }");

        assertFalse(hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));

        m_repo.prepareUpdate(update.toString()).evaluate();

        assertEquals(expected, hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
	}

    public void testInsertWhereFalse()
            throws Exception
    {
        executeInsert("FILTER ( false )", false);
    }
    

    public void testInsertWhereOptionallyTrue()
            throws Exception
    {
        executeInsert("OPTIONAL { FILTER ( true ) }", true);
    }

    public void testInsertWhereOptionallyFalse()
            throws Exception
    {
        executeInsert("OPTIONAL { FILTER ( false ) }", true);
    }

}

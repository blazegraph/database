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
Portions of this code are:

Copyright Aduna (http://www.aduna-software.com/) 2001-2007

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

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.DC;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.repository.RepositoryException;

import com.bigdata.journal.IIndexManager;

public class AbstractSimpleInsertTest<S extends IIndexManager> extends
		AbstractTestNanoSparqlClient<S> {

	private static final String EX_NS = "http://example.org/";
	private ValueFactory f = new ValueFactoryImpl();
	private URI bob;

	public AbstractSimpleInsertTest() {
	}

	public AbstractSimpleInsertTest(String name) {
		super(name);
	}

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

	protected boolean hasStatement(final Resource subj, final URI pred, final Value obj, final boolean includeInferred, final Resource... contexts)
			throws RepositoryException {
			
			    try {
			
			        return m_repo.getStatements(subj, pred, obj, includeInferred,
			                contexts).hasNext();
			        
			    } catch (Exception e) {
			        
			        throw new RepositoryException(e);
			        
			    }
			
			}

	protected void executeInsert(String where, boolean expected) throws RepositoryException, Exception {
		final StringBuilder update = new StringBuilder();
	    update.append(getNamespaceDeclarations());
		update.append("INSERT { ex:bob rdfs:label \"Bob\" . } WHERE { " + where +" }");
	
	    assertFalse(hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
	
	    m_repo.prepareUpdate(update.toString()).evaluate();
	
	    assertEquals(expected, hasStatement(bob, RDFS.LABEL, f.createLiteral("Bob"), true));
	}

}

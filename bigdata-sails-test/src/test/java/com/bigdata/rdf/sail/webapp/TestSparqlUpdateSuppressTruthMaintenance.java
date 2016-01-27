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

import java.util.Arrays;
import java.util.LinkedHashSet;

import junit.framework.Test;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.IPreparedTupleQuery;


public class TestSparqlUpdateSuppressTruthMaintenance<S extends IIndexManager> extends
        AbstractTestNanoSparqlClient<S> {
    
    public TestSparqlUpdateSuppressTruthMaintenance() {

    }

	public TestSparqlUpdateSuppressTruthMaintenance(final String name) {

		super(name);

	}

   static public Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(TestSparqlUpdateSuppressTruthMaintenance.class,
		      "test.*",
		      new LinkedHashSet<BufferMode>(Arrays.asList(new BufferMode[]{ 
		    		  BufferMode.MemStore,  
		    		  })),
		    		  TestMode.triplesPlusTruthMaintenance
				);
	}

   	
   public void testSuppressTruthMaintenance() throws Exception {
	   
		//test disable truth maintenance
        String updateStr = "DISABLE ENTAILMENTS; " + //
        					"INSERT DATA { " + //
        					"<urn:1> <urn:property1> \"someValue\"^^<http://www.w3.org/2001/XMLSchema#string> ." + //
        					"<urn:property1> <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://www.w3.org/2000/01/rdf-schema#label> . " + //
        					"}";

        m_repo.prepareUpdate(updateStr).evaluate();

        String queryStr = "SELECT * {<urn:property1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o} ";

        IPreparedTupleQuery query = m_repo.prepareTupleQuery(queryStr);
        
        assertEquals(0, countResults(query.evaluate()));
        
        //test compute closure
        updateStr = "CREATE ENTAILMENTS; " + // 
        			"ENABLE ENTAILMENTS";
				 	
		m_repo.prepareUpdate(updateStr).evaluate();
		
		assertEquals(1, countResults(query.evaluate()));
		
		//test drop entailments
	    updateStr = "DISABLE ENTAILMENTS; " + //
	       			"DELETE DATA { " + //
	       			"<urn:1> <urn:property1> \"someValue\"^^<http://www.w3.org/2001/XMLSchema#string> ." + //
	       			"<urn:property1> <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://www.w3.org/2000/01/rdf-schema#label> . " + //
	       			"}";

	   m_repo.prepareUpdate(updateStr).evaluate();

	   assertEquals(1, countResults(query.evaluate()));
	       
	   updateStr = "DROP ENTAILMENTS; " + // 
	       		   "ENABLE ENTAILMENTS";
					 	
		m_repo.prepareUpdate(updateStr).evaluate();
			
		assertEquals(0, countResults(query.evaluate()));
		
	}
   
   
}

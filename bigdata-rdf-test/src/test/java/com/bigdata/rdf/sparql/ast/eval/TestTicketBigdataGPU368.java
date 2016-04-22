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
 * Created on April 22, 2016
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.Properties;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * Test for https://github.com/SYSTAP/bigdata-gpu/issues/368
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestTicketBigdataGPU368 extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestTicketBigdataGPU368() {
    }

    /**
     * @param name
     */
    public TestTicketBigdataGPU368(String name) {
        super(name);
    }
   
   /**
    * Ticket: https://github.com/SYSTAP/bigdata-gpu/issues/368
    * ClassCast Exception when Loading LUBM: com.bigdata.rdf.internal.impl.literal.XSDBooleanIV
    * cannot be cast to com.bigdata.rdf.internal.impl.literal.NumericIV
    */
   public void testTicketBigdataGPU368() throws Exception {
       
       new TestHelper( 
           "workbench1",      // test name
           "workbench1.rq",   // query file
           "data/lehigh/LUBM-U1.rdf.gz",  // data file
           "workbench1.srx"   // result file
           ).runTest();
   }
   
   @Override
   public Properties getProperties() {

       // Note: clone to avoid modifying!!!
       final Properties properties = (Properties) super.getProperties().clone();
       
       properties.setProperty(AbstractTripleStore.Options.QUADS_MODE, "false");
       properties.setProperty(AbstractTripleStore.Options.INLINE_DATE_TIMES, "false");
       properties.setProperty(AbstractTripleStore.Options.INLINE_XSD_DATATYPE_LITERALS, "false");
       properties.setProperty(AbstractTripleStore.Options.INLINE_BNODES, "false");
       properties.setProperty(AbstractTripleStore.Options.BLOBS_THRESHOLD, ""+Integer.MAX_VALUE);
       properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
       properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS, NoAxioms.class.getName());

       return properties;

   }
}

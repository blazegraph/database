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
 * Created on Feb 5, 2007
 */

package com.bigdata.rdf.metrics;

import java.io.IOException;

import org.openrdf.sesame.constants.RDFFormat;

import com.bigdata.rdf.store.DataLoader;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestReferenceLoad extends AbstractMetricsTestCase {

    public TestReferenceLoad() {
    }

    public TestReferenceLoad(String name) {
        super(name);
    }

    public void test_referenceLoad() throws IOException {
        
        String[] resource = new String[] {
          
                "data/APSTARS/ontology-2-5-07/combine-ont.owl",
                "data/APSTARS/ontology-2-5-07/combine-refload.rdf",
                "data/APSTARS/ontology-2-5-07/sameas.rdf"
                
        };
        
        String[] baseURL = new String[] {
                "",
                "",
                ""
        };
        
        RDFFormat[] rdfFormat = new RDFFormat[] {
                RDFFormat.RDFXML,
                RDFFormat.RDFXML,
                RDFFormat.RDFXML
        };
        
        DataLoader dataLoader = store.getDataLoader();
        
        dataLoader.loadData(resource, baseURL, rdfFormat);
        
        store.commit();
        
    }
    
}

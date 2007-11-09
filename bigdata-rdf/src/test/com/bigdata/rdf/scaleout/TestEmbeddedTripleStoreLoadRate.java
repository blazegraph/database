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
 * Created on Jul 25, 2007
 */

package com.bigdata.rdf.scaleout;

import java.io.IOException;

import org.openrdf.sesame.constants.RDFFormat;

/**
 * Variant of {@link TestDistributedTripleStoreLoadRate} that tests with an
 * embedded bigdata federation and therefore does not incur costs for network
 * IO.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestEmbeddedTripleStoreLoadRate extends
        AbstractEmbeddedTripleStoreTestCase {

    /**
     * 
     */
    public TestEmbeddedTripleStoreLoadRate() {
        super();
    }

    /**
     * @param arg0
     */
    public TestEmbeddedTripleStoreLoadRate(String arg0) {
        super(arg0);
    }

    public void test_loadNCIOncology() throws IOException {

        store.getDataLoader().loadData("data/nciOncology.owl", "", RDFFormat.RDFXML);

    }

    /**
     * Runs the test RDF/XML load.
     */
    public static void main(String[] args) throws Exception {

        TestEmbeddedTripleStoreLoadRate test = new TestEmbeddedTripleStoreLoadRate(
                "TestInsertRate");
        test.setUp();
        test.test_loadNCIOncology();
        test.tearDown();

    }

}

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
 * Created on Sep 4, 2011
 */

package com.bigdata.rdf.sparql.ast.eval.bsbm.bi;

import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;

/**
 * Data driven test suite for complex queries.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Verify against ground truth results.
 */
public class TestBsbmBIQuery extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestBsbmBIQuery() {
    }

    /**
     * @param name
     */
    public TestBsbmBIQuery(String name) {
        super(name);
    }

    /**
     * PC 10 data set. This is the data set which was used to generate the
     * concrete instances of the queries referenced from within this class.
     */
//     static private final String dataset = "bsbm/bsbm3_dataset_pc10.nt";

    /**
     * An empty data set. This may be used if you are simply examining the query
     * plans.
     */
    static private final String dataset = "bsbm/emptyDataset.nt";

    public void test_bsbm_bi_query1() throws Exception {

        new TestHelper("query1", // name
                "bsbm/bi/query1.rq",// query
                dataset,//
                "bsbm/bi/empty.srx"// result
        ).runTest();

    }

    public void test_bsbm_bi_query2() throws Exception {

        new TestHelper("query2", // name
                "bsbm/bi/query2.rq",// query
                dataset,//
                "bsbm/bi/empty.srx"// result
        ).runTest();

    }

    public void test_bsbm_bi_query3() throws Exception {

        new TestHelper("query3", // name
                "bsbm/bi/query3.rq",// query
                dataset,//
                "bsbm/bi/empty.srx"// result
        ).runTest();

    }

    public void test_bsbm_bi_query4() throws Exception {

        new TestHelper("query4", // name
                "bsbm/bi/query4.rq",// query
                dataset,//
                "bsbm/bi/empty.srx"// result
        ).runTest();

    }

    public void test_bsbm_bi_query5() throws Exception {

        new TestHelper("query5", // name
                "bsbm/bi/query5.rq",// query
                dataset,//
                "bsbm/bi/empty.srx"// result
        ).runTest();

    }

    public void test_bsbm_bi_query6() throws Exception {

        new TestHelper("query6", // name
                "bsbm/bi/query6.rq",// query
                dataset,//
                "bsbm/bi/empty.srx"// result
        ).runTest();

    }

    public void test_bsbm_bi_query7() throws Exception {

        new TestHelper("query7", // name
                "bsbm/bi/query7.rq",// query
                dataset,//
                "bsbm/bi/empty.srx"// result
        ).runTest();

    }

    public void test_bsbm_bi_query8() throws Exception {

        new TestHelper("query8", // name
                "bsbm/bi/query8.rq",// query
                dataset,//
                "bsbm/bi/empty.srx"// result
        ).runTest();

    }

}

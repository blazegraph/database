/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Aug 29, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;

/**
 * Test suite for {@link ASTAttachJoinFiltersOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestASTAttachJoinFiltersOptimizer extends AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTAttachJoinFiltersOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTAttachJoinFiltersOptimizer(String name) {
        super(name);
    }

    /**
     * TODO Write unit test. Note that the core logic for deciding join filter
     * attachment is tested elsewhere.
     * 
     * TODO Write unit test for re-attachment. This differs because the filters
     * need to be collected from the required statement pattern nodes and then
     * reattached for a new join ordering. (RTO use case).
     */
    public void test_attachFilters() {
        fail("write test");
    }

}

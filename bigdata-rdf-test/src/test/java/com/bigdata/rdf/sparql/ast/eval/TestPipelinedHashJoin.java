/**

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
/*
 * Created on Oct 26, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.join.PipelinedHashIndexAndSolutionSetOp;
import com.bigdata.rdf.sparql.ast.ASTContainer;

/**
 * Test suite for {@link PipelinedHashIndexAndSolutionSetOp}, which implements
 * a pipelined hash join.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestPipelinedHashJoin extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestPipelinedHashJoin() {
    }

    /**
     * @param name
     */
    public TestPipelinedHashJoin(String name) {
        super(name);
    }

    /**
     * Use pipelined hash join for OPTIONAL when LIMIT specified.
     */
    public void testPipelinedHashJoinUsedForOptional() throws Exception {
       
       final ASTContainer astContainer = new TestHelper(
             "pipelined-hashjoin",// testURI
             "pipelined-hashjoin-used-optional.rq", // queryURI
             "pipelined-hashjoin.trig", // dataURI
             "pipelined-hashjoin-optional.srx" // resultURI
             ).runTest();
       
       final PipelineOp queryPlan = astContainer.getQueryPlan();

       if (!BOpUtility.visitAll(
          queryPlan, PipelinedHashIndexAndSolutionSetOp.class).hasNext()) {

          fail("Expecting a PipelinedHashIndexAndSolutionSetOp in the query plan: "
                + astContainer.toString());
       }
      
    }

    /**
     * Use pipelined hash join for MINUS when LIMIT specified. 
     */
    public void testPipelinedHashJoinUsedForMinus() throws Exception  {
       
       final ASTContainer astContainer = new TestHelper(
             "pipelined-hashjoin",// testURI
             "pipelined-hashjoin-used-minus.rq", // queryURI
             "pipelined-hashjoin.trig", // dataURI
             "pipelined-hashjoin-minus.srx" // resultURI
             ).runTest();
       
       final PipelineOp queryPlan = astContainer.getQueryPlan();

       if (!BOpUtility.visitAll(
          queryPlan, PipelinedHashIndexAndSolutionSetOp.class).hasNext()) {

          fail("Expecting a PipelinedHashIndexAndSolutionSetOp in the query plan: "
                + astContainer.toString());
       }
       
    }
    
    /**
     * Use pipelined hash join for UNION when LIMIT specified. 
     */
    // TODO: this failing, check why...
    public void testPipelinedHashJoinUsedForUnion() throws Exception  {
       
       final ASTContainer astContainer = new TestHelper(
             "pipelined-hashjoin",// testURI
             "pipelined-hashjoin-used-union.rq", // queryURI
             "pipelined-hashjoin.trig", // dataURI
             "pipelined-hashjoin-union.srx" // resultURI
             ).runTest();
       
       final PipelineOp queryPlan = astContainer.getQueryPlan();

       if (!BOpUtility.visitAll(
          queryPlan, PipelinedHashIndexAndSolutionSetOp.class).hasNext()) {

          fail("Expecting a PipelinedHashIndexAndSolutionSetOp in the query plan: "
                + astContainer.toString());
       }
       
    }

    /**
     * Use pipelined hash join for ALP node.
     */
    public void testPipelinedHashJoinUsedForALP() throws Exception  {
       
    }
    
    /**
     * Use pipelined hash join for SERVICE node.
     */
    public void testPipelinedHashJoinUsedForService() throws Exception  {
       
    }

    /**
     * Use pipelined hash join for SERVICE node.
     */
    public void testPipelinedHashJoinUsedForExists() throws Exception  {
       
    }
    
    /**
     * Use pipelined hash join for SERVICE node.
     */
    public void testPipelinedHashJoinUsedForValues() throws Exception  {
       
    }

    /**
     * Make sure the pipelined hash join operator is not used as a standard
     * for non-LIMIT query. Query body is the same as for 
     * testPipelinedHashJoinUsedForOptional(), but no LIMIT.
     */
    public void testPipelinedHashJoinNotUsed01() throws Exception  {
       
       final ASTContainer astContainer = new TestHelper(
             "pipelined-hashjoin",// testURI
             "pipelined-hashjoin-notused-optional01.rq", // queryURI
             "pipelined-hashjoin.trig", // dataURI
             "pipelined-hashjoin-optional.srx" // resultURI
             ).runTest();    

       final PipelineOp queryPlan = astContainer.getQueryPlan();

       if (BOpUtility.visitAll(
          queryPlan, PipelinedHashIndexAndSolutionSetOp.class).hasNext()) {

          fail("Expecting *no* PipelinedHashIndexAndSolutionSetOp in the query plan: "
                + astContainer.toString());
       }       
    }

    /**
     * Make sure the pipelined hash join operator is not used as a standard
     * for LIMIT+OFFSET queries. Query body is the same as for 
     * testPipelinedHashJoinUsedForOptional(), but including LIMIT *and*
     * ORDER BY.
     */
    public void testPipelinedHashJoinNotUsed02() throws Exception  {
       
       final ASTContainer astContainer = new TestHelper(
             "pipelined-hashjoin",// testURI
             "pipelined-hashjoin-notused-optional02.rq", // queryURI
             "pipelined-hashjoin.trig", // dataURI
             "pipelined-hashjoin-optional.srx" // resultURI
             ).runTest();     
       
       final PipelineOp queryPlan = astContainer.getQueryPlan();

       if (BOpUtility.visitAll(
          queryPlan, PipelinedHashIndexAndSolutionSetOp.class).hasNext()) {

          fail("Expecting *no* PipelinedHashIndexAndSolutionSetOp in the query plan: "
                + astContainer.toString());
       }           
    }
    
    /**
     * Test enabling the pipelined hash join by query hint.
     */
    public void testPipelinedHashJoinUsedByQueryHint() throws Exception  {
       
    }

}
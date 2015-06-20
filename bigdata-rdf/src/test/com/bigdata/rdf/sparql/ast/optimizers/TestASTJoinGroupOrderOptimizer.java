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
 * Created on June 16, 2015
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.ArbitraryLengthPathNode;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.BindingsClause;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.BDS;
import com.bigdata.service.fts.FTS;


/**
 * Test suite for the {@link ASTJoinGroupOrderOptimizer}.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
@SuppressWarnings({ "rawtypes" })
public class TestASTJoinGroupOrderOptimizer extends AbstractOptimizerTestCase {

   public TestASTJoinGroupOrderOptimizer() {
   }

   public TestASTJoinGroupOrderOptimizer(String name) {
       super(name);
   }
   
   @Override
   IASTOptimizer newOptimizer() {
      return new ASTOptimizerList(new ASTJoinGroupOrderOptimizer());
   }


   /**
    * Complex test case where we have a mix of all possible constructs
    * within a join group.
    */
   public void testAllConstructs() {

      // TODO: implement complex test case
//      new Helper(){{
//         
//         given = 
//            select(varNode(x), 
//            where (
//               stmtPatternWithVar("x1"), /* non optional stmt pattern */
//               stmtPatternWithVarOptional("x2"),  /* optional stmt pattern */
//               filterWithVar("x3"),
//               filterWithVars("x4","x5"),
//               filterExistsWithVars("x6","x7"),
//               filterExistsWithVars("x8","x9","x10"),
//               joinGroupWithVars("x11","x12"),
//               serviceSparql11WithConstant("x13","x14"),
//               serviceSparql11WithVariable("x15","x16","x17"),
//               serviceBDSWithVariable("x18"),
//               serviceFTSWithVariable("x19","x20","x21","x22"),
//               alpNodeWithVars("x23","x24","x25"),
//               unionWithVars("x26","x27"),
//               assignmentWithVar("x28","x29"),
//               bindingsClauseWithVars("x32","x33","x34"),
//               subqueryWithVars("x35","x36")
//            ));
//         
//         
//      }}.test();
//      
   }

   public void testFilterPlacement01() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3"),
               filterWithVars("x1","x2")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVar("x2"),
               filterWithVars("x1","x2"),
               stmtPatternWithVar("x3")
               ));
         
      }}.test();
   }
   
   public void testFilterPlacement02() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               joinGroupWithVars("x1"),
               joinGroupWithVars("x2"),
               joinGroupWithVars("x1","x3"),
               filterWithVar("x4"),
               filterWithVar("x1"),
               filterExistsWithVars("x1","x2"),
               filterNotExistsWithVars("x3")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               joinGroupWithVars("x1"),
               filterExistsWithVars("x1","x2"),
               filterWithVar("x1"),
               joinGroupWithVars("x2"),
               joinGroupWithVars("x1","x3"),
               filterNotExistsWithVars("x3"),
               filterWithVar("x4")
            ));
         
      }}.test();
   }
   
   public void testFilterPlacement03() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               joinGroupNode(
                  stmtPatternWithVar("x2"),
                  filterWithVar("x1")
               )
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
                  joinGroupNode(
                  filterWithVar("x1"),
                  stmtPatternWithVar("x2")
               )
            ));

         
      }}.test();
   }

   public void testBindPlacement01() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVar("x2"),
               assignmentWithConst("x1"),
               assignmentWithConst("x4"),
               assignmentWithConst("x2")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               assignmentWithConst("x1"),
               stmtPatternWithVar("x1"),
               assignmentWithConst("x2"),
               stmtPatternWithVar("x2"),
               assignmentWithConst("x4")
           ));
         
      }}.test();
   } 
   
   public void testBindPlacement02() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3"),
               stmtPatternWithVar("x4"),
               assignmentWithVar("x2","x1"),
               assignmentWithVar("x2","x3")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               assignmentWithVar("x2","x1"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3"),
               assignmentWithVar("x2","x3"),
               stmtPatternWithVar("x4")
           ));
         
      }}.test();
   } 
   
   
   public void testValuesPlacement01() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVar("x2"),
               bindingsClauseWithVars("x1"),
               bindingsClauseWithVars("x1","x3"),
               stmtPatternWithVar("x3"),
               bindingsClauseWithVars("x2"),
               stmtPatternWithVar("x4")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               bindingsClauseWithVars("x1"),
               bindingsClauseWithVars("x1","x3"),
               stmtPatternWithVar("x1"),
               bindingsClauseWithVars("x2"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3"),
               stmtPatternWithVar("x4")
           ));
         
      }}.test();
   }    
   
   public void testServicePlacementSparql11a() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3"),
               stmtPatternWithVar("x4"),
               serviceSparql11WithVariable("x1", "x2"),
               serviceSparql11WithVariable("x3", "x4", "x5")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               serviceSparql11WithVariable("x1", "x2"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3"),
               serviceSparql11WithVariable("x3", "x4", "x5"),
               stmtPatternWithVar("x4")
           ));
         
      }}.test();
   } 
   
   public void testServicePlacementSparql11b() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3"),
               serviceSparql11WithVariable("x1", "x2"),
               serviceSparql11WithVariable("x2", "x1", "x3")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               serviceSparql11WithVariable("x1", "x2"),
               serviceSparql11WithVariable("x2", "x1", "x3"), /* x2 bound by previous service! */
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3")
           ));
         
      }}.test();
   } 
   
   public void testServicePlacementSparqlBDS() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3"),
               stmtPatternWithVar("x4"),
               serviceBDSWithVariable("x2")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               serviceBDSWithVariable("x2"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3"),
               stmtPatternWithVar("x4")
           ));
         
      }}.test();
   } 
   
   public void testServicePlacementSparqlFTS01() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("inSearch"),
               stmtPatternWithVar("inEndpoint"),
               stmtPatternWithVar("inParams"),
               stmtPatternWithVar("outRes"),
               stmtPatternWithVar("outScore"),
               stmtPatternWithVar("outSnippet"),
               serviceFTSWithVariable(
                  "outRes", "outScore", "outSnippet", 
                  "inSearch", "inEndpoint", "inParams")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("inSearch"),
               stmtPatternWithVar("inEndpoint"),
               stmtPatternWithVar("inParams"),
               serviceFTSWithVariable(
                     "outRes", "outScore", "outSnippet", 
                     "inSearch", "inEndpoint", "inParams"),
               stmtPatternWithVar("outRes"),
               stmtPatternWithVar("outScore"),
               stmtPatternWithVar("outSnippet")
           ));
         
      }}.test();
   } 
   
   public void testServicePlacementSparqlFTS02() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               serviceFTSWithVariable(
                  "outRes", "outScore", "outSnippet", 
                  "inSearch", "inEndpoint", "inParams2"),
               serviceFTSWithVariable(
                  "outRes", "outScore", "outSnippet", 
                  "inSearch", "inEndpoint", "inParams"), 
               stmtPatternWithVar("inSearch"),
               stmtPatternWithVar("inEndpoint"),
               stmtPatternWithVar("outRes"),
               stmtPatternWithVar("inParams"),
               stmtPatternWithVar("x"),
               stmtPatternWithVar("outScore"),
               stmtPatternWithVar("outSnippet")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("inSearch"),
               stmtPatternWithVar("inEndpoint"),
               stmtPatternWithVar("outRes"),
               stmtPatternWithVar("inParams"),
               serviceFTSWithVariable(
                  "outRes", "outScore", "outSnippet", 
                  "inSearch", "inEndpoint", "inParams"),
               stmtPatternWithVar("x"),
               stmtPatternWithVar("outScore"),
               stmtPatternWithVar("outSnippet"),
               serviceFTSWithVariable(
                  "outRes", "outScore", "outSnippet", 
                  "inSearch", "inEndpoint", "inParams2")
           ));
         
      }}.test();
   } 
   
   public void testPlacementInContextOfUnions() {
      
      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               unionWithVars("x1","x2"),
               unionWithVars("x1", "x1", "x2"),
               unionWithVars("x3","x3"),
               unionWithVars("bound","x3"),
               assignmentWithVar("bound", "x1")
            ));
         
         /**
          * Only the second UNION expression bounds x1 for sure. The BIND node
          * is placed at the first "useful" position after that one.
          */
         expected = 
            select(varNode(x), 
            where (
               unionWithVars("x1","x2"),
               unionWithVars("x1", "x1", "x2"), /* { ... x1 ... } UNION { ... x1 ... x2 } */
               unionWithVars("x3","x3"),
               assignmentWithVar("bound", "x1"),
               unionWithVars("bound","x3")
           ));
         
      }}.test();   
      
   }

   public void testPlacementInContextOfSubquery() {
      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               assignmentWithVar("boundVar", "x3"),
               subqueryWithVars("x1", "x2"),
               subqueryWithVars("x2", "x3"),
               subqueryWithVars("x3", "x4"),
               subqueryWithVars("x4", "boundVar")
            ));
         
         expected = 
           select(varNode(x), 
           where (
               subqueryWithVars("x1", "x2"),
               subqueryWithVars("x2", "x3"),
               subqueryWithVars("x3", "x4"),
               assignmentWithVar("boundVar", "x3"),
               subqueryWithVars("x4", "boundVar")
            ));

         
      }}.test();   
   }

   public void testPlacementInContextOfNamedSubquery() {
      
      new Helper(){{
         
         given = 
            select(varNode(x), 
            namedSubQuery(
               "_set1",
               varNode("x1"),
               where(statementPatternNode(varNode("x1"), varNode("inner"), constantNode(b)))
            ),
            where (
               assignmentWithVar("boundVar", "x1"),
               namedSubQueryInclude("_set1"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("boundVar")
            ));
         
         expected = 
            select(varNode(x), 
               namedSubQuery(
                  "_set1",
                  varNode("x1"),
                  where(statementPatternNode(varNode("x1"), varNode("inner"), constantNode(b)))
               ),
               where (
                  namedSubQueryInclude("_set1"),
                  stmtPatternWithVar("x2"),
                  assignmentWithVar("boundVar", "x1"),
                  stmtPatternWithVar("boundVar")
               ));

         
      }}.test();   
   }

   public void testPlacementInContextOfOptional() {
      
      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVarOptional("x1"),
               stmtPatternWithVar("x1"),
               stmtPatternWithVarOptional("x2"),
               stmtPatternWithVarOptional("x2"),
               stmtPatternWithVarOptional("bound"),
               assignmentWithVar("bound", "x1")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVarOptional("x1"),
               stmtPatternWithVar("x1"),
               stmtPatternWithVarOptional("x2"),
               stmtPatternWithVarOptional("x2"),
               assignmentWithVar("bound", "x1"),
               stmtPatternWithVarOptional("bound")
           ));
         
      }}.test();   
   }

   /** 
    * Returns a fresh statement pattern with the specified variable bound.
    */
   StatementPatternNode stmtPatternWithVar(final String varName) {
      return stmtPatternWithVar(varName, false);
   }

   /** 
    * Returns a fresh optional statement pattern with the specified variable bound.
    */
   StatementPatternNode stmtPatternWithVarOptional(final String varName) {
      return stmtPatternWithVar(varName, true);
   }

   /** 
    * Returns a fresh statement pattern with the specified variable bound that
    * is either optional or not, as specified by the second parameter.
    */
   StatementPatternNode stmtPatternWithVar(
      final String varName, final boolean optional) {
      
      final StatementPatternNode spn = 
         (StatementPatternNode) new Helper(){{
            tmp = statementPatternNode(varNode(varName),constantNode(a),constantNode(b));
         }}.getTmp();
      spn.setOptional(optional);
      
      return spn;
   }
   
   /**
    * For each variable, a fresh statement pattern with the variable is created
    * and the list of statement patterns is returned.
    */
   StatementPatternNode[] stmtPatternsWithVars(final String... varNames) {
      
      final StatementPatternNode[] statementPatterns = 
         new StatementPatternNode[varNames.length];
      for (int i=0; i<varNames.length; i++) {
         statementPatterns[i] = stmtPatternWithVar(varNames[i], false);
      }
      
      return statementPatterns;
   }
   

   /**
    * Returns a fresh filter node with the specified variable in their
    * {@link ValueExpressionNode}. The filter is not of type
    * EXISTS or NOT EXISTS.
    */
   FilterNode filterWithVar(final String varName) {
      
      final FilterNode fn = (FilterNode) new Helper(){{
         tmp = filter(FunctionNode.EQ(varNode(varName),constantNode(a)));
      }}.getTmp();
      
      return resolveVEs(fn);
   }

   /**
    * Returns a fresh filter node with the specified variables in their
    * {@link ValueExpressionNode}. The filter is not of type
    * EXISTS or NOT EXISTS.
    */
   FilterNode filterWithVars(final String varName1, final String varName2) {

      final FilterNode fn = (FilterNode) new Helper(){{
         tmp = filter(FunctionNode.NE(varNode(varName1),varNode(varName2)));
      }}.getTmp();
      
      return resolveVEs(fn);
   }
   
   /**
    * Returns a fresh FILTER NOT EXISTS node with the specified variables 
    * in their body.
    */
   FilterNode filterExistsWithVars(
      final String internalVar, final String... varNames) {

      final StatementPatternNode[] statementPatterns = 
            stmtPatternsWithVars(varNames);
      
      final FilterNode fn = 
         (FilterNode) new Helper(){{
         tmp = 
            filter(
               notExists(
                  varNode(internalVar), 
                  joinGroupNode((Object[])statementPatterns)));
         }}.getTmp();
         
      return resolveVEs(fn);
   }
   
   
   /**
    * Returns a fresh FILTER EXISTS node with the specified variables in 
    * their body.
    */
   FilterNode filterNotExistsWithVars(
      final String internalVar, final String... varNames) {

      final StatementPatternNode[] statementPatterns = 
         stmtPatternsWithVars(varNames);
      
      final FilterNode fn = 
         (FilterNode) new Helper(){{
         tmp = 
            filter(
               exists(
                  varNode(internalVar), 
                  joinGroupNode((Object[])statementPatterns)));
         }}.getTmp();
         
      return resolveVEs(fn);
   }
   
   /**
    * Returns a {@link JoinGroupNode} binding the specified vars.
    */
   JoinGroupNode joinGroupWithVars(final String... varNames) {
      
      final StatementPatternNode[] statementPatterns = 
            stmtPatternsWithVars(varNames);
      
      final JoinGroupNode jgn = 
         (JoinGroupNode) new Helper(){{
            tmp = joinGroupNode((Object[])statementPatterns);
         }}.getTmp();
      
      return jgn;         
   }

   /**
    * Returns a {@link UnionNode} binding varNames in their
    * body, where left variable is the (one and only) in the left pattern,
    * and rights are the variables in the right union pattern.
    */
   UnionNode unionWithVars(final String varLeft, final String... varsRight) {
      
      final StatementPatternNode[] statementPatternsRight = 
            stmtPatternsWithVars(varsRight);
      
      final UnionNode unionNode = (UnionNode) new Helper(){{
         tmp = 
            unionNode(
               joinGroupNode(stmtPatternWithVar(varLeft, false)),
               joinGroupNode((Object[])statementPatternsRight));
      }}.getTmp();

      return unionNode;   
   }
   
   
   /**
    * Returns an {@link AssignmentNode} binding varNames in their body to
    * the specified var.
    */
   AssignmentNode assignmentWithConst(final String boundVar) {
      
      final AssignmentNode an = (AssignmentNode) new Helper() {{
         tmp = bind(constantNode(a), varNode(boundVar) );
      }}.getTmp();

      return an;
   }
   
   
   /**
    * Returns an {@link AssignmentNode} binding varNames in their body to
    * the specified var.
    */
   AssignmentNode assignmentWithVar(final String boundVar, final String usedVar) {
      
      final AssignmentNode an = (AssignmentNode) new Helper() {{
         tmp = bind(varNode(usedVar), varNode(boundVar) );
      }}.getTmp();

      return an;
   }
   
   /**
    * Returns an {@link AssignmentNode} binding the vars to themselves (useful
    * for projection nodes).
    */
   AssignmentNode[] assignmentWithVars(final String... varNames) {
      
      AssignmentNode[] assignmentNodes = new AssignmentNode[varNames.length];

      for (int i=0; i<varNames.length; i++) {
         assignmentNodes[i] = assignmentWithVar(varNames[i], varNames[i]);
      }

      return assignmentNodes;
   }

   /**
    * Returns a {@link BindingsClause} binding the specified vars.
    */
   BindingsClause bindingsClauseWithVars(final String... varNames) {
      
      final LinkedHashSet<IVariable<?>> declaredVars = 
          new LinkedHashSet<IVariable<?>>();
      for (String varName : varNames) {
         declaredVars.add(Var.var(varName));
      }

      final List<IBindingSet> bindingSets = new ArrayList<IBindingSet>();
      IBindingSet bs = new ListBindingSet();
      for (String varName : varNames) {
         bs.set(Var.var(varName), new Constant<>(TermId.mockIV(VTE.URI)));
      }
            
      return new BindingsClause(declaredVars, bindingSets);
   }
   
   /**
    * Returns a {@link SubqueryRoot} projecting the specified vars.
    */
   SubqueryRoot subqueryWithVars(final String... varNames) {
      
      final AssignmentNode[] assignments = 
         assignmentWithVars(varNames);
      final StatementPatternNode[] statementPatterns = 
         stmtPatternsWithVars(varNames);
      
      final SubqueryRoot subquery = (SubqueryRoot) new Helper() {{
         tmp = 
            selectSubQuery(
               projection(assignments),
               where(joinGroupNode((Object[])statementPatterns)));
      }}.getTmp();
      
      return subquery;
      
   }

   
   /**
    * Returns an {@link ArbitraryLengthPathNode} binding varNames in their
    * body, where the first parameter specified the 
    * {@link ArbitraryLengthPathNode}s internal variable.
    */
   ArbitraryLengthPathNode alpNodeWithVars(
      final String alpVar, final String... varNames) {

      final StatementPatternNode[] statementPatterns = 
            stmtPatternsWithVars(varNames);
      
      final ArbitraryLengthPathNode alpNode = 
            (ArbitraryLengthPathNode) new Helper() {{
               tmp = 
                  arbitartyLengthPropertyPath(
                     varNode(alpVar), 
                     constantNode(b), 
                     HelperFlag.ZERO_OR_MORE,
                     joinGroupNode((Object[])statementPatterns));
         }}.getTmp();
         
      return alpNode;
   }
   
   /**
    * Returns a SPARQL 1.1 {@link ServiceNode} using the specified vars in its
    * body, with a constant endpoint.
    */
   ServiceNode serviceSparql11WithConstant(final String... varNames) {
      
      final JoinGroupNode jgn = joinGroupWithVars(varNames);
      
      final BigdataValueFactory f = store.getValueFactory();
      final BigdataURI serviceEndpoint = f.createURI("http://custom.endpoint");
      final IV serviceEndpointIV = makeIV(serviceEndpoint);
      
      final BigdataValue[] values = new BigdataValue[] { serviceEndpoint };       
      store.getLexiconRelation().addTerms(
         values, values.length, false/* readOnly */);
      
       final ServiceNode serviceNode = 
          (ServiceNode) new Helper(){{
             tmp = service(constantNode(serviceEndpointIV),jgn);
          }}.getTmp();  
          
       return serviceNode;
   }
   
   /**
    * Returns a SPARQL 1.1 {@link ServiceNode} using the specified vars in its 
    * body, with an endpoint specified through endpointVar.
    */
   ServiceNode serviceSparql11WithVariable(
      final String endpointVar, final String... varNames) {
      
      final JoinGroupNode jgn = joinGroupWithVars(varNames);
      
      final ServiceNode serviceNode = 
         (ServiceNode) new Helper(){{
            tmp = service(varNode(endpointVar),jgn);
         }}.getTmp();  
          
       return serviceNode;
   }   
   
   /**
    * Returns a BDS type {@link ServiceNode} using the specified vars in its
    * body.
    */
   ServiceNode serviceBDSWithVariable(final String inputVar) {
      
      final BigdataValueFactory f = store.getValueFactory();
      final BigdataURI bdsSearch = f.createURI(BDS.NAMESPACE + "search");
      final BigdataURI predSearch = f.createURI(BDS.SEARCH.toString());
      final BigdataURI predSearchTimeout = f.createURI(BDS.SEARCH_TIMEOUT.toString());
      final BigdataURI predMatchExact = f.createURI(BDS.MATCH_EXACT.toString());
      
      final BigdataValue[] values = 
         new BigdataValue[] { bdsSearch, predSearch, predSearchTimeout, predMatchExact };       
      store.getLexiconRelation().addTerms(values, values.length, false/* readOnly */);

      final ServiceNode serviceNode = 
         (ServiceNode) new Helper(){{
            tmp = 
               service(
                  constantNode(makeIV(bdsSearch)), 
                  joinGroupNode(
                     statementPatternNode(varNode(inputVar), constantNode(makeIV(predSearch)), constantNode("search")),
                     statementPatternNode(varNode(inputVar), constantNode(makeIV(predSearchTimeout)), constantNode("1000")),
                     statementPatternNode(varNode(inputVar), constantNode(makeIV(predMatchExact)), constantNode("false"))));
            }}.getTmp();
            
       return serviceNode;
   }
   
   /**
    * Return a FTS type {@link ServiceNode} using the specified variables.
    */
   ServiceNode serviceFTSWithVariable(
      // output variables:
      final String resultVar, final String scoreVar, final String snippetVar, 
      // input variables:
      final String searchVar, final String endpointVar, final String paramsVar) {
      
      final BigdataValueFactory f = store.getValueFactory();
      final BigdataURI ftsSearch = f.createURI(FTS.NAMESPACE + "search");
      final BigdataURI predSearch = f.createURI(FTS.SEARCH.toString());
      final BigdataURI predEndpoint = f.createURI(FTS.ENDPOINT.toString());
      final BigdataURI predParams = f.createURI(FTS.PARAMS.toString());
      
      final BigdataValue[] values = 
         new BigdataValue[] { 
            ftsSearch, predSearch, predEndpoint, predParams };       
      store.getLexiconRelation().addTerms(values, values.length, false/* readOnly */);
      
      final ServiceNode serviceNode = (ServiceNode) new Helper(){{
         tmp = 
            service(
               constantNode(makeIV(ftsSearch)), 
               joinGroupNode(
                  statementPatternNode(varNode(resultVar), constantNode(makeIV(predSearch)), varNode(searchVar)),
                  statementPatternNode(varNode(resultVar), constantNode(makeIV(predEndpoint)), varNode(endpointVar)),
                  statementPatternNode(varNode(resultVar), constantNode(makeIV(predParams)), varNode(paramsVar))));
         }}.getTmp();
         
      return serviceNode;
   }   

   /**
    * Properly resolves the value expressions in the {@link FilterNode}.
    */
   FilterNode resolveVEs(FilterNode fn) {
      
      final QueryRoot query = new QueryRoot(QueryType.SELECT);
      final JoinGroupNode jgn = new JoinGroupNode(fn);
      query.setWhereClause(jgn);
      
      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(query), store);
      final GlobalAnnotations globals = 
         new GlobalAnnotations(
            context.getLexiconNamespace(), context.getTimestamp());
      AST2BOpUtility.toVE(globals, fn.getValueExpressionNode());
      
      return fn;
   }
   
   // TODO: test case with named subquery
   // TODO: verify case of ASK subqueries

   
}
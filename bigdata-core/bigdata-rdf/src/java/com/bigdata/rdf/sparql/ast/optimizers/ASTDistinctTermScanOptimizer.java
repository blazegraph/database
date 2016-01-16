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
 * Created on Sep 14, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.Var;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryBase;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpBase;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.striterator.IKeyOrder;

/**
 * Optimizes
 * <code>SELECT (DISTINCT|REDUCED) ?property WHERE { ?x ?property ?y . }</code>
 * and similar patterns using an O(N) algorithm, where N is the number of
 * distinct solutions.
 * <p>
 * The main advantage here is to turn an access path that is fully unbound into
 * a distinct-term scan. If the access path would be evaluated as-bound with at
 * least one variable bound, then the distinct term scan might not have any
 * advantage over the pipeline join (and the semantics of DISTINCT would be
 * violated with multiple as-bound evaluations of the distinct-term-scan without
 * a hash index to impose the DISTINCT constraint).
 * 
 * TODO We are doing something very similar for <code>GRAPH ?g {}</code>. It
 * would be worth while to look at that code in the light of this optimizer.
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/1035" > DISTINCT PREDICATEs
 *      query is slow </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class ASTDistinctTermScanOptimizer implements IASTOptimizer {

   /**
     * 
     */
   public ASTDistinctTermScanOptimizer() {
   }

   @Override
   public QueryNodeWithBindingSet optimize(
      final AST2BOpContext context, final QueryNodeWithBindingSet input) {

      final IQueryNode queryNode = input.getQueryNode();
      final IBindingSet[] bindingSets = input.getBindingSets();     

      final QueryRoot queryRoot = (QueryRoot) queryNode;

      final StaticAnalysis sa = new StaticAnalysis(queryRoot, context);

      final DatasetNode dataset = queryRoot.getDataset();

      if (context.getAbstractTripleStore().isQuads()) {
         boolean ok = false;
         if (dataset == null || dataset.getNamedGraphs() == null) {
            /*
             * The dataset is all graphs.
             */
            ok = true;
         }

         if (!ok) {
            return new QueryNodeWithBindingSet(queryNode, bindingSets);
         }
      }
        
      // First, process any pre-existing named subqueries.
      {

         final NamedSubqueriesNode namedSubqueries = queryRoot
               .getNamedSubqueries();

         if (namedSubqueries != null) {

            // Note: works around concurrent modification error.
            final List<NamedSubqueryRoot> list = BOpUtility.toList(
                  namedSubqueries, NamedSubqueryRoot.class);

            for (NamedSubqueryRoot namedSubquery : list) {

               // Rewrite the named sub-select
               doSelectQuery(context, sa, (QueryRoot) queryNode, namedSubquery);

            }

         }

      }

      // rewrite the top-level select
      doSelectQuery(context, sa, (QueryRoot) queryNode, (QueryBase) queryNode);

      return new QueryNodeWithBindingSet(queryNode, bindingSets);
   }

   private void doRecursiveRewrite(final AST2BOpContext context,
         final StaticAnalysis sa, final QueryRoot queryRoot,
         final GraphPatternGroup<IGroupMemberNode> group) {

      final int arity = group.arity();

      for (int i = 0; i < arity; i++) {

         final BOp child = (BOp) group.get(i);

         if (child instanceof GraphPatternGroup<?>) {

            // Recursion into groups.
            doRecursiveRewrite(context, sa, queryRoot,
                  ((GraphPatternGroup<IGroupMemberNode>) child));

         } else if (child instanceof SubqueryRoot) {

            // Recursion into subqueries.
            final SubqueryRoot subqueryRoot = (SubqueryRoot) child;
            doRecursiveRewrite(context, sa, queryRoot,
                  subqueryRoot.getWhereClause());

            // rewrite the sub-select
            doSelectQuery(context, sa, queryRoot, (SubqueryBase) child);

         } else if (child instanceof ServiceNode) {

            // Do not rewrite things inside of a SERVICE node.
            continue;

         }

      }

   }

   /**
    * Attempt to rewrite the SELECT.
    * 
    * @param context
    * @param sa
    * @param queryRoot
    *           The top-level of the query.
    * @param queryBase
    *           Either a top-level query or a sub-query.
    */
   private void doSelectQuery(final AST2BOpContext context,
         final StaticAnalysis sa, final QueryRoot queryRoot,
         final QueryBase queryBase) {

      // recursion first.
      doRecursiveRewrite(context, sa, queryRoot, queryBase.getWhereClause());

      if (queryBase.getQueryType() != QueryType.SELECT) {
         return;
      }

      /*
       * Looking for SELECT ?var { triple-or-quads-pattern }
       * 
       * where ?var is one of the variables in that triple or quads pattern.
       */
      final ProjectionNode projection = queryBase.getProjection();

      if (!projection.isDistinct() && !projection.isReduced()) {
         /*
          * The distinct term scan automatically eliminates duplicates.
          * Therefore it is only allowable with SELECT DISTINCT or SELECT
          * REDUCED.
          */
         return;
      }

      if (projection.isEmpty())
         return;

      if (projection.arity() > 1)
         return;

      final AssignmentNode assignmentNode = projection.getExpr(0);

      if (!(assignmentNode.getValueExpressionNode() instanceof VarNode)) {
         /*
          * The projection needs to be a single, simple variable.
          */
         return;
      }

      final IVariable<?> projectedVar = assignmentNode.getVar();

      /**
       * Looking for a single triple or quad pattern in the WHERE clause.
       */

      final GraphPatternGroup<IGroupMemberNode> whereClause = 
            queryBase.getWhereClause();

      if (whereClause == null || whereClause.arity() != 1) {
         // Not simple triple pattern.
         return;
      }

      if (!(whereClause.get(0) instanceof StatementPatternNode)) {
         // Not simple triple pattern.
         return;
      }

      // The single triple pattern.
      final StatementPatternNode sp = (StatementPatternNode) whereClause.get(0);

      /*
       * When in history mode, we can't do a distinct term scan unless the 
       * StatementPatternNode has been marked to read history.  The distinct
       * term scan will visit terms that might have been deleted.
       */
      if (context.getAbstractTripleStore().isRDRHistory()) {
          
          if (!sp.getQueryHintAsBoolean(QueryHints.HISTORY, false)) {
              // Can not rewrite.
              return;
          }
          
      }
      
      IKeyOrder<ISPO> keyOrder = getApplicableKeyOrderIfExists(sp,
            projectedVar, context);
      if (keyOrder == null) {
         return;
      }

      /*
       * Make sure that there are no correlated variables in the SP.
       */
      {
         final Set<VarNode> vars = new LinkedHashSet<VarNode>();

         for (VarNode varNode : BOpUtility.toList(sp, VarNode.class)) {

            if (!vars.add(varNode)) {

               // This variable appears more than once.
               return;

            }

         }

      }

      final Set<IVariable<?>> producedBindings = sp.getProducedBindings();

      if (!producedBindings.contains(projectedVar)) {
         /*
          * The projected variable is not any of the variables used in the
          * triple pattern.
          * 
          * Note: This rewrite is only advantageous when a single variable is
          * bound by the triple pattern is projected out of the query (or
          * perhaps when 2 variables are bound by a quad pattern and projected
          * out of the query). The benefit of this rewrite is that we can avoid
          * binding the variables that are NOT projected out of the query.
          * 
          * TODO Does this already handle named graph APs?
          */
         return;
      }

      if (queryBase instanceof SubqueryRoot) {

         /*
          * The pattern is detected is a sub-select.
          * 
          * Mark the SELECT as "run-once". This will cause it to be lifted out
          * as a named subquery. This is how we enforce bottom-up evaluation. In
          * this case it is REQUIRED since the semantics of DISTINCT / REDUCED
          * would not be enforced across multiple as-bound invocations of the
          * rewritten sub-SELECT.
          * 
          * FIXME Make sure that we have a correctness test for the case of an
          * embedded sub-select where the DISTINCT semantics would otherwise
          * break. (We are REQUIRED to use bottom-up evaluation in order to have
          * the semantics of DISTINCT/REDUCED across the SELECT if this clause
          * is appearing as a sub-SELECT. Otherwise the sub-SELECT could be
          * evaluated for multiple source bindings leading to multiple
          * applications of the distinct-term-scan and that would break the
          * DISTINCT/REDUCED semantics of the operator.)
          */
         ((SubqueryRoot) queryBase).setRunOnce(true/* runOnce */);

      }

      /*
       * Disable DISTINCT/REDUCED. The distinct-term-scan will automatically
       * enforce this.
       */
      projection.setDistinct(false);
      projection.setReduced(false);

      /*
       * Setup the distinct-term-scan annotation with the variables that will be
       * projected out of the SELECT.
       */
      final VarNode distinctTermScanVar = new VarNode(projectedVar.getName());
      sp.setDistinctTermScanVar(distinctTermScanVar);
      sp.setQueryHint(IPredicate.Annotations.KEY_ORDER, keyOrder.toString());

      /**
       * Change the estimated cardinality.
       * 
       * The new cardinality is:
       * 
       * <pre>
       * newCard = oldCard * 1.0 / arity(context, sp)
       * </pre>
       * 
       * where arity() is 3 for triples and 4 for quads.
       */
      final Long oldCard = (Long) sp
            .getProperty(AST2BOpBase.Annotations.ESTIMATED_CARDINALITY);

      if (oldCard == null) {
         throw new AssertionError(
               "Expecting estimated-cardinality to be bound: sp=" + sp);
      }
      final int arity = context.isQuads() ? 4 : 3;
      final long newCard = (long) (1.0 / arity);

      sp.setProperty(AST2BOpBase.Annotations.ESTIMATED_CARDINALITY, newCard);

   }

   /**
    * Computes an applicable key order for performing a distinct range term
    * scan, if exists. Such a key order must be formed out of a prefix
    * [ConstList + DistinctVar], where ConstList is the list of constants in the
    * triple pattern.
    * 
    * @param sp
    * @param context
    * @return matching key order, if exists, null if not (indicating failure)
    */
   private IKeyOrder<ISPO> getApplicableKeyOrderIfExists(
         StatementPatternNode sp, IVariable<?> termScanVar,
         AST2BOpContext context) {

      boolean isQuads = context.getAbstractTripleStore().isQuads();

      // first, construct a predicate for index probing
      IVariableOrConstant[] args = new IVariableOrConstant[isQuads ? 4 : 3];
      args[0] = sp.s().getValueExpression();
      args[1] = sp.p().getValueExpression();
      args[2] = sp.o().getValueExpression();
      if (isQuads) {
         args[3] = sp.c() == null ? Var.var("--anon-" + context.nextId()) : sp
               .c().getValueExpression();
      }

      Set<SPOKeyOrder> candidateKeyOrder = getCandidateKeyOrders(sp,
            termScanVar, context, isQuads);
      if (candidateKeyOrder.isEmpty()) {
         return null;
      } else {
         return candidateKeyOrder.iterator().next();
      }
   }

   /**
    * Return all candidate key orders. Candidate key orders must satisfy the
    * condition that the constants in the triple pattern form a prefix, followed
    * by the term scan variable in the position right behind the constant
    * positions.
    * 
    * @param sp
    * @param termScanVar
    * @param context
    * @param isQuads
    * @return
    */
   private Set<SPOKeyOrder> getCandidateKeyOrders(StatementPatternNode sp,
         IVariable<?> termScanVar, AST2BOpContext context, boolean isQuads) {

      /**
       * Constraints on the positions are as follows:
       * 
       * 2 - constant 1 - the distinct term scan var 0 - unconstrained
       */
      final StringBuffer constantPosBuf = new StringBuffer();
      Character distinctTermScanPos = null;
      final StringBuffer unconstrainedPosBuf = new StringBuffer();

      final int pcS = getPositionConstraint(sp.s().getValueExpression(), termScanVar);
      final int pcP = getPositionConstraint(sp.p().getValueExpression(), termScanVar);
      final int pcO = getPositionConstraint(sp.o().getValueExpression(), termScanVar);

      if (pcS == 2)
         constantPosBuf.append("S");
      if (pcP == 2)
         constantPosBuf.append("P");
      if (pcO == 2)
         constantPosBuf.append("O");
      if (pcS == 1)
         distinctTermScanPos = 'S';
      if (pcP == 1)
         distinctTermScanPos = 'P';
      if (pcO == 1)
         distinctTermScanPos = 'O';
      if (pcS == 0)
         unconstrainedPosBuf.append("S");
      if (pcP == 0)
         unconstrainedPosBuf.append("P");
      if (pcO == 0)
         unconstrainedPosBuf.append("O");

      if (isQuads) {
         if (sp.c() == null || sp.c().getValueExpression() == null) {
            unconstrainedPosBuf.append("C");
         } else {
            int pcC = getPositionConstraint(sp.c().getValueExpression(),
                  termScanVar);
            if (pcC == 2)
               constantPosBuf.append("C");
            if (pcC == 1)
               distinctTermScanPos = 'C';
            if (pcC == 0)
               unconstrainedPosBuf.append("C");
         }
      }
      
      /**
       * There's some bug if all but one of the positions are constrained,
       * we do *not* want to apply the optimization in that case (though it
       * should be possible). For now, the fix is to not optimize, while in
       * future we may want to address the root cause of the issue.
       * 
       * See https://jira.blazegraph.com/browse/BLZG-1346.
       */
      final int maxLength = isQuads ? 2 : 1;
      if (constantPosBuf.length() > maxLength) {
         return new LinkedHashSet<SPOKeyOrder>();
      }

      

      final String prefix = constantPosBuf.toString();
      final Set<String> allPossibleConstPrefixes = new LinkedHashSet<String>();
      getPermutations(prefix, allPossibleConstPrefixes);
      if (allPossibleConstPrefixes.isEmpty())
         allPossibleConstPrefixes.add(""); // neutral element

      final String suffix = unconstrainedPosBuf.toString();
      final Set<String> allPossibleConstSuffixes = new LinkedHashSet<String>();
      getPermutations(suffix, allPossibleConstSuffixes);
      if (allPossibleConstSuffixes.isEmpty())
         allPossibleConstSuffixes.add(""); // neutral element

      // calculate set of all key order candidates
      final Set<SPOKeyOrder> allPossiblePrefixes = 
         new LinkedHashSet<SPOKeyOrder>();
      for (String constPrefix : allPossibleConstPrefixes) {
         for (String constSuffix : allPossibleConstSuffixes) {
            final String index = constPrefix + distinctTermScanPos + constSuffix;
            try {
               allPossiblePrefixes.add(SPOKeyOrder.fromString(index));
            } catch (IllegalArgumentException e) {
               // key order does not exist, ignoring
            }

         }
      }

      return allPossiblePrefixes;
   }

   /**
    * Returns a constraint ID defined as follows:
    * 
    * 2 - constant 1 - the distinct term scan var 0 - unconstrained
    */
   private int getPositionConstraint(IVariableOrConstant val,
         IVariable<?> termScanVar) {
      if (val instanceof IConstant) {
         return 2;
      } else if (val instanceof IVariable) {
         return val.equals(termScanVar) ? 1 : 0;
      } else {
         return 0; // val == null
      }
   }

   
   private void getPermutations(String str, Set<String> collector) {
      getPermutations("", str, collector);
   }

   
   private void getPermutations(
      String prefix, String str, Set<String> collector) {
      
      final int n = str.length();
      if (n == 0)
         collector.add(prefix);
      else {
         for (int i = 0; i < n; i++)
            getPermutations(prefix + str.charAt(i),
                  str.substring(0, i) + str.substring(i + 1, n), collector);
      }
   }
}

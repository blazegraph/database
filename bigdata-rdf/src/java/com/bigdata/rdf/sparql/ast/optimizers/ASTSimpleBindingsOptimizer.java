package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Removed simple {@link AssignmentNode} in graph patterns, adding them directly
 * to the bindingSet. Note that ASTBind nodes (i.e., BIND expressions) in
 * the query are translated into {@link AssignmentNode} in the bigdata model.
 * Assignment nodes in graph patterns that bind variables to constants
 * (i.e., do not require evaluation at runtime) can be safely removed when
 * adding the binding to the bindingSet. Note that this optimizer clears the
 * way for further optimizations based on the {@link ASTBindingAssigner}, which
 * replaces bound variables inside the query.
 * 
 * Example: a query with WHERE clause
 *
 * <code>WHERE { BIND ( <http://my.uri.com> as ?uri ) ... }<code>
 * 
 * exhibits an optimized AST of the form
 * 
 * <code>
   JoinGroupNode {
     ( ConstantNode(TermId(X)[http://my.uri.com]) AS VarNode(uri) )
     ...
   }
   </code>
 * 
 * We then add the binding ?uri -> ConstantNode(TermId(X)[http://my.uri.com])
 * to the binding set and remove the {@link AssignmentNode} from the query.
 * The {@link ASTBindingAssigner} will then replace occurrences of variable
 * ?uri through a {@link Constant} node for more efficient evaluation.
 * 
 * @see <a href="http://trac.bigdata.com/ticket/653">Slow query with bind</a>
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * 
 */
public class ASTSimpleBindingsOptimizer implements IASTOptimizer {

   @Override
   public IQueryNode optimize(AST2BOpContext context, IQueryNode queryNode,
         IBindingSet[] bindingSets) {

      /**
       * In case the binding sets variable is null, nothing needs to be done 
       */
      if (bindingSets == null) {
         return queryNode;
      }
      
      if (!(queryNode instanceof QueryRoot))
         return queryNode;

      final QueryRoot queryRoot = (QueryRoot) queryNode;

      /**
       * One restriction is that the query must not have a VALUES
       * clause at top level, as this may conflict with pulling bindings
       * outside into the bindingsSet (again, this is a bit strict, but we
       * consider this an edge case).
       */
      {
         if (queryRoot.getBindingsClause()!=null) {
            return queryNode;
         }
      }
      
      
      /**
       *  We only apply this optimization at top-level, since the
       * binding set applies to top-level constructs (adding bindings for
       * variables in sub-queries might cause problems for same-named 
       * variables at top level).
       *
       * Note that this assumption is a bit too strict, as there are cases
       * in which the optimization is also applicable for subqueries (e.g., 
       * if the variable names are disjoint, or whenever we do alpha-renaming
       * of the variables. However, BIND clauses in subqueries is considered
       * an edge case which probably is not worth the effort.
       */
      {
         @SuppressWarnings("unchecked")
         final GroupNodeBase<IGroupMemberNode> whereClause = 
            (GroupNodeBase<IGroupMemberNode>) queryRoot.getWhereClause();

         if (whereClause != null) {

            doOptimize(whereClause, bindingSets);

         }

      }      
      
      return queryNode;
   }

   private void doOptimize(
      GroupNodeBase<IGroupMemberNode> whereClause, IBindingSet[] bindingSets) {

      // first collect simple assignment nodes assigning vars to constants
      Map<IVariable,IConstant> bindings = new HashMap<IVariable, IConstant>();
      Set<AssignmentNode> simpleAssignmentNodes = new HashSet<AssignmentNode>();
      for (int i=whereClause.arity()-1; i>=0; i--) {
         final BOp op = whereClause.get(i);
         if (op instanceof AssignmentNode) {
            final AssignmentNode an = (AssignmentNode)op;
            final IValueExpression<?> ve = an.getValueExpression();
            if (ve instanceof IConstant) {
               simpleAssignmentNodes.add(an);
            }
         }
         
      }

      // next, remove the assignment nodes from the query (taking care of
      // potential duplicates)
      for (AssignmentNode simpleAssignmentNode : simpleAssignmentNodes) {
         while (whereClause.removeArg(simpleAssignmentNode)) {
            // repeat
         }
      }
      
      // finally, extend the binding set
      for (int i=0; i<bindingSets.length; i++) {
         for (AssignmentNode simpleAssignmentNode : simpleAssignmentNodes) {

            IBindingSet bs = bindingSets[i];
            
            // the following cast works by design, cf. check before assigning
            // assignment node to simpleAssignmentNodes array
            IConstant bindingValue = 
               (IConstant)simpleAssignmentNode.getValueExpression();
            bs.set(simpleAssignmentNode.getVar(), bindingValue);
         }
      }
   }
}

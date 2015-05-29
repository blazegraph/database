package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.optimizers.ASTStaticBindingsOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.IASTOptimizer;

/**
 * Class for wrapping results of an {@link IASTOptimizer}. Typically,
 * optimizer do only modify the AST, but in some cases they may involve
 * manipulation of the base mapping set (see for instance the
 * {@link ASTStaticBindingsOptimizer}).
 * 
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * 
 */
public class ASTOptimizerResult {
   
   private final IQueryNode optimizedQueryNode;
   private final IBindingSet[] optimizedBindingSet;

   public ASTOptimizerResult(
      IQueryNode optimizedQueryNode, IBindingSet[] optimizedBindingSet) {
      
      this.optimizedQueryNode = optimizedQueryNode;
      this.optimizedBindingSet = optimizedBindingSet;
   }
   
   public IQueryNode getOptimizedQueryNode() {
      return optimizedQueryNode;
   }

   public IBindingSet[] getOptimizedBindingSet() {
      return optimizedBindingSet;
   }
}

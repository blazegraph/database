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
 * Created on Aug 28, 2015
 */
package com.bigdata.rdf.sparql.ast.explainhints;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;

/**
 * Explain hint indicating potential problems caused by the bottom-up evaluation
 * semantics of SPARQL. These could, for instance, be FILTER or BIND expressions
 * referencing variables that are not in scope.
 * 
 * @author <a href="ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class BottomUpSemanticsExplainHint extends ExplainHint {

   private static final String EXPLAIN_HINT_TYPE = "Bottom-up Semantics";
   
   private static final String DESCRIPTION1 = 
         "SPARQL semantics is defined bottom up, and in the query " +
         "we detected a variable that is used in a value expression " +
         "but known not to be in scope when evaluating the value " +
         "expression. To fix the problem, you may want to push the construct " +
         "binding the variable (this might be a triple pattern, BIND, or " +
         "VALUES clause) inside the scope in which the variable is used. " +
         "The affected variable is '";
   private static final String DESCRIPTION2 = 
      "', which has been renamed in the optimized AST to '";
   private static final String DESCRIPTION3 = "' in order to avoid conflicts.";

   public BottomUpSemanticsExplainHint(
      final IVariable<?> original, final IVariable<?> renamed,
      final BOp explainHintASTBase) {
      
      super(
         DESCRIPTION1 + original + DESCRIPTION2 + renamed + DESCRIPTION3, 
         EXPLAIN_HINT_TYPE, 
         ExplainHintCategory.CORRECTNESS,
         ExplainHintSeverity.SEVERE,
         explainHintASTBase);
      
   }

}

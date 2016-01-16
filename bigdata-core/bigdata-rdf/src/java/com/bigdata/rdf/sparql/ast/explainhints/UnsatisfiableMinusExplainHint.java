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

/**
 * Explain hint indicating a MINUS expression that is unsatisfiable, i.e. does
 * not have any effect at all. This happens if the left- and right-hand side
 * of the minus have no variables in common. In such cases, the minus is
 * optimized away, and there's most likely a problem in the query.
 * 
 * @author <a href="ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class UnsatisfiableMinusExplainHint extends ExplainHint {

   private static final String EXPLAIN_HINT_TYPE = "Unsatisfiable Minus";
   
   private static final String EXPLAIN_HINT_DESCRIPTION =
      "The referenced AST node has been eliminated, because the MINUS " +
      "expression that it represents has no effect. The reason is that " +
      "the left-hand side expressiona and the minus expression have no " +
      "shared variables. You may consider using FILTER NOT EXISTS as an " +
      "alternative, see section http://www.w3.org/TR/2013/REC-sparql11-query-"
      + "20130321/#neg-notexists-minus in the SPARQL 1.1 standard.";
   
   public UnsatisfiableMinusExplainHint(final BOp explainHintASTBase) {
         
      super(
         EXPLAIN_HINT_DESCRIPTION, 
         EXPLAIN_HINT_TYPE, 
         ExplainHintCategory.CORRECTNESS,
         ExplainHintSeverity.MODERATE,
         explainHintASTBase);
   }
}

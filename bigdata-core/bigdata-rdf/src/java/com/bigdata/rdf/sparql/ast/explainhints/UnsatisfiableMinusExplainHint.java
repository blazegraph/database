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
 * Created on Aug 28, 2015
 */
package com.bigdata.rdf.sparql.ast.explainhints;

import org.openrdf.query.parser.serql.ast.ASTNode;

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

   private static final String EXPLAIN_HINT_TYPE = "Unsatisfiable MINUS";
   
   public UnsatisfiableMinusExplainHint(
      final String explainHintDescription, final ASTNode explainHintASTNode) {
         
      super(
         explainHintDescription, 
         EXPLAIN_HINT_TYPE, 
         ExplainHintCategory.CORRECTNESS,
         ExplainHintSeverity.MODERATE,
         explainHintASTNode);
   }
}

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

import com.bigdata.rdf.sparql.ast.IGroupMemberNode;

/**
 * Explain hint indicating potential problems caused by the join order within
 * a given join group, such as non-optional non-minus nodes that could not be
 * moved in front of optionals. Such constructs are often undesired and may
 * cause performance bottlenecks.
 * 
 * @author <a href="ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class JoinOrderExplainHint extends ExplainHint {

   private static final String EXPLAIN_HINT_TYPE = "Join Order";
   
   public static final String ACROSS_PARTITION_REORDERING_PROBLEM = 
      "The referenced AST node cannot be moved in front of an " +
      "OPTIONAL or MINUS expression. Blazegraph tries to do " +
      "so in order to assert best performance, but in this " +
      "it seems like query semantics might change if we move " +
      "the node. Please review your query and move the node to " +
      "in front of all OPTIONAL or MINUS nodes manually if " + 
      "this is what you wanted to implement. Note that this " +
      "hint does not imply that your query is wrong, but there " +
      "might be a problem with it.";
   
   public JoinOrderExplainHint(
      final String explainHintDescription, final IGroupMemberNode candidate) {
         
      super(
         explainHintDescription, 
         EXPLAIN_HINT_TYPE, 
         ExplainHintCategory.CORRECTNESS,
         ExplainHintSeverity.MODERATE,
         candidate);
   }
}

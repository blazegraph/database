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
 * Hint to be interpreted by EXPLAIN, containing information to be exposed
 * to the user. Such information could be potential performance bottlenecks
 * identified throughout the query optimization phase, or potential problems
 * due to SPARQL's bottom-up semantics.
 * 
 * @author <a href="ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public interface IExplainHint {

   /**
    * Enum datatype specifing the severity of a given explain hint
    */
   public enum ExplainHintSeverity {
      SEVERE,     /* could well/likely be a severe problem */
      MODERATE,   /* could be a problem, but not critical */
      INFO        /* informative explain hint */
   }

   /**
    * Enum datatype categorizing an explain hint 
    */
   public enum ExplainHintCategory {
      CORRECTNESS,   /* hint regarding the correctness of a query construct */
      PERFORMANCE,   /* hint indicating performance related issues */
      OTHER          /* anything unclassified */
   }

   /**
    * @return textual representation of the explain hint type
    */
   public String getExplainHintType();

   /**
    * @return severity of an explain hint
    */
   public ExplainHintSeverity getExplainHintSeverity();
   
   /**
    * @return category of an explain hint
    */
   public ExplainHintCategory getExplainHintCategory();
   
   /**
    * @return a detailed description, representing the content of the hint
    */
   public String getExplainHintDescription();
   
   /**
    * @return the node affected by the explain node; note that we attach
    *         this node explicitly, since the node to which the hint is attached
    *         is not always the affected node, for instance if the affected node
    *         has been optimized away
    */
   public BOp getExplainHintNode();

   /**
    * @return a link to an external help page.
    */
   public String getHelpLink();
   
}

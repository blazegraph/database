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
 * Abstract base class for explain hints, providing a common base
 * implementations, to be reused in concrete subclasses.
 * 
 * @author <a href="ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public abstract class ExplainHint implements IExplainHint {

   /**
    * By default, the links are composed according to the following scheme:
    * a link to the Wiki's Explain summary page, where we have a subsection
    * named according to the {@link #getExplainHintType()}. This is, whenever
    * you implement a new {@link ExplainHint} subclass, make sure to include
    * a respective subsection in the Wiki.
    */
   private final String LINK_BASE = 
      "https://wiki.blazegraph.com/wiki/index.php/Explain#";
   
   private final String explainHintDescription;
   
   private final String explainHintType;
   
   private final ExplainHintCategory explainHintCategory;
   
   private final ExplainHintSeverity explainHintSeverity;
   
   private final BOp explainHintNode;
   
   /**
    * Constructor
    */
   public ExplainHint(
      final String explainHintDescription, final String explainHintType,
      final ExplainHintCategory explainHintCategory, 
      final ExplainHintSeverity explainHintSeverity,
      final BOp explainHintNode) {
      
      this.explainHintDescription = explainHintDescription;
      this.explainHintType = explainHintType;
      this.explainHintCategory = explainHintCategory;
      this.explainHintSeverity = explainHintSeverity;
      this.explainHintNode = explainHintNode;
   }
   
   @Override
   public String toString() {
      
      final StringBuffer buf = new StringBuffer();
      buf.append("Type: " + getExplainHintType());
      buf.append("\n");
      buf.append("Category: " + getExplainHintCategory());
      buf.append("\n");
      buf.append("Severity: " + getExplainHintSeverity());
      buf.append("\n");
      buf.append("Description: " + getExplainHintDescription());
      buf.append("\n");
      buf.append("Node: " + getExplainHintNode());
      
      return buf.toString();
   }

   @Override
   public String getExplainHintType() {
      return explainHintType;
   }
   
   @Override
   public ExplainHintCategory getExplainHintCategory() {
      return explainHintCategory;
   }
   
   @Override
   public ExplainHintSeverity getExplainHintSeverity() {
      return explainHintSeverity;
   }

   @Override
   public String getExplainHintDescription() {
      return explainHintDescription;
   }
   
   @Override
   public BOp getExplainHintNode() {
      return explainHintNode;
   }

   @Override
   public String getHelpLink() {
      return LINK_BASE + getExplainHintType().replaceAll(" ", "_");
   }
   

}

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
 * Created on May 14, 2015
 */
package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.optimizers.ASTStaticBindingsOptimizer;

/**
 * Class for wrapping a query node and an associated binding set (as starting
 * point for evaluating the query). Mainly used in the context of the AST
 * optimizer pipeline. Typically, these optimizers do only modify the AST,
 * but in some cases they may involve manipulation of the binding sets
 * (see for instance the {@link ASTStaticBindingsOptimizer}).
 * 
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * 
 */
public class QueryNodeWithBindingSet {
   
   private final IQueryNode queryNode;
   private final IBindingSet[] bindingSets;

   public QueryNodeWithBindingSet(
      IQueryNode queryNode, IBindingSet[] bindingSets) {
      
      this.queryNode = queryNode;
      this.bindingSets = bindingSets;
   }
   
   public IQueryNode getQueryNode() {
      return queryNode;
   }

   public IBindingSet[] getBindingSets() {
      return bindingSets;
   }
}

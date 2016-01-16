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
 * Created on June 20, 2015
 */
package com.bigdata.rdf.sparql.ast;

import java.util.HashMap;
import java.util.Map;

import com.bigdata.rdf.sparql.ast.optimizers.ASTJoinGroupFilterExistsInfo;



/**
 * Map from nodes to their respective {@link GroupNodeVarBindingInfo} object,
 * including setup method.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class GroupNodeVarBindingInfoMap {
   
   final private Map<IGroupMemberNode, GroupNodeVarBindingInfo> bindingInfo;
   
   
   /**
    * Constructor, setting up an object given a list of {@link IGroupMemberNode}
    * objects and the associated {@link StaticAnalysis} object as input.
    * 
    * @param nodes
    */
   public GroupNodeVarBindingInfoMap(
      final Iterable<IGroupMemberNode> nodes, 
      final StaticAnalysis sa,
      final ASTJoinGroupFilterExistsInfo fExInfo) {

      bindingInfo = new HashMap<IGroupMemberNode, GroupNodeVarBindingInfo>();
      for (IGroupMemberNode node : nodes) {
         
         final GroupNodeVarBindingInfo vbc = 
            new GroupNodeVarBindingInfo(node, sa, fExInfo);
         bindingInfo.put(node,vbc); 
         
      }
      
            
   }
   /**
    * Get the {@link GroupNodeVarBindingInfo} for the given node. Returns null
    * if the node is not registered.
    */
   public GroupNodeVarBindingInfo get(IGroupMemberNode node) {
      return bindingInfo.get(node);
   }
   
}

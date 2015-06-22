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
 * Created on June 22, 2015
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.bigdata.rdf.sparql.ast.IGroupMemberNode;

/**
 * Classification of {@link IGroupMemberNode}s along a set of specified
 * types. For nodes matching a given type, lookup is possible (returning
 * an ordered list of nodes), all other nodes are stored in a dedicated list.
 * @author msc
 *
 */
class ASTTypeBasedNodeClassifier {
   
   Class<?>[] clazzez;
   
   List<IGroupMemberNode> unclassifiedNodes;
   
   Map<Class<?>,List<IGroupMemberNode>> classifiedNodes;
   
   /**
    * Constructor, receiving as an argument a list of types based on
    * which classification is done. 
    * 
    * @param types
    */
   public ASTTypeBasedNodeClassifier(
      final Class<?>[] clazzez, final List<IGroupMemberNode> nodeList) {
      
      this.clazzez = clazzez;
      unclassifiedNodes = new LinkedList<IGroupMemberNode>();
      classifiedNodes = new HashMap<Class<?>, List<IGroupMemberNode>>();
      
      registerNodes(nodeList);
   }
   

   public void registerNodes(final List<IGroupMemberNode> nodeList) {
      
      // initialize map with empty arrays
      for (Class<?> clazz : clazzez) {
         classifiedNodes.put(clazz, new LinkedList<IGroupMemberNode>());
      }

      // and popuplate it
      for (IGroupMemberNode node : nodeList) {

         boolean classified = false;
         for (int i=0; i<clazzez.length && !classified; i++) {
            
            Class<?> clazz = clazzez[i];
            if (clazz.isInstance(node)) {
               classifiedNodes.get(clazz).add(node);
               classified = true;
            }               
         }
         
         if (!classified) {
            unclassifiedNodes.add(node);
         }
      }
   }
   
   /**
    * Return all those nodes for which classification failed.
    */
   public List<IGroupMemberNode> getUnclassifiedNodes() {
      return unclassifiedNodes;
   }
   
   /**
    * Returns the list of nodes that are classified with the given type.
    * If the type was passed when constructing the object, the result
    * is the (possibly empty) list of nodes with the given type. If the
    * type was not provided, null is returned.
    */
   public List<IGroupMemberNode> get(Class<?> clazz) {
      return classifiedNodes.get(clazz);
   }
   
}

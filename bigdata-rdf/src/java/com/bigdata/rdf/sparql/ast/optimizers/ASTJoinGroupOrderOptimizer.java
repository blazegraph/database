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
 * Created on June 12, 2015
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.ArbitraryLengthPathNode;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.BindingsClause;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.GroupNodeVarBindingInfo;
import com.bigdata.rdf.sparql.ast.GroupNodeVarBindingInfoMap;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.PropertyPathUnionNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.ZeroLengthPathNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.service.ServiceFactory;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;


/**
 * This optimizer brings a join group node into a valid order according to the
 * SPARQL 1.1 semantics and optimizes the order of the nodes in the join group
 * using various heuristics.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
@SuppressWarnings("deprecation")
public class ASTJoinGroupOrderOptimizer extends AbstractJoinGroupOptimizer 
implements IASTOptimizer {
   
   @Override
   protected void optimizeJoinGroup(AST2BOpContext ctx, StaticAnalysis sa,
         IBindingSet[] bSets, JoinGroupNode joinGroup) {

      if (!ASTStaticJoinOptimizer.isStaticOptimizer(ctx, joinGroup))
         return;

      /**
       * Initialize summary containers with a single pass over the children;
       * they allow for efficient lookup of required information in the
       * following.
       */
            
      final Set<IVariable<?>> externallyIncoming = 
         sa.getDefinitelyIncomingBindings(
            joinGroup, new HashSet<IVariable<?>>());
      
      final GroupNodeVarBindingInfoMap bindingInfoMap =
         new GroupNodeVarBindingInfoMap(joinGroup, sa);
      
      
      /**
       * Filter out the filter nodes from the join group, they will receive
       * special handling in the end.
       */
      final TypeBasedNodeClassifier filterNodeClassifier = 
         new TypeBasedNodeClassifier(
            new Class<?>[]{ FilterNode.class }, joinGroup.getChildren());

      
      // special handling: if there are no non-filter nodes, we can skip all
      // the rest
      if (filterNodeClassifier.getUnclassifiedNodes().isEmpty()) {
         return;
      }
      
      /**
       * Set up the partitions, ignoring the FILTER nodes (they'll be added
       * in the end, after having flattened the partitions again).
       */
      final ASTJoinGroupPartitions partitions = 
         new ASTJoinGroupPartitions(
            filterNodeClassifier.getUnclassifiedNodes() /* non filter nodes */,
            bindingInfoMap, externallyIncoming);
      
      /**
       * First, optimize across the partitions, trying to move forward
       * non-optional non-minus patterns wherever possible. It is important to
       * do this first, before reordering within partitions, as it shifts
       * nodes around across them.
       */
      optimizeAcrossPartitions(
            partitions, bindingInfoMap, externallyIncoming);
      
      /** 
       * Second, optimize within the individual partitions.
       */
      optimizeWithinPartitions(partitions);


      /**
       * ... finally place the filters ...
       */
      for (IGroupMemberNode node : 
            filterNodeClassifier.get(FilterNode.class)) {
         partitions.placeAtFirstPossiblePosition(node);
      }
      
      
      /**
       * Now, flatten the partitions again and replace the children of the
       * join group with the new list.
       */
      final LinkedList<IGroupMemberNode> nodeList = 
         partitions.extractNodeList();
      for (int i = 0; i < joinGroup.arity(); i++) {
          joinGroup.setArg(i, (BOp) nodeList.get(i));
      }

      
      // TODO: creation of subgroups at "borders"

   }




  /**
   * Moves the nodes contained in the set as far to the beginning of the join
   * group as possible without violating the semantics. In particular, this
   * function takes care to not "skip" OPTIONAL and MINUS constructs when
   * it would change the outcome of the query.
   * 
   * @param set a set of nodes
   */
  void optimizeAcrossPartitions(
     final ASTJoinGroupPartitions partitions,
     final GroupNodeVarBindingInfoMap bindingInfoMap,      
     final Set<IVariable<?>> externallyKnownProduced) {      

     
     final List<ASTJoinGroupPartition> partitionList = 
        partitions.getPartitionList();
     
     /**
      * In the following list, we store the variables that are definitely
      * produced *before* evaluating a partition. We maintain this set
      * for fast lookup.
      */
     final List<Set<IVariable<?>>> definitelyProducedUpToPartition =
         new ArrayList<Set<IVariable<?>>>(partitionList.size());
     
     final Set<IVariable<?>> producedUpToPartition =
        new HashSet<IVariable<?>>(externallyKnownProduced);
     for (int i=0; i<partitionList.size();i++) {

        // we start out with the second partitions, so this will succeed
        if (i>0) {
           producedUpToPartition.addAll(
              partitionList.get(i-1).getDefinitelyProduced());
        }        
        
        definitelyProducedUpToPartition.add(
           new HashSet<IVariable<?>>(producedUpToPartition));
        
     }
     
     /**
      * Having initialized the map now, we iterate over the patterns in the
      * partitions and try to shift them to the first possible partition.
      * My intuition is that the algorithm is optimal in the sense that,
      * after running it, every node is in the firstmost partition where
      * it can be safely placed.
      */
     for (int i=1; i<partitionList.size(); i++) {
        
        final ASTJoinGroupPartition partition = partitionList.get(i);
        
        final List<IGroupMemberNode> unmovableNodes = 
              new ArrayList<IGroupMemberNode>();
        for (IGroupMemberNode candidate : partition.nonOptionalNonMinusNodes) {
           
           // find the firstmost partition in which the node can be moved
           Integer partitionForCandidate = null;
           for (int j=i-1; j>=0; j--) {
              
              final ASTJoinGroupPartition candPartition = partitionList.get(j);
              
              /**
               * Calculate the conflicting vars as the intersection of the
               * maybe vars of the bordering OPTIONAL or MINUS with the maybe
               * vars of the node to move around, minus the nodes that are
               * known to be bound upfront.
               */
              final Set<IVariable<?>> conflictingVars;
              if (candPartition.optionalOrMinus == null) {
                 conflictingVars = new HashSet<IVariable<?>>();
              } else {
                 conflictingVars = 
                    new HashSet<IVariable<?>>(
                       bindingInfoMap.get(
                          candPartition.optionalOrMinus).getMaybeProduced());
              }

              conflictingVars.retainAll(bindingInfoMap.get(candidate).getMaybeProduced());
              
              conflictingVars.removeAll(
                 definitelyProducedUpToPartition.get(j));
              
              if (conflictingVars.isEmpty()) {
                 partitionForCandidate = j;
              } else {
                 // can't place here, abort
                 break;
              }
              
           }
           
           if (partitionForCandidate!=null) {

              // add the node to the partition (removal will be done later on)
              final ASTJoinGroupPartition partitionToMove = 
                 partitionList.get(partitionForCandidate);
              partitionToMove.addNonOptionalNonMinusNodeToPartition(candidate);
              
              /**
               * Given that the node has been moved to partitionForCandidate,
               * the definitelyProducedUpToPartition needs to be updated for
               * all partitions starting at the partition following the 
               * partitionForCandidate, up to the partition i, which contained
               * the node before (later partitions carry this info already).
               * 
               * Note that k<=i<partitions.size() guarantees that we don't run
               * into an index out of bound exception.
               */
             for (int k=partitionForCandidate+1; k<=i; k++) {
                 definitelyProducedUpToPartition.get(k).addAll(
                    bindingInfoMap.get(candidate).getDefinitelyProduced());
              }
              
              // the node will be removed from the current partition at the end
              
           } else {
              unmovableNodes.add(candidate);
           }
           
        }

        // the nodes that remain in this position are the unmovable nodes
        partition.replaceNonOptionalNonMinusNodesWith(unmovableNodes, true);
     }
     
  }

   /**
    * Optimize the order of nodes within a single partition. The nice thing
    * about partitions is that we can freely reorder the non-optional
    * non-minus nodes within them. 
    */
   void optimizeWithinPartitions(final ASTJoinGroupPartitions partitions) {
 
      final List<ASTJoinGroupPartition> partitionList = 
         partitions.getPartitionList();
      
      final Set<IVariable<?>> knownBoundFromPrevPartitions =
         new HashSet<IVariable<?>>();
      for (ASTJoinGroupPartition partition : partitionList) {
         
         final TypeBasedNodeClassifier classifier = 
            new TypeBasedNodeClassifier(
               new Class<?>[] { 
                  ServiceNode.class,AssignmentNode.class,BindingsClause.class }, 
               partition.extractNodeList());
         
         /**
          * In a first step, we remove service nodes, assignment nodes, and
          * bindings clauses from the partition. They will be handled in a
          * special way.
          */
         final List<IGroupMemberNode> toRemove = 
            new ArrayList<IGroupMemberNode>();
         toRemove.addAll(classifier.get(ServiceNode.class));
         toRemove.addAll(classifier.get(AssignmentNode.class));
         toRemove.addAll(classifier.get(BindingsClause.class));
         partition.removeNodesFromPartition(toRemove);
         
         // the remaining elements will be reordered based on their type
         final IASTJoinGroupPartitionReorderer reorderer =
               new TypeBasedASTJoinGroupPartitionReorderer();
         reorderer.reorderNodes(partition);
         
         /**
          * Place the special handled SERVICE nodes.
          */
         for (IGroupMemberNode node : classifier.get(ServiceNode.class)) {
            partition.placeAtFirstPossiblePosition(
               node,knownBoundFromPrevPartitions);
         }

         /**
          * Place the VALUES nodes.
          */
         for (IGroupMemberNode node : classifier.get(BindingsClause.class)) {
            partition.placeAtFirstContributingPosition(
               node,knownBoundFromPrevPartitions);
         }

         /**
          * Place the BIND nodes: it is important that we bring them into the
          * right order, e.g. if bind node 1 uses variables bound by bind node 2,
          * then we must insert node 2 first in order to be able to place node 1
          * after the first bind node.
          */
         final Set<IVariable<?>> knownBoundSomewhere =
            new HashSet<IVariable<?>>(partition.definitelyProduced);

         
         // ... order the bind nodes according to dependencies
         final List<AssignmentNode> bindNodesOrdered = 
            orderBindNodesByDependencies(
                  classifier.get(AssignmentNode.class), 
                  partition.bindingInfoMap, knownBoundSomewhere);
         
         // ... and place the bind nodes
         for (AssignmentNode node : bindNodesOrdered) {
            partition.placeAtFirstContributingPosition(
               node,knownBoundFromPrevPartitions);
         }       
         
         knownBoundFromPrevPartitions.addAll(partition.getDefinitelyProduced());
      }
   }


   /**
    * Brings the BIND nodes in a correct order according to the dependencies
    * that they have. Note that this is a best effort approach, which may
    * fail in cases where we allow for liberate patterns that do not strictly
    * follow the restriction of SPARQL semantics (e.g., for cyclic patterns
    * such as BIND(?x AS ?y) . BIND(?y AS ?x)). We may want to check the query
    * for such patterns statically and throw a pre-runtime exception, as the
    * SPARQL 1.1 standard suggests.
    * 
    * Failing means we return an order that is not guaranteed to be "safe", 
    * which might give us unexpected results in some exceptional cases.
    * However, whenever the pattern is valid according to the SPARQL 1.1
    * semantics, this method should return nodes in a valid order.
    * 
    * @param bindNodes the list of BIND nodes to reorder
    * @param bindingInfoMap hash map for binding info lookup
    * @param knownBoundSomewhere variables that are known to be bound somewhere
    *        in the node list in which  we want to place the BIND nodes
    *        
    * @return the ordered node set if exists, otherwise a "best effort" order
    */
   List<AssignmentNode> orderBindNodesByDependencies(
      final List<IGroupMemberNode> bindNodes,
      final GroupNodeVarBindingInfoMap bindingInfoMap,
      final Set<IVariable<?>> knownBoundSomewhere) {
      
      final List<AssignmentNode> ordered = 
         new ArrayList<AssignmentNode>(bindNodes.size());

      final LinkedList<AssignmentNode> toBePlaced = 
         new LinkedList<AssignmentNode>();
      for (int i=0; i<bindNodes.size(); i++) {
         final IGroupMemberNode assNodeCandidate = bindNodes.get(i);
         if (assNodeCandidate instanceof AssignmentNode) {
            toBePlaced.add((AssignmentNode)assNodeCandidate);
         }
      }

      final Set<IVariable<?>> knownBound =
         new HashSet<IVariable<?>>(knownBoundSomewhere);
      while (!toBePlaced.isEmpty()) {
         
         for (int i=0 ; i<toBePlaced.size(); i++) {
            
            final AssignmentNode node = toBePlaced.get(i);
            final GroupNodeVarBindingInfo nodeBindingInfo = bindingInfoMap.get(node);
            
            /**
             * The first condition is that the node can be safely placed. The
             * second condition is a fallback, where we randomly pick the last
             * node (even if it does not satisfy the condition).
             */
            if (nodeBindingInfo.leftToBeBound(knownBound).isEmpty() ||
                i+1==toBePlaced.size()) {

               // add the node to the ordered set
               ordered.add(node);
               
               // remove it from the toBePlaced array
               toBePlaced.remove(i);
               
               // add its bound variables to the known bound set
               knownBound.addAll(nodeBindingInfo.getDefinitelyProduced());
               
               break;
            }
            
         }
         
      }
      
      return ordered;
      
   }





   /**
    * Classification of nodes. This class maintains
    * 
    * - Lists of nodes classified by types that are treated differently
    *   according to the standard, in particular (in original order)
    *   - One list for filter nodes.
    *   - One list of BIND and VALUES node.
    *   - One list containing other nodes with binding requirements.
    *   - One list containing the remaining nodes.
    *   
    * Note that, according to the SPARQL 1.1 semantics, use of BIND ends the
    * preceding graph pattern. Here, we place BIND nodes at the first position
    * where it can be evaluated, i.e. treat them the same way as we do with
    * FILTERs (giving priority to FILTERs). This might not always be 100%
    * inline with the standard, but is probably the most efficient way to
    * deal with them.
    *   
    * - A map, mapping from node type to a set of nodes with the respective type
    */
   protected static class NodeClassification {

      /**
       * Filter nodes.
       */
      List<FilterNode> filters;
      
      /**
       * SPARQL 1.1 bindings clause such as VALUES (?x ?y) { (1 2) (3 4) }
       */
      List<BindingsClause> valuesNodes;

      /**
       * SPARQL 1.1 BIND nodes.
       */
      List<AssignmentNode> bindNodes;
      
      /**
       * Can be freely reordered in the join group as long as 
       */
      List<ServiceNode> serviceNodesWithSpecialHandling;
      
      /**
       * All nodes that have binding requirements, i.e. must not be evaluated
       * before certain variables are bound. 
       */
      List<IGroupMemberNode> noneSpecialNodes;
      
            
      /**
       * Constructor. The first parameter is the list of nodes, the second
       * one the {@link GroupNodeVarBindingInfoMap} for all the methods
       * (which can be obtained by calling the constructor for the 
       * {@link GroupNodeVarBindingInfo} object).
       */
      public NodeClassification(
         final Iterable<IGroupMemberNode> nodes, 
         final GroupNodeVarBindingInfoMap bindingInfoMap) {

         filters = new ArrayList<FilterNode>();
         valuesNodes = new ArrayList<BindingsClause>();
         bindNodes = new ArrayList<AssignmentNode>();
         serviceNodesWithSpecialHandling = new ArrayList<ServiceNode>();
         noneSpecialNodes = new ArrayList<IGroupMemberNode>();
         
         for (IGroupMemberNode node : nodes) {
            registerNode(node, bindingInfoMap.get(node));
         }
      }

      
      /**
       * Registers an additional node to the {@link NodeClassification} object.
       * Requires the {@link GroupNodeVarBindingInfo} associated to the node.
       * 
       * @param node
       * @param bindingInfo
       */
      void registerNode(
         final IGroupMemberNode node, 
         final GroupNodeVarBindingInfo bindingInfo) {
                  
         if (bindingInfo==null) {
            throw new RuntimeException("No GroupNodeVarBindingInfo provided.");
         }
         
         final ServiceFactory defaultSF = 
            ServiceRegistry.getInstance().getDefaultServiceFactory();
         
         if (node instanceof AssignmentNode) {

            bindNodes.add((AssignmentNode)node);
            
         } else if (node instanceof BindingsClause) {
            
            valuesNodes.add((BindingsClause)node);

         } else if (node instanceof FilterNode) {
            
            filters.add((FilterNode)node);

         } else if (node instanceof StatementPatternNode) {
            
            noneSpecialNodes.add(node);
            
         } else if (node instanceof NamedSubqueryInclude) {
            
            noneSpecialNodes.add(node);
            
         } else if (node instanceof ArbitraryLengthPathNode ||
            node instanceof ZeroLengthPathNode || 
            node instanceof PropertyPathUnionNode) {
            
            noneSpecialNodes.add(node);
            
         } else if (node instanceof SubqueryRoot) {
            
            noneSpecialNodes.add(node);
               
         } else if (node instanceof ServiceNode) {

            final ServiceNode serviceNode = (ServiceNode)node;
            
            // add service node to the list it belongs to
            if (!bindingInfo.leftToBeBound(new HashSet<IVariable<?>>()).isEmpty() ||
                  !serviceNode.getResponsibleServiceFactory().equals(defaultSF)) {
               
               serviceNodesWithSpecialHandling.add((ServiceNode)node);
               
            } else {
               
               // only SPARQL 1.1 SERVICE nodes with constant go here (they
               // can be treated strictly according to the semantics); note
               // that the SPARQL 1.1 standard doesn't give a concise semantics
               // for SPARQL 1.1 queries with variable endpoint
               noneSpecialNodes.add(node);
               
            }
            
         // NOTE: don't move around, this is in the end in purpose to allow
         // the treatment of more specialized cases
         } else if (node instanceof GraphPatternGroup<?>) {
            
            noneSpecialNodes.add(node);

         } else {
            
            noneSpecialNodes.add(node);
            
         }
      }      

   }
   
   /**
    * Classification of {@link IGroupMemberNode}s along a set of specified
    * types. For nodes matching a given type, lookup is possible (returning
    * an ordered list of nodes), all other nodes are stored in a dedicated list.
    * @author msc
    *
    */
   protected static class TypeBasedNodeClassifier {
      
      Class<?>[] clazzez;
      
      List<IGroupMemberNode> unclassifiedNodes;
      
      Map<Class<?>,List<IGroupMemberNode>> classifiedNodes;
      
      /**
       * Constructor, receiving as an argument a list of types based on
       * which classification is done. 
       * 
       * @param types
       */
      public TypeBasedNodeClassifier(
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

}

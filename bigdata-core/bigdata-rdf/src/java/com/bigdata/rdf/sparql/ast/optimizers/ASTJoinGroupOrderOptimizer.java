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
 * Created on June 12, 2015
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.BindingsClause;
import com.bigdata.rdf.sparql.ast.GroupNodeVarBindingInfo;
import com.bigdata.rdf.sparql.ast.GroupNodeVarBindingInfoMap;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.explainhints.ExplainHint;
import com.bigdata.rdf.sparql.ast.explainhints.JoinOrderExplainHint;
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
public class ASTJoinGroupOrderOptimizer extends AbstractJoinGroupOptimizer 
implements IASTOptimizer {
   
   private final boolean assertCorrectnessOnly;

   /**
    * Default constructor, running the optimizer with optimizations turned on.
    */
   public ASTJoinGroupOrderOptimizer() {
      this(false);
   }
   
   /**
    * Constructor allowing to run the optimizer in an "assert-correctness-only"
    * mode that makes only minor modifications to the join order.
    */
   public ASTJoinGroupOrderOptimizer(final boolean assertCorrectnessOnly) {
      this.assertCorrectnessOnly = assertCorrectnessOnly;
   }
   
   @Override
   protected void optimizeJoinGroup(AST2BOpContext ctx, StaticAnalysis sa,
         IBindingSet[] bSets, JoinGroupNode joinGroup) {

      final boolean reorderNodes = 
         ASTStaticJoinOptimizer.isStaticOptimizer(ctx, joinGroup);

      /**
       * Initialize summary containers with a single pass over the children;
       * they allow for efficient lookup of required information in the
       * following.
       */
            
      // variables incoming externally (i.e. those variables definitely bound
      // when evaluating this join group)
      final Set<IVariable<?>> externallyIncoming = 
         sa.getDefinitelyIncomingBindings(
            joinGroup, new HashSet<IVariable<?>>());

      // easy-access information to FILTER [NOT] EXISTS constructs in this group
      final ASTJoinGroupFilterExistsInfo fExInfo = 
            new ASTJoinGroupFilterExistsInfo(joinGroup);

      // easy-access information about variable bindings for nodes in this group
      final GroupNodeVarBindingInfoMap bindingInfoMap =
         new GroupNodeVarBindingInfoMap(joinGroup, sa, fExInfo);

      /**
       * Setup helper class for proper placement of FILTER nodes in join group.
       */
      final ASTFilterPlacer filterPlacer = new ASTFilterPlacer(joinGroup, fExInfo);
      
      /**
       * Set up the partitions, ignoring the FILTER nodes. FILTER nodes apply
       * to the whole join group by semantics, so they are not considered here
       * but will be added in the end. 
       */
      final ASTJoinGroupPartitions partitions = 
         new ASTJoinGroupPartitions(
            filterPlacer.getNonFilterNodes(), 
            bindingInfoMap, externallyIncoming);
      
      /**
       * First, optimize across the partitions, trying to move forward
       * non-optional non-minus patterns wherever possible. It is important to
       * do this first, before reordering within partitions, as it shifts
       * nodes around across them. See 
       * https://docs.google.com/document/d/1Fu0QLj1ML6CdaysREDFsJt8fT048zBWp0ssMDITAYt8
       * for a formal explanation and justification of our approach.
       */
      if (reorderNodes && !assertCorrectnessOnly) {
         optimizeAcrossPartitions(joinGroup, partitions, bindingInfoMap, externallyIncoming);
      }
      
      /** 
       * Second, optimize within the individual partitions. This optimization
       * is based on both hard constraints (w.r.t. the placement of nodes
       * that require bindings) and heuristics (e.g. an order based on node
       * types).
       * 
       * Note: we may want to pass in (and use) information about existing
       * filters, which might be valuable information for the optimizer.
       */
      if (reorderNodes) {
         optimizeWithinPartitions(partitions, bindingInfoMap, assertCorrectnessOnly);
      }
      
      /**
       * Third, place the FILTERs at appropriate positions (across partitions).
       * Note that this is required for correctness, and hence done even if
       * reorderNodes is set to false (i.e., reordering is disabled through
       * the query hint).
       */
      filterPlacer.placeFiltersInPartitions(partitions);

      /**
       * Now, flatten the partitions again and replace the children of the
       * join group with the new list.
       */
      final LinkedList<IGroupMemberNode> nodeList = 
         partitions.extractNodeList(true /* includeOptionalOrMinusNode */);
      for (int i = 0; i < joinGroup.arity(); i++) {
          joinGroup.setArg(i, (BOp) nodeList.get(i));
      }
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
     final JoinGroupNode joinGroup,
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

        // we start out with the second partition, so this will succeed
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
              
              final ASTJoinGroupPartition candidatePartition = partitionList.get(j);
              
              /**
               * Calculate the conflicting vars as the intersection of the
               * maybe vars of the bordering OPTIONAL or MINUS with the maybe
               * vars of the node to move around, minus the nodes that are
               * known to be bound upfront.
               */
              final Set<IVariable<?>> conflictingVars;
              if (candidatePartition.optionalOrMinus == null) {
                 conflictingVars = new HashSet<IVariable<?>>();
              } else {
                 conflictingVars = 
                    new HashSet<IVariable<?>>(
                       bindingInfoMap.get(
                          candidatePartition.optionalOrMinus).getMaybeProduced());
              }

              final GroupNodeVarBindingInfo candidateBindingInfo = 
                 bindingInfoMap.get(candidate);
              conflictingVars.retainAll(candidateBindingInfo.getMaybeProduced());
              
              conflictingVars.removeAll(
                 definitelyProducedUpToPartition.get(j+1));
              
              if (conflictingVars.isEmpty() && 
                    definitelyProducedUpToPartition.get(j).containsAll(
                       candidateBindingInfo.getRequiredBound())) {
                 
                 // record candidate and continue, maybe we can do even better...
                 partitionForCandidate = j;
                 
              } else {
                 
                 // stop here, definitely can't place in prior part as well
                 
                 // if we reach this code, there might actually be something 
                 // wrong with the query: usually non-optional non-minus nodes
                 // can be executed before OPTIONAL or MINUS blocks -> we 
                 // therefore append an EXPLAIN hint to the join group                 
                 final ExplainHint explainHint = 
                    new JoinOrderExplainHint(
                       JoinOrderExplainHint.
                          ACROSS_PARTITION_REORDERING_PROBLEM, candidate);
                 joinGroup.addExplainHint(explainHint);
                 
                 break; 
              }
              
           }
           
           if (partitionForCandidate!=null) { // if can be moved:

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
   * Optimize the order of nodes within the single partitions. The nice thing
   * about partitions is that we can freely reorder the non-optional
   * non-minus nodes within them. 
   */
  void optimizeWithinPartitions(
     final ASTJoinGroupPartitions partitions,
     final GroupNodeVarBindingInfoMap bindingInfoMap,
     final boolean assertCorrectnessOnly) {
     
     final List<ASTJoinGroupPartition> partitionList = 
           partitions.getPartitionList();
        
     final Set<IVariable<?>> knownBoundFromPrevPartitions =
        new HashSet<IVariable<?>>();
        
     for (ASTJoinGroupPartition partition : partitionList) {
        optimizeWithinPartition(
           partition, assertCorrectnessOnly, 
           bindingInfoMap, knownBoundFromPrevPartitions);
     }
  }
  
   /**
    * Optimize the order of nodes within the given partition. The nice thing
    * about partitions is that we can freely reorder the non-optional
    * non-minus nodes within them. 
    */
   void optimizeWithinPartition(
      final ASTJoinGroupPartition partition,
      final boolean assertCorrectnessOnly,
      final GroupNodeVarBindingInfoMap bindingInfoMap,
      final Set<IVariable<?>> knownBoundFromPrevPartitions) {
          
      final ASTTypeBasedNodeClassifier classifier = 
         new ASTTypeBasedNodeClassifier(
            new Class<?>[] { 
               ServiceNode.class, /* see condition A */
               AssignmentNode.class,
               BindingsClause.class,
               IGroupMemberNode.class /* see condition B */ });
     
      /**
       * ### Condition A:
       * 
       * We only consider special service nodes for placement, all other service
       * nodes are treated through a standard reorder.
       */
      classifier.addConstraintForType(
         ServiceNode.class,
          new ASTTypeBasedNodeClassifierConstraint() {
            
            @Override
            boolean appliesTo(final IGroupMemberNode node) {

               if (node instanceof ServiceNode) {

                  /**
                   * Return true if the service is not a SPARQL 1.1 SERVICE.
                   */
                  final ServiceNode sn = (ServiceNode) node;
                  if (!sn.getResponsibleServiceFactory().equals(
                        ServiceRegistry.getInstance()
                              .getDefaultServiceFactory())) {
                     return true;
                  }

                  /**
                    * Return true if it is a SPARQL 1.1 SERVICE, but the
                    * constant is not bound.
                    */
                  if (!sn.getServiceRef().isConstant()) {
                     return true;
                  }
               }

                  // as a fallback return false
               return false;

            }

         });
      
      /**
       * ### Condition B:
       * 
       * Additional nodes that have binding requirements (e.g.
       * { BIND ?x AS ?y } UNION { ... } that can be satisified through
       * this partition.
       */
      classifier.addConstraintForType(
          IGroupMemberNode.class,
          new ASTTypeBasedNodeClassifierConstraint() {
         
             @Override
             boolean appliesTo(final IGroupMemberNode node) {
                
                // get the variables that are required bound in the node ...
                final Set<IVariable<?>> reqBoundInNode = 
                   new HashSet<IVariable<?>>(
                      bindingInfoMap.get(node).getRequiredBound());
                // ... and substract those that maybe prodcued inside
                reqBoundInNode.removeAll(
                    bindingInfoMap.get(node).getMaybeProduced());
                
                // -> if this set has not become empty, we need to aim at
                //    binding variables of this node from this outside partition
                return !reqBoundInNode.isEmpty();

             }

         });      

      classifier.registerNodes(
         partition.extractNodeList(false /* includeOptionalOrMinus */));

      /**
       * In a first step, we remove service nodes, assignment nodes, and
       * bindings clauses from the partition. They will be handled in a special
       * way.
       */
      final List<IGroupMemberNode> toRemove = new ArrayList<IGroupMemberNode>();
      toRemove.addAll(classifier.get(ServiceNode.class));
      toRemove.addAll(classifier.get(AssignmentNode.class));
      toRemove.addAll(classifier.get(BindingsClause.class));
      toRemove.addAll(classifier.get(IGroupMemberNode.class));
      partition.removeNodesFromPartition(toRemove);

      /**
       * The remaining elements will be reordered based on their type. This is
       * only done if optimization was turned on though. Otherwise, this method
       * just asserts correct placement of nodes with binding requirements. This
       * is where the optimization takes place.
       * 
       * Note: this is the place where we want to integrate the StaticOptimizer.
       */
      if (!assertCorrectnessOnly) {
         final IASTJoinGroupPartitionReorderer reorderer = 
            new TypeBasedASTJoinGroupPartitionReorderer();
         reorderer.reorderNodes(partition);
      }

      // the non-VALUE nodes that will be placed (some lines below)
      final List<IGroupMemberNode> nonValueNodesToBePlaced =
            new LinkedList<IGroupMemberNode>();
         nonValueNodesToBePlaced.addAll(classifier.get(AssignmentNode.class));
         nonValueNodesToBePlaced.addAll(classifier.get(ServiceNode.class));
         nonValueNodesToBePlaced.addAll(classifier.get(IGroupMemberNode.class));
      
      /**
       * Place the VALUES nodes. Generally, it is desirable to place the VALUES
       * clause at the first contributing position. However, in some cases
       * (see e.g. https://jira.blazegraph.com/browse/BLZG-1463) the VALUES
       * clause may introduce variables for the remainingToBePlaced constructs
       * (e.g., a subsequent BIND node, as in the ticket example) s.t. it is
       * desirable to place the VALUES clause early on: this clears the way
       * for placing subsequent nodes earlier in the query execution plan.
       * 
       * We therefore check the variables in the remainingToBePlaced array
       * for dependencies towards variables introduced in our bindings clauses.
       * If there are such dependencies, we place the VALUES clauses at the
       * first possible position, otherwise we choose the first contributing
       * position.
       */
      for (IGroupMemberNode node : classifier.get(BindingsClause.class)) {
         
         final BindingsClause bc = (BindingsClause)node;
         final Set<IVariable<?>> declaredVars = bc.getDeclaredVariables();
         
         final Set<IVariable<?>> intersectionWithNonValueNodesToBePlaced = 
            new HashSet<IVariable<?>>();
         
         // start out with all non value nodes to be placed and intersect
         for (final IGroupMemberNode cur : nonValueNodesToBePlaced) {
            intersectionWithNonValueNodesToBePlaced.addAll(
               bindingInfoMap.get(cur).leftToBeBound(knownBoundFromPrevPartitions));
         }
         intersectionWithNonValueNodesToBePlaced.retainAll(declaredVars);
         
         if (intersectionWithNonValueNodesToBePlaced.isEmpty()) {
            // case 1: no dependencies -> late placement
            partition.placeAtFirstContributingPosition(node,
                  knownBoundFromPrevPartitions, false /* requires all bound */);            
         } else {
            // case 1: dependencies -> early placement
            partition.placeAtFirstPossiblePosition(node,
               knownBoundFromPrevPartitions, false /* requires all bound */);
         }
      }

      /**
       * Place the BIND nodes: it is important that we bring them into the right
       * order, e.g. if bind node 1 uses variables bound by bind node 2, then we
       * must insert node 2 first in order to be able to place node 1 after the
       * first bind node.
       */
      final Set<IVariable<?>> knownBoundSomewhere = new HashSet<IVariable<?>>(
            partition.definitelyProduced);

      // ... order the bind and SERVICE nodes according to dependencies,
      // essentially constructing a dependency graph over these nodes      
      final List<IGroupMemberNode> orderedNodes = 
         orderNodesByDependencies(
            nonValueNodesToBePlaced, partition.bindingInfoMap,
            knownBoundSomewhere);

      // ... and place the bind nodes
      for (IGroupMemberNode node : orderedNodes) {

         /**
          * We run service with runFirst query hint first (=as early as possible)
          */
         boolean runFirst = false;
         if (node instanceof ServiceNode) {
            final ServiceNode sn = (ServiceNode)node;
            runFirst = 
               sn.getResponsibleServiceFactory().
                  getServiceOptions().isRunFirst();
         }
         
         if (runFirst) {
            partition.placeAtFirstPossiblePosition(node,
                  knownBoundFromPrevPartitions, false /* requiresAllBound */);            
         } else {
            partition.placeAtFirstContributingPosition(node,
               knownBoundFromPrevPartitions, false /* requiresAllBound */);
         }
      }

      knownBoundFromPrevPartitions.addAll(partition.getDefinitelyProduced());
   }


   /**
    * Brings the nodes in a correct order according to binding req dependencies
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
   List<IGroupMemberNode> orderNodesByDependencies(
      final List<IGroupMemberNode> nodes,
      final GroupNodeVarBindingInfoMap bindingInfoMap,
      final Set<IVariable<?>> knownBoundSomewhere) {
      
      final List<IGroupMemberNode> ordered = 
         new ArrayList<IGroupMemberNode>(nodes.size());

      /**
       * Initially, all nodes must be placed
       */
      final LinkedList<IGroupMemberNode> toBePlaced = 
         new LinkedList<IGroupMemberNode>(nodes);

      final Set<IVariable<?>> knownBound =
         new HashSet<IVariable<?>>(knownBoundSomewhere);
      while (!toBePlaced.isEmpty()) {
         
         for (int i=0 ; i<toBePlaced.size(); i++) {
            
            final IGroupMemberNode node = toBePlaced.get(i);
            final GroupNodeVarBindingInfo nodeBindingInfo = 
               bindingInfoMap.get(node);
            
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
}

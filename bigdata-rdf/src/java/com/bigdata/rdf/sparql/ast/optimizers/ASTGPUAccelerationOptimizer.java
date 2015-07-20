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
 * Created on July 14, 2015
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.join.GPUJoinGroupOp;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode.Annotations;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * <p>
 * The {@link ASTGPUAccelerationOptimizer} identifies join groups that can
 * be handled by the GPU, lifts them into join groups, and annotates them
 * accordingly s.t. they can easily identified in the subsequent translation
 * process. Annotated groups are always of the form 
 * 
 * sp_1 . . ... sp_n . fn_1 ... fn_m
 * 
 * , where the spi are statement patterns without join filters and the fn_j
 * are filter expressions (previously attached to the statement patterns).
 * </p>
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class ASTGPUAccelerationOptimizer extends AbstractJoinGroupOptimizer {

   
   public ASTGPUAccelerationOptimizer() {
      super(true,false); // optimize children first!
   }
   
   @Override
   protected void optimizeJoinGroup(
         AST2BOpContext ctx, StaticAnalysis sa,
         IBindingSet[] bSets, JoinGroupNode group) {  

      if (ctx.gpuEvaluation == null)
         return;
      
      // TODO: disable for quads mode, sids mode, etc., check whether
      // GPU mode is turned on
      
      // Verify that this optimizer has not been disabled with a query hint.
      if (group.getProperty(
            QueryHints.DISABLE_GPU_ACCELERATION, 
            QueryHints.DEFAULT_DISABLE_GPU_ACCELERATION))
         return;
      
      // the subgroups
      final List<GPUAcceleratableGroup> gpuAcceleratableGroups = 
         new LinkedList<GPUAcceleratableGroup>();
      
      final List<IGroupMemberNode> children = group.getChildren();
      final List<StatementPatternNode> toRemove = 
         new LinkedList<StatementPatternNode>();
      for (int i=0; i<children.size(); ) {
         
         final IGroupMemberNode childStart = children.get(i);
         if (childStart instanceof StatementPatternNode) {

            toRemove.add((StatementPatternNode)childStart); // lifted
            
            // scan to the first non-statement pattern child
            int j = i;
            while (++j<children.size() && 
                     children.get(j) instanceof StatementPatternNode) {

               toRemove.add((StatementPatternNode)children.get(j)); // lifted
               
            }
            
            // add the range of statement patterns to the group
            List<StatementPatternNode> inGroup = 
               new LinkedList<StatementPatternNode>();
            for (int k=i; k<j; k++) {
               inGroup.add((StatementPatternNode)children.get(k));
            }
            gpuAcceleratableGroups.add(
               new GPUAcceleratableGroup(inGroup, i));
            
            i=j; // continue at index j

         } else {
            
            i++; // not a candidate, advance
            
         }
      }

      // having collected the GPU acceleratable groups, let's now transform
      // lift these groups out; we do this in inverse order to avoid conflicts
      for (int i=gpuAcceleratableGroups.size()-1; i>=0; i--) {
         final GPUAcceleratableGroup ag = gpuAcceleratableGroups.get(i);
         group.addArg(ag.getIndexToAdd(), ag.asLiftedGroup());
      }
      
      // finally remove the statement pattern nodes that were lifted
      for (StatementPatternNode spn : toRemove) {
         group.removeChild(spn);
      }
   }
   
   /**
    * Metainformation for a group of nodes that can be accelerated using
    * Mapgraph in the GPU.
    */
   public static class GPUAcceleratableGroup {
      
      private final List<StatementPatternNode> stmtPatterns;
      int indexToAdd;
      
      public GPUAcceleratableGroup(
         final List<StatementPatternNode> stmtPatterns,
         final int indexToAdd) {
         
         this.stmtPatterns = stmtPatterns;
         this.indexToAdd = indexToAdd;
         
      }

      
      
      /**
       * Serialize the GPU acceleratable group into a join group node with
       * preceding statement patterns, followed by filters. The statement
       * patterns are free of filters.
       */
      public JoinGroupNode asLiftedGroup() {
         
         final JoinGroupNode jgn = new JoinGroupNode();
         jgn.setProperty(GPUJoinGroupOp.Annotations.EVALUATE_ON_GPU, true);
         
         final List<FilterNode> filterNodes = new ArrayList<FilterNode>();
         for (StatementPatternNode stmtPattern : stmtPatterns) {
            
            // take a copy, to leave the original pattern unmodified
            final StatementPatternNode stmtPatternCpy = 
               BOpUtility.deepCopy(stmtPattern);
            
            // move filters from statement pattern to list
            filterNodes.addAll(stmtPatternCpy.getAttachedJoinFilters());
            stmtPatternCpy.setProperty(Annotations.FILTERS, null);
            
            // record statement pattern
            jgn.addChild(stmtPatternCpy);
         }
         
        
         // finally, add the filter nodes that were lifted out
         for (FilterNode filterNode : filterNodes) {
            jgn.addChild(filterNode);
         }
         
         return jgn;
               
      }

      public List<StatementPatternNode> getStmtPatterns() {
         return stmtPatterns;
      }
      
      public int getIndexToAdd() {
         return indexToAdd;
      }

      
   }
}

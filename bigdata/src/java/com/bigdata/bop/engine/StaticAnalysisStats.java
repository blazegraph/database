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
 * Created on Aug 26, 2010
 */

package com.bigdata.bop.engine;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import com.bigdata.rdf.sparql.ast.optimizers.ASTOptimizerList;

/**
 * Statistics associated with the Static Analysis phase, such as runtime for
 * the parser, given optimizers, etc.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class StaticAnalysisStats implements Serializable {


   private static final long serialVersionUID = 3092439315838261120L;

   /**
    * Parser statistics.
    */
   private StaticAnalysisStat parserStat;
   
   /**
    * Parser statistics.
    */
   private StaticAnalysisStat optimizerLoopStat;
   
   /**
    * Range count statistics.
    */
   private StaticAnalysisStat rangeCountStat;

   /**
    * Map for the statistics of the individual (non-loop).
    */
   private final Map<String,StaticAnalysisStat> optimizerStats;

   /**
    * Create a new, initially empty stats object.
    * 
    * @param staticAnalysisParseTimeElapsed time needed for parsing (ignored
    *          if left null)
    */
   public StaticAnalysisStats(Long staticAnalysisParseTimeElapsed) {
      this.optimizerStats = new LinkedHashMap<String, StaticAnalysisStat>();
     
      if (staticAnalysisParseTimeElapsed!=null) {
         registerParserCall(staticAnalysisParseTimeElapsed);
      } 
      // otherwise: delayed initialization, don't show parser stats if
      //            for some reason not present in some mode
      
      rangeCountStat = new StaticAnalysisStat("RangeCount"); 
      optimizerLoopStat = new StaticAnalysisStat(ASTOptimizerList.class.getName());
      
   }

   /**
    * Registers a call of the parser.
    * 
    * @param elapsed the elapsed time for the parsing phase.
    */
   public void registerParserCall(final Long elapsed) {
      
      if (parserStat==null) {
         parserStat = new StaticAnalysisStat("Parse Time");
      }

      parserStat.incrementNrCalls();
      parserStat.addElapsed(elapsed);
   }
   
   /**
    * Registers a call for the key optimizer loop in {@link ASTOptimizerList}.
    * 
    * @param optimizerName the name of the optimizer
    * @param elapsed the elapsed time for the optimizer loop
    */
   public void registerOptimizerLoopCall(final Long elapsed) {

      optimizerLoopStat.incrementNrCalls();
      optimizerLoopStat.addElapsed(elapsed);
   }
   
   public void registerRangeCountCall(final Long elapsed) {
      
      rangeCountStat.incrementNrCalls();
      rangeCountStat.addElapsed(elapsed);
   }
   
   /**
    * Registers a call to a specific AST optimizer (different the main loop).
    * 
    * @param optimizerName the name of the optimizer
    * @param elapsed the elapsed time for the optimizer loop
    */
   public void registerOptimizerCall(
      final String optimizerName, final Long elapsed) {
      
      if (optimizerStats.get(optimizerName)==null) {
         optimizerStats.put(optimizerName, new StaticAnalysisStat(optimizerName));
      }
      
      StaticAnalysisStat stat = optimizerStats.get(optimizerName);
      stat.incrementNrCalls();
      stat.addElapsed(elapsed);
   }

   @Override
   public String toString() {
       final StringBuilder sb = new StringBuilder();
       sb.append("#StaticAnalysisStats: ");
       if (parserStat!=null) {
          sb.append(parserStat);
          sb.append(" ");
       }
       if (optimizerLoopStat!=null) {
          sb.append(optimizerLoopStat);
          sb.append(" ");
       }
       for (StaticAnalysisStat optimizerStat : optimizerStats.values()) {
          sb.append(optimizerStat);          
          sb.append(" ");
       }
       
       return sb.toString();
   }

   /**
    * @return stat object associated with the parser
    */
   public StaticAnalysisStat getParserStat() {
      return parserStat;
   }
   
   /**
    * @return stats object associated with the optimizer loop class
    */
   public StaticAnalysisStat getOptimizerLoopStat() {
      return optimizerLoopStat;
   }
   
   /**
    * @return stat object associated with range count operations
    */
   public StaticAnalysisStat getRangeCountStat() {
      return rangeCountStat;
   }
   
   /**
    * @return stats object associated with the optimizer loop class
    */
   public Collection<StaticAnalysisStat> getOptimizerStats() {
      return optimizerStats.values();
   }

}

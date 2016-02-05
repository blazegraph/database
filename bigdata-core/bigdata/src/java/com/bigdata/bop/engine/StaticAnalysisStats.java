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
 * Created on Aug 26, 2010
 */

package com.bigdata.bop.engine;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.openrdf.model.Value;

import com.bigdata.rdf.sparql.ast.ASTContainer;
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
    * Batch resolve values to IVs stats.
    */
   private final StaticAnalysisStat resolveValuesStat;
   
   /**
    * Parser statistics.
    */
   private final StaticAnalysisStat parserStat;
   
   /**
    * Optimizer statistics.
    */
   private final StaticAnalysisStat optimizerLoopStat;
   
   /**
    * Range count statistics.
    */
   private final StaticAnalysisStat rangeCountStat;

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
   public StaticAnalysisStats() {
      
      this.optimizerStats = new LinkedHashMap<String, StaticAnalysisStat>();
     
      parserStat = new StaticAnalysisStat("ParseTime"); 
      resolveValuesStat = new StaticAnalysisStat("ResolveValues"); 
      rangeCountStat = new StaticAnalysisStat("RangeCount"); 
      optimizerLoopStat = new StaticAnalysisStat("Optimizers");
      
   }


   /**
    * Registers a call of the parser.
    * 
    * @param elapsed the elapsed time for the parsing phase.
    */
   public void registerParserCall(final ASTContainer astContainer) {
      
		{
			final Long elapsedNanoSec = astContainer.getQueryParseTime();
			if (elapsedNanoSec != null) {
				parserStat.incrementNrCalls();
				parserStat.addElapsed(elapsedNanoSec);
			}
		}

		{
			final Long elapsedNanoSec = astContainer.getResolveValuesTime();
			if (elapsedNanoSec != null) {
				resolveValuesStat.incrementNrCalls();
				resolveValuesStat.addElapsed(elapsedNanoSec);
			}
		}

   }
   
   /**
    * Registers a call for the key optimizer loop in {@link ASTOptimizerList}.
    * 
    * @param optimizerName the name of the optimizer
    * @param elapsed the elapsed time for the optimizer loop
    */
   public void registerOptimizerLoopCall(final long elapsedNanoSec) {

      optimizerLoopStat.incrementNrCalls();
      optimizerLoopStat.addElapsed(elapsedNanoSec);
   }
   
   public void registerRangeCountCall(final long elapsedNanoSec) {
      
      rangeCountStat.incrementNrCalls();
      rangeCountStat.addElapsed(elapsedNanoSec);
   }
   
   /**
    * Registers a call to a specific AST optimizer (different the main loop).
    * 
    * @param optimizerName the name of the optimizer
    * @param elapsed the elapsed time for the optimizer loop
    */
   public void registerOptimizerCall(
      final String optimizerName, final long elapsedNanoSec) {
      
      if (optimizerStats.get(optimizerName)==null) {
         optimizerStats.put(optimizerName, new StaticAnalysisStat(optimizerName));
      }
      
      StaticAnalysisStat stat = optimizerStats.get(optimizerName);
      stat.incrementNrCalls();
      stat.addElapsed(elapsedNanoSec);
   }

   @Override
   public String toString() {
       final StringBuilder sb = new StringBuilder();
       sb.append("#StaticAnalysisStats: ");
       if (parserStat!=null) {
           sb.append(parserStat);
           sb.append(" ");
        }
       if (resolveValuesStat!=null) {
           sb.append(resolveValuesStat);
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
    * @return stat object associated with batch resolution of RDF {@link Value}s to IVs.
    */
   public StaticAnalysisStat getResolveValuesStat() {
      return resolveValuesStat;
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

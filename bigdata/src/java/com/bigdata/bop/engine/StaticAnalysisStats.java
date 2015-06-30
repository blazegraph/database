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
import java.util.LinkedHashMap;
import java.util.Map;

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
    * Parser statistics
    */
   private StaticAnalysisStat parserStat;

   /**
    * Map for the statistics of the individual optimizers.
    */
   private final Map<String,StaticAnalysisStat> optimizerStats;

   /**
    * Create a new, initially empty stats object.
    * 
    * @param name a descriptive, human understandable name for the stats object
    */
   public StaticAnalysisStats(Long parseTimeElapsed) {
      this.optimizerStats = new LinkedHashMap<String, StaticAnalysisStat>();

      if (parseTimeElapsed!=null) {
         parserStat = new StaticAnalysisStat("ParseTime");
         parserStat.incrementNrCalls();
         parserStat.addElapsed(parseTimeElapsed);
      }
      
   }


   /**
    * @return stats object associated with the parser
    */
   public StaticAnalysisStat getParserStat() {
      return parserStat;
   }


   /**
    * Sets the stats object associated with the parser.
    * 
    * @param parserStat
    */
   public void setParserStat(StaticAnalysisStat parserStat) {
      this.parserStat = parserStat;
   }
   
   /**
    * Initializes an empty stats object for the optimizer with the given name.
    * Increases the number of calls made to this optimizer.
    * 
    * @param optimizerName
    */
   public void registerOptimizerCall(String optimizerName) {
      
      if (optimizerStats.get(optimizerName)==null) {
         optimizerStats.put(optimizerName, new StaticAnalysisStat(optimizerName));
      }
      
      StaticAnalysisStat stat = optimizerStats.get(optimizerName);
      stat.incrementNrCalls(); // this has just been called
   }

   /**
    * Increments the elapsed time associated with the optimizerStats object.
    * Object must exist.
    * 
    * @param optimizerName
    * @param elapsed
    */
   public void addElapsedToOptimizerStat(String optimizerName, long elapsed) {
      optimizerStats.get(optimizerName).addElapsed(elapsed);      
   }

   @Override
   public String toString() {
       final StringBuilder sb = new StringBuilder();
       sb.append("#StaticAnalysisStats: ");
       if (parserStat!=null) {
          sb.append(parserStat);
          sb.append(" ");
       }
       for (StaticAnalysisStat optimizerStat : optimizerStats.values()) {
          sb.append(optimizerStat);          
          sb.append(" ");
       }
       
       return sb.toString();
   }
}

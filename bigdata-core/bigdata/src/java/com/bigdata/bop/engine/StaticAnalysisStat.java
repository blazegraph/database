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

import com.bigdata.counters.CAT;

/**
 * Statistics associated with the Static Analysis phase, such as runtime for
 * the parser, given optimizers, etc.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class StaticAnalysisStat implements Serializable {

   private static final long serialVersionUID = 7973851199166467621L;
   
   /**
    * Human understandable name describing what these stats are for.
    */
   private final String statName;
   
   /**
    * The number of calls to this optimizer.
    */
   private final CAT nrCalls = new CAT();
    
    /**
     * The elapsed time (milliseconds) for the statistics object.
     */
    private final CAT elapsed = new CAT();
    
   /**
    * Create a new, initially empty statistics object.
    * 
    * @param statName
    *           a descriptive, human understandable name for the stats object
    */
   public StaticAnalysisStat(String statName) {
      this.statName = statName;
   }


   public void addElapsed(long elapsed) {
      this.elapsed.add(elapsed);
   }


   
   public void incrementNrCalls() {
      this.nrCalls.increment();
   }
   
   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("Stats for " + statName + ":");
      sb.append("{elapsed=" + elapsed.get());
      sb.append(", nrCalls=" + nrCalls);
      sb.append("}");
      return sb.toString();
   }
    

   public long getNrCalls() {
      return nrCalls.get();
   }


   public long getElapsed() {
      return elapsed.get();
   }
   
   public String getStatName() {
      return statName;
   }

}

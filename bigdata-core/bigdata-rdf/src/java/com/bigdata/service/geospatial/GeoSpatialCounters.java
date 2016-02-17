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
 * Created on Sep 18, 2015
 */

package com.bigdata.service.geospatial;

import com.bigdata.counters.CAT;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSetAccess;
import com.bigdata.counters.Instrument;

/**
 * Counters related to the usage of GeoSpatial services
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class GeoSpatialCounters implements ICounterSetAccess {

    /**
     * The #of GeoSpatial service searches executed. Note that a single
     * query may trigger multiple search requests.
     */
    protected final CAT geoSpatialSearchRequests = new CAT();

    /**
     * The #of values scanned
     */
    protected final CAT zOrderIndexScannedValues = new CAT();

    /**
     * The #of values that matched the search range
     * and were returned by the zOrderIndex advancer.
     */
    protected final CAT zOrderIndexHits = new CAT();

    /**
     * The #of misses when scanning the search range.
     */
    protected final CAT zOrderIndexMisses = new CAT();

    /**
     * The time spent in bigmin calculations (not including
     * the subsequent advancement of the cursor)
     */
    protected final CAT bigMinCalculationTime = new CAT();

    /**
     * The time spent in verifying whether detected values are
     * actually in the geospatial range.
     */
    protected final CAT rangeCheckCalculationTime = new CAT();

    /**
     * The time spent in evaluating the filter on top (such as
     * filtering for whether a point lies in a given circle).
     */
    protected final CAT filterCalculationTime = new CAT();

    /**
     * The #of GeoSpatialServiceCallTask tasks scheduled
     */
    protected final CAT geoSpatialServiceCallTasks = new CAT();

    /**
     * The #of GeoSpatialServiceCallSubRangeTask 
     */
    protected final CAT geoSpatialServiceCallSubRangeTasks = new CAT();


    public void registerGeoSpatialSearchRequest() {
       geoSpatialSearchRequests.increment();
    }

    public void registerZOrderIndexHit() {
       zOrderIndexScannedValues.increment();
       zOrderIndexHits.increment();
    }

    public void registerZOrderIndexMiss() {
       zOrderIndexScannedValues.increment();
       zOrderIndexMisses.increment();
    }

    public void addBigMinCalculationTime(long timeInNanoSec) {
       bigMinCalculationTime.add(timeInNanoSec);
    }
    
    public void addRangeCheckCalculationTime(long timeInNanoSec) {
       rangeCheckCalculationTime.add(timeInNanoSec);
    }
    
    public void addFilterCalculationTime(long timeInNanoSec) {
       filterCalculationTime.add(timeInNanoSec);
    }
    
    public void registerGeoSpatialServiceCallTask() {
       geoSpatialServiceCallTasks.increment();
    }

    public void registerGeoSpatialServiceCallSubRangeTasks(long numSubtasks) {
       geoSpatialServiceCallSubRangeTasks.add(numSubtasks);
    }
    
    @Override
    public CounterSet getCounters() {

        final CounterSet root = new CounterSet();

        root.addCounter("geoSpatialSearchRequests", new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(geoSpatialSearchRequests.get());
            }
        });
        
        root.addCounter("geoSpatialServiceCallTasks", new Instrument<Long>() {
           @Override
           public void sample() {
               setValue(geoSpatialServiceCallTasks.get());
           }
        });
        
        root.addCounter("geoSpatialServiceCallSubRangeTasks", new Instrument<Long>() {
           @Override
           public void sample() {
               setValue(geoSpatialServiceCallSubRangeTasks.get());
           }
        });

        root.addCounter("zOrderIndexScannedValues", new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(zOrderIndexScannedValues.get());
            }
        });

        root.addCounter("zOrderIndexHits", new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(zOrderIndexHits.get());
            }
        });
        
        root.addCounter("zOrderIndexMisses", new Instrument<Long>() {
            @Override 
            public void sample() {
                setValue(zOrderIndexMisses.get());
            }
        });

        // average #of operator tasks evaluated per query
        root.addCounter("zOrderIndexHitRatio", new Instrument<Double>() {
            @Override
            public void sample() {
               
               final long hits = zOrderIndexHits.get();
               final long misses = zOrderIndexMisses.get();

               if (hits>0 || misses>0)
                  setValue(hits/(double)(hits+misses));
            }
        });
        
        root.addCounter("bigMinCalculationTimeMS", new Instrument<Long>() {
           @Override 
           public void sample() {
               setValue(bigMinCalculationTime.get()/1000000);
           }
       });
        
       root.addCounter("rangeCheckCalculationTimeMS", new Instrument<Long>() {
           @Override 
           public void sample() {
               setValue(rangeCheckCalculationTime.get()/1000000);
           }
       });
       
       root.addCounter("filterCalculationTimeMS", new Instrument<Long>() {
          @Override 
          public void sample() {
              setValue(filterCalculationTime.get()/1000000);
          }
       });
       
       return root;

    }

}

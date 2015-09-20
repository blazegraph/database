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

//    protected final CAT ctr0 = new CAT();
//    public void addToCtr0(final long timeInNanoSec) { ctr0.add(timeInNanoSec); }
//    
//    protected final CAT ctr1 = new CAT();
//    public void addToCtr1(final long timeInNanoSec) { ctr1.add(timeInNanoSec); }
//    
//    protected final CAT ctr2 = new CAT();
//    public void addToCtr2(final long timeInNanoSec) { ctr2.add(timeInNanoSec); }
//
//    protected final CAT ctr3 = new CAT();
//    public void addToCtr3(final long timeInNanoSec) { ctr3.add(timeInNanoSec); }
//
//    protected final CAT ctr4 = new CAT();
//    public void addToCtr4(final long timeInNanoSec) { ctr4.add(timeInNanoSec); }
//
//    protected final CAT ctr5 = new CAT();
//    public void addToCtr5(final long timeInNanoSec) { ctr5.add(timeInNanoSec); }
//
//    protected final CAT ctr6 = new CAT();
//    public void addToCtr6(final long timeInNanoSec) { ctr6.add(timeInNanoSec); }
//
//    protected final CAT ctr7 = new CAT();
//    public void addToCtr7(final long timeInNanoSec) { ctr7.add(timeInNanoSec); }
//
//    protected final CAT ctr8 = new CAT();
//    public void addToCtr8(final long timeInNanoSec) { ctr8.add(timeInNanoSec); }
//
//    protected final CAT ctr9 = new CAT();
//    public void addToCtr9(final long timeInNanoSec) { ctr9.add(timeInNanoSec); }


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
    
    
    @Override
    public CounterSet getCounters() {

        final CounterSet root = new CounterSet();

        root.addCounter("geoSpatialSearchRequests", new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(geoSpatialSearchRequests.get());
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
        
        root.addCounter("bigMinCalculationTime", new Instrument<Long>() {
           @Override 
           public void sample() {
               setValue(bigMinCalculationTime.get()/1000000);
           }
       });
        
       root.addCounter("rangeCheckCalculationTime", new Instrument<Long>() {
           @Override 
           public void sample() {
               setValue(rangeCheckCalculationTime.get()/1000000);
           }
       });
       
       root.addCounter("filterCalculationTime", new Instrument<Long>() {
          @Override 
          public void sample() {
              setValue(filterCalculationTime.get()/1000000);
          }
       });
//       
//       root.addCounter("ctr0", new Instrument<Long>() {
//          @Override 
//          public void sample() {
//              setValue(ctr0.get()/1000000);
//          }
//       });
//       
//       root.addCounter("ctr1", new Instrument<Long>() {
//          @Override 
//          public void sample() {
//              setValue(ctr1.get()/1000000);
//          }
//       });
//       
//       root.addCounter("ctr2", new Instrument<Long>() {
//          @Override 
//          public void sample() {
//              setValue(ctr2.get()/1000000);
//          }
//       });
//       
//       root.addCounter("ctr3", new Instrument<Long>() {
//          @Override 
//          public void sample() {
//              setValue(ctr3.get()/1000000);
//          }
//       });
//       
//       root.addCounter("ctr4", new Instrument<Long>() {
//          @Override 
//          public void sample() {
//              setValue(ctr4.get()/1000000);
//          }
//       });
//       
//       root.addCounter("ctr5", new Instrument<Long>() {
//          @Override 
//          public void sample() {
//              setValue(ctr5.get()/1000000);
//          }
//       });
//       
//       root.addCounter("ctr6", new Instrument<Long>() {
//          @Override 
//          public void sample() {
//              setValue(ctr6.get()/1000000);
//          }
//       });
//       
//       root.addCounter("ctr7", new Instrument<Long>() {
//          @Override 
//          public void sample() {
//              setValue(ctr7.get()/1000000);
//          }
//       });
//       
//       root.addCounter("ctr8", new Instrument<Long>() {
//          @Override 
//          public void sample() {
//              setValue(ctr8.get()/1000000);
//          }
//       });
//       
//       root.addCounter("ctr9", new Instrument<Long>() {
//          @Override 
//          public void sample() {
//              setValue(ctr9.get()/1000000);
//          }
//       });
//       
       
       return root;

    }

}

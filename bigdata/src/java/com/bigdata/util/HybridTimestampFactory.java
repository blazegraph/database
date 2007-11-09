/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
 * Created on Sep 15, 2007
 */

package com.bigdata.util;

import java.math.BigInteger;

import org.apache.log4j.Logger;

/**
 * A timestamp factory using {@link System#currentTimeMillis()} and an internal
 * counter to provide unique timestamps with greater than millisecond
 * resolution.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated This class has not been fully debugged and SHOULD NOT be used.
 */
public class HybridTimestampFactory {

    /**
     * Logger.
     */
    public static final Logger log = Logger.getLogger(HybridTimestampFactory.class);

//    /**
//     * Allows up to 1024 distinct timestamps per millisecond.
//     */
//    public static HybridTimestampFactory INSTANCE = new HybridTimestampFactory();
    
    /**
     * The #of low bits in the generated timestamps that are allocated to the
     * internal counter.
     */
    private final int counterBits;
    
    /**
     * The maximum value of the internal counter before it will rollover.  On
     * rollover the factory sleeps until a new time in milliseconds is reported
     * by {@link System#currentTimeMillis()}.
     */
    private final int maxCounter;

    /**
     * The milliseconds component of the last generated timestamp (without
     * erasure of the hit bits).
     */
    private long lastTimestamp;

    /**
     * The last value of the internal counter used to make a unique timestamp.
     * This is reset each time the time returned by
     * {@link System#currentTimeMillis()} differs from {@link #lastTimestamp}.
     */
    private int counter = 0;

    /**
     * The #of times the factory needed to sleep the current thread in order to
     * generate a distinct timestamp.
     */
    private long sleepCounter = 0L;
    
    /**
     * The #of times the factory needed to sleep the current thread in order to
     * generate a distinct timestamp.
     */
    public long getSleepCounter() {
        
        return sleepCounter;
        
    }
    
    /**
     * Allows up to 1024 distinct timestamps per millisecond.
     */
    protected HybridTimestampFactory() {

        this( 10 );
        
    }
    
    /**
     * Allows up to <code>2^counterBits</code> distinct timestamps per
     * millisecond.
     * 
     * @param counterBits
     *            The #of bits in the long timestamp that are used to represent
     *            a counter.
     * 
     * @todo Compute the maximum period after which the timestamps will
     *       overflow.
     * 
     * @todo Set the epoch when the timestamp factory is created and persist
     *       that epoch so that overflow is set the maximum #of milliseconds
     *       into the future. Note that overflow will be the point after which
     *       we can no longer generate timestamps (aka transaction identifiers)
     *       for a given database.
     */
    public HybridTimestampFactory(int counterBits) {
        
        if (counterBits < 0 || counterBits > 31) {

            // Note: would overflow an int32 at 32 bits.
            
            throw new IllegalArgumentException("counterBits must be in [0:31]");
            
        }
        
        lastTimestamp = 0L;
        
        this.counterBits = counterBits;
        
        /*
         * Construct a bit mask - this will have zeros in the high bits that
         * correspond to the millis and ones in the low bits that correspond to
         * the counter.
         * 
         * @todo this is just Math.pow(2,counterBits)-1 and could be simplified as
         * such (in the WormAddressManager also).
         */
//        final long counterMask;
//        {
//
//            long mask = 0;
//            
//            long bit;
//            
//            for (int i = 0; i < counterBits; i++) {
//
//                bit = (1L << i);
//                
//                mask |= bit;
//                
//            }
//
//            counterMask = mask;
//            
//        }
//
//        /*
//         * The resulting mask is also the maximum value for the counter.
//         */
//
//        maxCounter = (int)counterMask;

        maxCounter = BigInteger.valueOf(2).pow(counterBits).intValue() - 1;

        log.warn("#counterBits="+counterBits+", maxCounter="+maxCounter);
        
    }
    
    public long nextTimestamp() {

        long timestamp = System.currentTimeMillis();
        
        // @todo this still does not work at counterBits == 0.
        if (timestamp == lastTimestamp && (counter == maxCounter || counterBits == 0)) {

            // wait for the next millisecond.
            
            do {

                try {
                    
//                    System.err.print("S");
                    
                    sleepCounter++;
                    
                    Thread.sleep(1/* ms */);
                    
                } catch (InterruptedException ex) {
                    
                    /* ignore */
                    
                }

                timestamp = System.currentTimeMillis();

            } while (timestamp == lastTimestamp);

            // this is a new millisecond.
            
            counter = 0; // reset the counter.

            lastTimestamp = timestamp; // update the last millisecond used.

        } else {

            // otherwise just increment the counter.
            
            counter++;

        }
        
        if (counterBits == 0)
            return timestamp;

        return timestamp << counterBits | counter;

    }

}

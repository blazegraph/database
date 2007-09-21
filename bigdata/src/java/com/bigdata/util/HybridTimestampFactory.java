/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

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

/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 15, 2009
 */

package com.bigdata.util.concurrent;

import java.util.concurrent.Callable;

/**
 * Moving average based on the change in some sampled value.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DeltaMovingAverageTask extends MovingAverageTask {

    /**
     * @param name
     * @param sampleTask
     */
    public DeltaMovingAverageTask(String name,
            Callable<? extends Number> sampleTask) {
        super(name, sampleTask);
    }

    /**
     * @param name
     * @param sampleTask
     * @param w
     */
    public DeltaMovingAverageTask(String name,
            Callable<? extends Number> sampleTask, double w) {
        super(name, sampleTask, w);
    }

    /**
     * Note: don't throw anything from here or it will cause the task to no
     * longer be run!
     */
    public void run() {

        try {

            final double sample = sampleTask.call().doubleValue();
            
            final double delta = sample - oldValue;

            oldValue = sample;

            average = getMovingAverage(average, delta, w);

            nsamples++;

        } catch (Exception ex) {

            log.warn(name, ex);

        }

    }

    private double oldValue = 0d;
    
}

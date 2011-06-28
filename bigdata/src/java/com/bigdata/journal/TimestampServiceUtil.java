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
 * Created on Apr 4, 2008
 */

package com.bigdata.journal;

import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * Robust request for a timestamp from an {@link ITimestampService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TimestampServiceUtil {

    protected static Logger log = Logger.getLogger(TimestampServiceUtil.class);
    
    /**
     * Utility method retries several times if there is a problem before
     * throwing a {@link RuntimeException}.
     * 
     * @param service
     *            The timestamp service.
     * 
     * @return The timestamp.
     */
    static public long nextTimestamp(final ITimestampService service) {

        if (service == null)
            throw new IllegalArgumentException();

        final int maxtries = 3;

        IOException cause = null;

        int ntries;

        for (ntries = 0; ntries < maxtries; ntries++) {

            try {

                return service.nextTimestamp();

            } catch (IOException e) {

                log.warn("Could not get timestamp: " + e, e);

                cause = e;

            }

        }

        log.error("Could not get timestamp after: " + ntries, cause);

        throw new RuntimeException("Could not get timestamp after " + ntries,
                cause);

    }

}

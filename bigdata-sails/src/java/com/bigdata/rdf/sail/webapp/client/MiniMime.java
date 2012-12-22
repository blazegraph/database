/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Mar 7, 2012
 */

package com.bigdata.rdf.sail.webapp.client;

import org.apache.log4j.Logger;

/**
 * Extract and return the quality score for the mime type (defaults to
 * <code>1.0</code>).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Lift out patterns.
 */
public class MiniMime {
    
    static private final Logger log = Logger.getLogger(MiniMime.class);

    public final float q;

    private final String mimeType;

    public final String[][] params;

    public MiniMime(final String s) {
        final String[] b = s.split(";");
        mimeType = b[0];
        float q = 1f;
        params = new String[b.length][];
        for (int i = 1; i < b.length; i++) {
            final String c = b[i];
            final String[] d = c.split("=");
            if (d.length < 2)
                continue;
            params[i] = d;
            // params[i][0] = d[0];
            // params[i][1] = d[1];
            if (!d[0].equals("q"))
                continue;
            q = Float.valueOf(d[1]);
        }
        if (log.isDebugEnabled())
            log.debug("Considering: " + s + " :: q=" + q);
        this.q = q;
    }

    public String getMimeType() {
        return mimeType;
    }
}
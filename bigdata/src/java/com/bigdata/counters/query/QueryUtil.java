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
 * Created on Apr 6, 2009
 */

package com.bigdata.counters.query;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.bigdata.counters.ICounter;
import com.bigdata.counters.ICounterNode;

/**
 * Some static utility methods.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class QueryUtil {

    protected static final Logger log = Logger.getLogger(QueryUtil.class);

    /**
     * Return the data captured by {@link Pattern} from the path of the
     * specified <i>counter</i>.
     * <p>
     * Note: This is used to extract the parts of the {@link Pattern} which are
     * of interest - presuming that you mark some parts with capturing groups
     * and other parts that you do not care about with non-capturing groups.
     * <p>
     * Note: There is a presumption that {@link Pattern} was used to select the
     * counters and that <i>counter</i> is one of the selected counters, in
     * which case {@link Pattern} is KNOWN to match
     * {@link ICounterNode#getPath()}.
     * 
     * @return The captured groups -or- <code>null</code> if there are no
     *         capturing groups.
     */
    static public String[] getCapturedGroups(final Pattern pattern,
            final ICounter counter) {
    
        if (counter == null)
            throw new IllegalArgumentException();
        
        if (pattern == null) {
    
            // no pattern, so no captured groups.
            return null;
            
        }
        
        final Matcher m = pattern.matcher(counter.getPath());
        
        // #of capturing groups in the pattern.
        final int groupCount = m.groupCount();
    
        if(groupCount == 0) {
            
            // No capturing groups.
            return null;
    
        }
    
        if (!m.matches()) {
    
            throw new IllegalArgumentException("No match? counter=" + counter
                    + ", regex=" + pattern);
    
        }
    
        /*
         * Pattern is matched w/ at least one capturing group so assemble a
         * label from the matched capturing groups.
         */
    
        if (log.isDebugEnabled()) {
            log.debug("input  : " + counter.getPath());
            log.debug("pattern: " + pattern);
            log.debug("matcher: " + m);
            log.debug("result : " + m.toMatchResult());
        }
    
        final String[] groups = new String[groupCount];
    
        for (int i = 1; i <= groupCount; i++) {
    
            final String s = m.group(i);
    
            if (log.isDebugEnabled())
                log.debug("group[" + i + "]: " + m.group(i));
    
            groups[i - 1] = s;
    
        }
    
        return groups;
    
    }

}

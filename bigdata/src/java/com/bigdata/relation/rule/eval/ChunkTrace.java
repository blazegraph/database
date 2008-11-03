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
 * Created on Oct 29, 2008
 */

package com.bigdata.relation.rule.eval;

import com.bigdata.relation.rule.eval.JoinMasterTask.JoinTask;

/**
 * Utility class that may be used to trace the chunks accepted for join
 * processing for each predicate. The output has the form
 * 
 * <pre>
 * n[m]
 * </pre>
 * 
 * where <code>n</code> is the evaluation order of the predicate and
 * <code>m</code> is the #of elements in the chunk.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ChunkTrace {

    /**
     * Zero to enable the {@link System#err} trace of chunks accepted for each
     * join dimension -or- <code>-1</code> to disable. This field is the
     * column count of the output and is used to wrap the output so as to not
     * cause eclipse undue distress with long lines.
     * <p>
     * Note: This is static so that it will count for all concurrent
     * {@link JoinTask} for some {@link JoinMasterTask}, at least those running
     * in the same JVM.
     */
    protected static int col = -1;

    static final void chunk(final int orderIndex, final Object[] chunk) {

        if (col >= 0) {

            final String s = Integer.toString(orderIndex) + "["
                    + Integer.toString(chunk.length) + "]";
            
            System.err.print(s);
            
            col = col + s.length() > 80 ? 0 : col + s.length();
            
            if (col == 0) {
            
                System.err.print("\n");
                
            }

        }
        
    }

}

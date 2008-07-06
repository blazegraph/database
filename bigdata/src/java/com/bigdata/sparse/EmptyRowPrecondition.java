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
 * Created on Jul 6, 2008
 */

package com.bigdata.sparse;

/**
 * {@link IPrecondition} succeeds iff there are no property values for the
 * logical row (it recognizes both a <code>null</code>, indicating no
 * property values, and an empty logical row, indicating that an
 * {@link INameFilter} was applied and that there were no property values which
 * satisified that filter).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EmptyRowPrecondition implements IPrecondition {

    /**
     * 
     */
    private static final long serialVersionUID = -1397012918552028222L;

    public boolean accept(ITPS logicalRow) {

        if (logicalRow == null || logicalRow.size() == 0) {

            return true;

        }

        return false;
        
    }

}

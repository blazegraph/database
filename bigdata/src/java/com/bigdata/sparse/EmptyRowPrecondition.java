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

import org.apache.log4j.Logger;

/**
 * {@link IPrecondition} succeeds iff there are no property values for the
 * logical row (it recognizes a <code>null</code>, indicating no property
 * values, and an empty logical row, indicating that an {@link INameFilter} was
 * applied and that there were no property values which satisified that filter,
 * and a deleted property value for the primary key, which is often used as a
 * shorthand for deleting the logical row).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EmptyRowPrecondition implements IPrecondition {

    protected static final transient Logger log = Logger.getLogger(EmptyRowPrecondition.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = -1397012918552028222L;

    public boolean accept(ITPS logicalRow) {

        if (logicalRow == null) {
        
            if(log.isInfoEnabled()) {
                
                log.info("No property values for row: (null)");
                
            }
            
            return true;
            
        }
        
        if(logicalRow.size() == 0) {

            if(log.isInfoEnabled()) {
                
                log.info("Logical row size is zero: "+logicalRow);
                
            }

            return true;
            
        }
        
        if(logicalRow.getPrimaryKey() == null) {

            if(log.isInfoEnabled()) {
                
                log.info("Primary key row is deleted: "+logicalRow);
                
            }
            
            return true;

        }

        return false;
        
    }

}

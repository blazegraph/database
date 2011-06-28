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

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Base class may be used for combining {@link IPrecondition}. The base class
 * by itself always succeeds, but you can add additional preconditions to be
 * tested.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Precondition implements IPrecondition {

    protected static final transient Logger log = Logger.getLogger(Precondition.class);

    /**
     * 
     */
    private static final long serialVersionUID = -5628116520592503669L;

    private final List<IPrecondition> conditions = new LinkedList<IPrecondition>();
    
    /**
     * De-serializator ctor.
     */
    public Precondition() {
        
    }

    public Precondition(IPrecondition c) {
        
        add(c);
        
    }

    public void add(IPrecondition c) {
        
        if (c == null)
            throw new IllegalArgumentException();
        
        conditions.add( c );
        
    }
    
    public boolean accept(ITPS logicalRow) {
        
        for(IPrecondition c : conditions) {
        
            if(!c.accept(logicalRow)) {
                
                if(log.isInfoEnabled()) {
                    
                    log.info("Failed: condition="+c+", logicalRow="+logicalRow);
                    
                }
                
                return false;
                
            }
            
        }
        
        return true;
        
    }

}

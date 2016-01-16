/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Jun 10, 2011
 */

package com.bigdata.rdf.lexicon;

import com.bigdata.bop.IPredicate;

/**
 * Type safe enumeration of access patterns for the {@link LexPredicate}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum LexAccessPatternEnum {

    /** Only the IV position is bound. */
    IVBound,
    /** Only the Value position is bound. */
    ValueBound,
    /** Neither position is bound. */
    NoneBound,
    /** Both positions are bound. */
    FullyBound;
    
    static public LexAccessPatternEnum valueOf(final IPredicate<?> predicate) {
        
        if (predicate.arity() != 2)
            throw new IllegalArgumentException();

        if (predicate.get(1/* IV */).isConstant()) {

            if(predicate.get(0/* Value */).isConstant()) {
            
                return FullyBound;
                
            }
            
            return IVBound;
            
        } else if (predicate.get(0/* Value */).isConstant()) {
            
            return ValueBound;
            
        } else {
            
            return NoneBound;
            
        }

    }

}

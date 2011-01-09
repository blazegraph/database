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
 * Created on Aug 26, 2008
 */

package com.bigdata.rdf.vocab;

import java.util.Iterator;
import org.openrdf.model.Value;
import com.bigdata.rdf.internal.IV;
import com.bigdata.bop.IConstant;
import com.bigdata.rdf.internal.IV;

/**
 * Interface for a pre-defined vocabulary.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface Vocabulary {

    /**
     * The term identifier for the pre-defined {@link Value}.
     * 
     * @param value
     *            The value.
     * @return The term identifier.
     * 
     * @throws IllegalArgumentException
     *             if that {@link Value} is not defined for this vocabulary.
     */
    public IV get(Value value);
    
    /**
     * Returns the {@link IConstant} for the {@link Value}.
     * 
     * @param value
     *            The value.
     *            
     * @return The {@link IConstant}.
     * 
     * @throws IllegalArgumentException
     *             if that {@link Value} is not defined for this vocabulary.
     */
    public IConstant<IV> getConstant(Value value);
    
    /**
     * The #of defined {@link Value}s.
     * 
     * @throws IllegalStateException
     *             if the values have not been defined.
     */
    int size();

    /**
     * The {@link Value}s in an arbitrary order.
     * 
     * @throws IllegalStateException
     *             if the values have not been defined.
     */
    Iterator<Value> values();

}

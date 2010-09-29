/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Sep 29, 2010
 */

package cutthecrap.utils.striterators;

import java.io.Serializable;

/**
 * Interface for a method which filters objects visited by a {@link Filterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IFilterTest extends Serializable {

    /**
     * True iff the argument is matched by the filter.
     * 
     * @param obj
     *            Something visited by the iterator.
     * 
     * @return true iff that object is accepted by the filter.
     */
    public boolean isValid(Object e);

}

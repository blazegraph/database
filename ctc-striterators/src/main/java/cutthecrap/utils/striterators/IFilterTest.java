/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
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

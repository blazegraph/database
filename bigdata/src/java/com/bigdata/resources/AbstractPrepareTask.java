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
 * Created on Feb 2, 2009
 */

package com.bigdata.resources;

import java.lang.ref.SoftReference;

/**
 * Base class for the prepare phase which reads on the old journal.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractPrepareTask<T> extends
        AbstractResourceManagerTask<T> {
    
    /**
     * @param resourceManager
     * @param timestamp
     * @param resource
     */
    public AbstractPrepareTask(ResourceManager resourceManager, long timestamp,
            String resource) {

        super(resourceManager, timestamp, resource);

    }

    /**
     * @param resourceManager
     * @param timestamp
     * @param resource
     */
    public AbstractPrepareTask(ResourceManager resourceManager, long timestamp,
            String[] resource) {

        super(resourceManager, timestamp, resource);
        
    }

    /**
     * Method is responsible for clearing the {@link SoftReference}s held by
     * {@link ViewMetadata} for the source view(s) on the old journal.
     * <p>
     * Note: This method MUST be invoked in order to permit those references to
     * be cleared more eagerly than the end of the entire asynchronous overflow
     * operation (which is when the task references would themselves go out of
     * scope and become available for GC).
     */
    abstract protected void clearRefs();
    
}

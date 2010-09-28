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
 * Created on Sep 28, 2010
 */

package com.bigdata.bop;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Annotations for an operator using an internal concurrent hash map.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ConcurrentHashMapAnnotations extends HashMapAnnotations {

    /**
     * The concurrency level of the {@link ConcurrentHashMap} used to impose the
     * distinct constraint.
     * 
     * @see #DEFAULT_CONCURRENCY_LEVEL
     */
    String CONCURRENCY_LEVEL = ConcurrentHashMapAnnotations.class.getName()
            + ".concurrencyLevel";

    int DEFAULT_CONCURRENCY_LEVEL = 16;

}

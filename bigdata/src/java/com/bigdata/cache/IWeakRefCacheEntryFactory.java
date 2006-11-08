/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Dec 13, 2005
 */
package com.bigdata.cache;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;

/**
 * Interface supports choice of either weak or soft references for cache
 * entries and makes it possible for the application to extend the metadata
 * associated with and entry in the {@link WeakValueCache}.
 * 
 * @author thompsonbry
 * @version $Id$
 */
public interface IWeakRefCacheEntryFactory<T>
{

    /**
     * Creates a weak reference object to serve as the value in the cache for
     * the given application object.
     * 
     * @param key
     *            The object identifier.
     * 
     * @param obj
     *            The application object.
     * 
     * @param queue
     *            The weak or soft reference object must be created such that it
     *            will appear on this queue when the reference is cleared by the
     *            garbage collector.
     * 
     * @return The new cache entry for that application object.
     * 
     * @see WeakReference
     * @see SoftReference
     */
    
    public IWeakRefCacheEntry<T> newCacheEntry( long key, T obj, ReferenceQueue<T> queue );
    
}

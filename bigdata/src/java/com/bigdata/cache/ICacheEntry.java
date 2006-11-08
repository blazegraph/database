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

/**
 * <p>
 * Interface for hard reference cache entries exposes a <i>dirty</i> flag in
 * addition to the object identifier and object reference.
 * </p>
 * 
 * @author thompsonbry
 * @version $Id$
 * 
 * @todo long oid to Object (see {@link ICachePolicy}.
 * 
 * @todo Support clearing updated objects for hot cache between transactions.
 *       Add metadata boolean that indicates whether the object was modified
 *       since the cache was last cleared regardless of whether the object has
 *       since been installed on the persistence layer and marked as clean. The
 *       purpose of this is to support clearing of modified objects from the
 *       object cache when a transaction is aborted. Such objects must be
 *       cleared if they have been modified since they were installed in the
 *       cache regardless of whether they are currently dirty or not. Supporting
 *       this feature will probably require a hard reference Set containing the
 *       object identifier of each object that was marked as dirty in the cache
 *       since the cache was last cleared.
 */
public interface ICacheEntry<K,T> extends IWeakRefCacheEntry<K,T> {

    /**
     * Return true iff the object associated with this entry is dirty.
     */
    public boolean isDirty();

    /**
     * Set the dirty flag.
     * 
     * @param dirty
     *            true iff the object associated with this entry is dirty.
     */
    public void setDirty(boolean dirty);

}

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
 * Created on Nov 20, 2006
 */

package com.bigdata.objndx;

/**
 * Interface for a B-Tree mapping arbitrary non-null keys to arbitrary values.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IBTree {

    /**
     * Insert an entry under the external key.
     * 
     * @param key
     *            The external key.
     * @param entry
     *            The value.
     *            
     * @return The previous entry under that key or <code>null</code> if the
     *         key was not found.
     */
    public Object insert(Object key, Object entry);

    /**
     * Lookup an entry for an external key.
     * 
     * @return The entry or null if there is no entry for that key.
     */
    public Object lookup(Object key);

    /**
     * Remove the entry for the external key.
     * 
     * @param key
     *            The external key.
     * 
     * @return The entry stored under that key and null if there was no
     *         entry for that key.
     */
    public Object remove(Object key);
    
    /**
     * Return an iterator that visits key-value pairs in a half-open key range.
     * 
     * @param fromKey
     *            The lowest key that will be visited (inclusive).
     * @param toKey
     *            The first key that will not be visited (exclusive).
     */
    public IRangeIterator rangeIterator(Object fromKey, Object toKey);
    
}

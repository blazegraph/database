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
 * Created on Feb 7, 2007
 */

package com.bigdata.btree;

/**
 * <p>
 * Interface for non-batch operations on a B+-Tree mapping arbitrary non-null
 * keys to arbitrary values.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo re-define this interface for byte[] keys
 * 
 * @see UnicodeKeyBuilder, which may be used to encode one or more primitive data type
 *      values or Unicode strings into a variable length unsigned byte[] key.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ISimpleBTree extends IRangeQuery {

    /**
     * Insert or update a value under the key.
     * 
     * @param key
     *            The key. 
     * @param value
     *            The value (may be null).
     * 
     * @return The previous value under that key or <code>null</code> if the
     *         key was not found.
     */
    public Object insert(Object key, Object value);
    
    /**
     * Lookup a value for a key.
     * 
     * @return The value or <code>null</code> if there is no entry for that
     *         key.
     */
    public Object lookup(Object key);

    /**
     * Return true iff there is an entry for the key.
     * 
     * @param key
     *            The key.
     * 
     * @return True if the btree contains an entry for that key.
     */
    public boolean contains(byte[] key);
        
    /**
     * Remove the key and its associated value.
     * 
     * @param key
     *            The key.
     * 
     * @return The value stored under that key or <code>null</code> if the key
     *         was not found.
     */
    public Object remove(Object key);

}

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
 * Created on Dec 5, 2006
 */

package com.bigdata.objndx;

/**
 * An iterator that visits key-value pairs in a half-open range.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Implement the key range iterator.
 * 
 * @todo Support iteration with concurrent structural modification. The iterator
 *       will have to listen for structural modifications and perhaps for insert /
 *       delete key operations on the current leaf.
 */
public class RangeIterator implements IRangeIterator {

    private final BTree btree;
    private final Object fromKey;
    private final Object toKey;
    
    /**
     * Create an iterator that will visit key-value pairs in the half-open range
     * from <tt>fromKey</tt>, inclusive, to <tt>toKey</tt>, exclusive.
     * 
     * @param fromKey
     *            The lowest key that will be visited by the iterator.
     * @param toKey
     *            The first key that will not be visited by the iterator.
     */
    public RangeIterator(BTree btree, Object fromKey, Object toKey ) {
        
        this.btree = btree;
        
        this.fromKey = fromKey;
        
        this.toKey = toKey;
        
    }

    public boolean hasNext() {

        throw new UnsupportedOperationException();
        
    }

    /**
     * Return the next value.
     */
    public Object next() {

        throw new UnsupportedOperationException();
        
    }

    public void remove() {

        throw new UnsupportedOperationException();
        
    }
    
    public Object getValue() {

        throw new UnsupportedOperationException();

    }

    public Object getKey() {

        throw new UnsupportedOperationException();

    }

}

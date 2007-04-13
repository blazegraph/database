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
 * Created on Feb 16, 2007
 */

package com.bigdata.btree;

import java.util.UUID;

/**
 * A fly-weight wrapper that does not permit write operations and reads
 * through onto an underlying {@link IIndex}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReadOnlyIndex implements IIndex, IRangeQuery {

    private final IIndex src;
    
    public ReadOnlyIndex(IIndex src) {
        
        if(src==null) throw new IllegalArgumentException();
        
        this.src = src;
        
    }

    public UUID getIndexUUID() {
        return src.getIndexUUID();
    }
    
    public boolean contains(byte[] key) {
        return src.contains(key);
    }

    public Object insert(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    public Object lookup(Object key) {
        return src.lookup(key);
    }

    public Object remove(Object key) {
        throw new UnsupportedOperationException();
    }

    public int rangeCount(byte[] fromKey, byte[] toKey) {
        return src.rangeCount(fromKey, toKey);
    }

    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey) {
        return src.rangeIterator(fromKey, toKey);
    }

    public void contains(BatchContains op) {
        src.contains(op);
    }

    public void insert(BatchInsert op) {
        throw new UnsupportedOperationException();
    }

    public void lookup(BatchLookup op) {
        src.lookup(op);
    }

    public void remove(BatchRemove op) {
        throw new UnsupportedOperationException();
    }

}

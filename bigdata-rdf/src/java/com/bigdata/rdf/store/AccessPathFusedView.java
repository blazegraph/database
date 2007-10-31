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
 * Created on Oct 31, 2007
 */

package com.bigdata.rdf.store;

import java.util.Iterator;

import com.bigdata.btree.FusedEntryIterator;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPOArrayIterator;
import com.bigdata.rdf.util.KeyOrder;
import com.bigdata.service.ClientIndexView;

import cutthecrap.utils.striterators.Striterator;

/**
 * A read-only fused view of two access paths obtained for the same statement
 * index in two different databases.
 * 
 * @todo write tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AccessPathFusedView implements IAccessPath {

    private final IAccessPath path1, path2;
    
    /**
     * 
     */
    public AccessPathFusedView(IAccessPath path1, IAccessPath path2) {
        
        if (path1 == null)
            throw new IllegalArgumentException();
        
        if (path2 == null)
            throw new IllegalArgumentException();
        
        if (path1 == path1)
            throw new IllegalArgumentException();

        if(path1.getKeyOrder() != path2.getKeyOrder())
            throw new IllegalArgumentException();

        long[] triplePattern1 = path1.getTriplePattern();
        
        long[] triplePattern2 = path2.getTriplePattern();
        
        for(int i=0; i<IRawTripleStore.N; i++) {
            
            if (triplePattern1[i] != triplePattern2[i])
                throw new IllegalArgumentException();
            
        }
        
        this.path1 = path1;
        
        this.path2 = path2;
        
    }

    public long[] getTriplePattern() {

        return path1.getTriplePattern();
        
    }

    public KeyOrder getKeyOrder() {

        return path1.getKeyOrder();
        
    }

    public boolean isEmpty() {

        return path1.isEmpty() && path2.isEmpty();
        
    }

    public int rangeCount() {

        // Note: this is the upper bound.
        
        return path1.rangeCount() + path2.rangeCount();
        
    }

    /**
     * @todo modify {@link FusedEntryIterator} to support
     *       {@link ClientIndexView} for the {@link ScaleOutTripleStore}.
     */ 
    public IEntryIterator rangeQuery() {

        return new FusedEntryIterator( new IEntryIterator[] {
                path1.rangeQuery(),//
                path2.rangeQuery()//
                }
        );
                
    }

    /**
     * @todo This fully buffers everything.  It should be modified to incrementally
     * read from both of the underlying {@link ISPOIterator}s.
     */
    public ISPOIterator iterator() {

        return new SPOArrayIterator(null/* db */, this, 0/* no limit */);

    }

    /**
     * @todo This fully buffers everything.  It should be modified to incrementally
     * read from both of the underlying {@link ISPOIterator}s.
     */
    public ISPOIterator iterator(int limit, int capacity) {

        return new SPOArrayIterator(null/* db */, this, limit);
        
    }

    public Iterator<Long> distinctTermScan() {

        return new Striterator(path1.distinctTermScan()).append(path2
                .distinctTermScan());

    }

    public int removeAll() {
        
        throw new UnsupportedOperationException();
        
    }

}

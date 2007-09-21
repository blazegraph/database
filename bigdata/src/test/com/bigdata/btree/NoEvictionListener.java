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
 * Created on Nov 17, 2006
 */
package com.bigdata.btree;

import com.bigdata.cache.HardReferenceQueue;

/**
 * Hard reference cache eviction listener for leaves always throws an
 * exception. This is used for some unit tests to ensure that cache
 * evictions are not occurring and that copy on write situations are
 * therefore never triggered (except that they will of course be triggered
 * following a commit).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see SimpleEntry#NoSerializer
 */
public class NoEvictionListener implements
        IEvictionListener {

    public void evicted(HardReferenceQueue<PO> cache, PO ref) {

        assert ref instanceof Leaf;
        
        if( ref.isDirty() ) {

            throw new UnsupportedOperationException(
                    "Leaf eviction is disabled for this unit test: leaf=" + ref);
            
        }

    }

}
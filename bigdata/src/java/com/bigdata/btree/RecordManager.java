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
 * Created on Oct 25, 2006
 */

package com.bigdata.btree;

/**
 * Interface encapsulates allocation operations on a persistence store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated This is being removed shortly. It is basically replaced by
 *             {@link IOM} and some low level operations in {@link BTree}.
 */
public interface RecordManager {

    /**
     * FIXME This assumption is not valid for the journal, which permits slot 0
     * as valid.  We can restrict the journal to deny slot zero, which might be
     * wise.  There really needs to be an oid for null.  (Also, the journal is
     * using int32 identifiers and long's are actually encoding contiguous slot
     * allocations formed from the firstSlot offset and the #of bytes in the
     * allocation).
     */
    long NULL_RECID = 0L;

//    long insert(Object obj);
//    
//    Object fetch(long id);
//    
//    void update(long id,Object obj);
//    
//    void delete(long id);

//    // @todo remove these two methods from this API once the tests are in line.
//    void close();
//    void commit();
    
}

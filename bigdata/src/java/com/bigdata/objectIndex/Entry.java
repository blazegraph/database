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
 * Created on Nov 15, 2006
 */
package com.bigdata.objectIndex;

import com.bigdata.journal.ISlotAllocation;
import com.bigdata.journal.SimpleObjectIndex.IObjectIndexEntry;

/**
 * An entry in a {@link Leaf}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Reconcile with {@link IObjectIndexEntry} and {@link NodeSerializer}.
 */
public class Entry implements IObjectIndexEntry {

    private static int nextId = 1;
    private int id;
    
    /**
     * Create a new entry.
     */
    public Entry() {
        id = nextId++;
    }

    /**
     * Copy constructor.
     * 
     * @param src
     *            The source to be copied.
     */
    public Entry(IObjectIndexEntry src) {
        id = (src instanceof Entry ?((Entry)src).id : nextId++);
    }

    public String toString() {
        return ""+id;
    }

    public ISlotAllocation getCurrentVersionSlots() {
        // TODO Auto-generated method stub
        return null;
    }

    public ISlotAllocation getPreExistingVersionSlots() {
        // TODO Auto-generated method stub
        return null;
    }

    public short getVersionCounter() {
        // TODO Auto-generated method stub
        return 0;
    }

    public boolean isDeleted() {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean isPreExistingVersionOverwritten() {
        // TODO Auto-generated method stub
        return false;
    }
    
}
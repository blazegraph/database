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
 * Created on Nov 5, 2006
 */
package com.bigdata.objectIndex;

import com.bigdata.journal.ISlotAllocation;
import com.bigdata.journal.SlotMath;
import com.bigdata.journal.SimpleObjectIndex.IObjectIndexEntry;

/**
 * A non-persistence capable implementation of {@link IObjectIndexEntry}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class IndexEntry implements IObjectIndexEntry {

    private final SlotMath slotMath;

    private short versionCounter;

    private long currentVersion;

    private long preExistingVersion;

    private IndexEntry() {

        throw new UnsupportedOperationException();

    }

    IndexEntry(SlotMath slotMath, short versionCounter,
            long currentVersion, long preExistingVersion) {

        this.slotMath = slotMath;
        this.versionCounter = versionCounter;
        this.currentVersion = currentVersion;
        this.preExistingVersion = preExistingVersion;

    }

    public short getVersionCounter() {

        return versionCounter;

    }

    public boolean isDeleted() {

        return currentVersion == 0L;

    }

    public boolean isPreExistingVersionOverwritten() {

        return preExistingVersion != 0L;

    }

    public ISlotAllocation getCurrentVersionSlots() {

        if (currentVersion == 0L)
            return null;

        return slotMath.toSlots(currentVersion);

    }

    public ISlotAllocation getPreExistingVersionSlots() {

        if (preExistingVersion == 0L)
            return null;

        return slotMath.toSlots(preExistingVersion);

    }

    /**
     * Dumps the state of the entry.
     */
    public String toString() {
        return "{versionCounter=" + versionCounter + ", currentVersion="
                + currentVersion + ", preExistingVersion=" + preExistingVersion
                + "}";
    }

}

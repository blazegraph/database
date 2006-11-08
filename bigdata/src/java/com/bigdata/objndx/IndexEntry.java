package com.bigdata.objndx;

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

package com.bigdata.rawstore;

import java.io.Serializable;
import java.util.UUID;

import com.bigdata.mdi.IResourceMetadata;

/**
 * Static class since must be {@link Serializable}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: AbstractBTree.java 4336 2011-03-24 01:04:33Z thompsonbry $
 */
public final class TransientResourceMetadata implements IResourceMetadata {

    private final UUID uuid;
    
    public TransientResourceMetadata(final UUID uuid) {
        this.uuid = uuid;
    }

    private static final long serialVersionUID = 1L;

    public boolean isJournal() {
        return false;
    }

    public boolean isIndexSegment() {
        return false;
    }

    public boolean equals(IResourceMetadata o) {
        return false;
    }

    public long getCreateTime() {
        return 0L;
    }
    
    public long getCommitTime() {
        return 0L;
    }

    public String getFile() {
        return "";
    }

    public UUID getUUID() {
        return uuid;
    }

}
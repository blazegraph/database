/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
/*
 * Created on May 2, 2009
 */

package com.bigdata.io.compression;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.zip.Deflater;

/**
 * A serializable compression provider based on {@link RecordCompressor}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultRecordCompressorFactory implements
        IRecordCompressorFactory<RecordCompressor>, Externalizable {

    private int level;

    /**
     * Instance configured for {@link Deflater#BEST_SPEED}.
     */
    public IRecordCompressorFactory<RecordCompressor> BEST_SPEED = new DefaultRecordCompressorFactory(
            Deflater.BEST_SPEED);

    /**
     * Instance configured for {@link Deflater#BEST_COMPRESSION}.
     */
    public IRecordCompressorFactory<RecordCompressor> BEST_COMPRESSION = new DefaultRecordCompressorFactory(
            Deflater.BEST_COMPRESSION);

    public String toString() {
        
        return getClass().getName() + "{level=" + level + "}";
        
    }

    private DefaultRecordCompressorFactory(final int level) {

        this.level = level;

    }

    public RecordCompressor getInstance() {

        return new RecordCompressor(level);

    }

    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        level = in.readInt();

    }

    public void writeExternal(final ObjectOutput out) throws IOException {

        out.writeInt(level);

    }

}

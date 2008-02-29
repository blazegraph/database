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
 * Created on Feb 29, 2008
 */

package com.bigdata.resources;

import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.mdi.MetadataIndex;

/**
 * Task performs an atomic update of the index partition view definitions on the
 * live journal and the {@link MetadataIndex}, thereby putting into effect the
 * changes made by a {@link JoinIndexPartitionTask}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class UpdateJoinIndexPartition extends AbstractTask {

    /**
     * @param concurrencyManager
     * @param startTime
     * @param resource
     */
    public UpdateJoinIndexPartition(IConcurrencyManager concurrencyManager,
            long startTime, String resource) {
        super(concurrencyManager, startTime, resource);
        // TODO Auto-generated constructor stub
    }

    /**
     * @param concurrencyManager
     * @param startTime
     * @param resource
     */
    public UpdateJoinIndexPartition(IConcurrencyManager concurrencyManager,
            long startTime, String[] resource) {
        super(concurrencyManager, startTime, resource);
        // TODO Auto-generated constructor stub
    }

    @Override
    protected Object doTask() throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

}

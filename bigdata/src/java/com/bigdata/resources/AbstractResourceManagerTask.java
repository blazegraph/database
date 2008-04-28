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
 * Created on Mar 12, 2008
 */

package com.bigdata.resources;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ITx;
import com.bigdata.service.IDataService;
import com.bigdata.service.IMetadataService;

/**
 * Abstract base class for tasks run during post-processing of a journal by the
 * {@link ResourceManager}. These tasks are designed with a historical read
 * that handles history up to some specified commitTime and the submits an
 * {@link ITx#UNISOLATED} task that brings things up to date and coordinates an
 * atomic update between the {@link IDataService}(s) and the
 * {@link IMetadataService}. You SHOULD be able to run any task at any time as
 * long as overflow is disabled while these tasks are running (this is a
 * pre-condition for all of these tasks).
 * 
 * FIXME There is a potential failure point once we notify the metadata service
 * of the change in the index partitions since this is done before the task has
 * actually committed its changes on the live journal. If the commit on the
 * journal fails then the metadata index will now reference index partitions
 * which do not exist (or do not have all their data) on the live journal. A
 * full transaction might be one way to close this gap. The opportunity for
 * failure could be significantly reduced by checkpointing all indices before we
 * RMI the MDS.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractResourceManagerTask extends AbstractTask {
    
    /**
     * Note: Logger shadows {@link AbstractTask#log}.
     */
    static protected final Logger log = Logger.getLogger(AbstractResourceManagerTask.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    static final protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    static final protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    protected final ResourceManager resourceManager;
    
    /**
     * @param resourceManager
     * @param timestamp
     * @param resource
     * @param latch
     */
    public AbstractResourceManagerTask(ResourceManager resourceManager, long timestamp,
            String resource) {

        super(resourceManager.getConcurrencyManager(), timestamp, resource);

        this.resourceManager = resourceManager;
        
    }

    /**
     * @param resourceManager
     * @param timestamp
     * @param resource
     * @param latch
     */
    public AbstractResourceManagerTask(ResourceManager resourceManager, long timestamp,
            String[] resource) { //, ILatch latch) {

        super(resourceManager.getConcurrencyManager(), timestamp, resource);

        this.resourceManager = resourceManager;

    }

}

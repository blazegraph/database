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
 * Created on Dec 18, 2008
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.Properties;

import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.ValidationError;

/**
 * Implementation for a standalone journal using single-phase commits.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class JournalTransactionService extends
        AbstractTransactionService {

    protected final Journal journal;

    /**
     * @param properties
     */
    public JournalTransactionService(Properties properties,
            final Journal journal) {

        super(properties);

        this.journal = journal;

    }

    protected void abortImpl(final long tx) {

        final ITx t = journal.getLocalTransactionManager().getTx(tx);

        if (t == null)
            throw new IllegalStateException();

        t.abort();

    }

    protected void commitImpl(final long tx, final long commitTime)
            throws ValidationError {

        if (commitTime <= journal.getRootBlockView().getLastCommitTime()) {

            // commit times must strictly advance.
            throw new IllegalArgumentException();

        }

        final ITx t = journal.getLocalTransactionManager().getTx(tx);

        if (t == null)
            throw new IllegalStateException();

        if (t.isEmptyWriteSet()) {

            t.prepare(0L/* commitTime */);

            t.commit();

        } else {

            t.prepare(commitTime);

            t.commit();

        }

    }

    /**
     * The last commit time from the current root block.
     */
    final public long lastCommitTime() {
        
        return journal.getRootBlockView().getLastCommitTime();
        
    }

    /**
     * Ignored since the {@link Journal} records the last commit time
     * in its root blocks.
     */
    public void notifyCommit(long commitTime) throws IOException {
    
        // NOP
        
    }

    /**
     * Ignored since the {@link Journal} can not release history.
     */
    public void setReleaseTime(long releaseTime) throws IOException {
        
        // NOP
        
    }
    
}

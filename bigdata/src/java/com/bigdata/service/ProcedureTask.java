/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Jan 10, 2008
 */
package com.bigdata.service;

import com.bigdata.btree.IIndexProcedure;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrentJournal;

/**
 * Class provides an adaptor allowing a {@link IIndexProcedure} to be executed on the
 * {@link ConcurrentJournal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ProcedureTask extends AbstractTask {

    protected final IIndexProcedure proc;

    /**
     * 
     * @param journal
     * @param startTime
     * @param readOnly
     * @param name
     * @param proc
     */
    public ProcedureTask(ConcurrentJournal journal, long startTime,
            boolean readOnly, String name, IIndexProcedure proc) {

        super(journal, startTime, readOnly, name);

        if (proc == null)
            throw new IllegalArgumentException();

        this.proc = proc;

    }

    final public Object doTask() throws Exception {

        return proc.apply(getIndex(getOnlyResource()));

    }

}

/*

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
package com.bigdata.service.mapred;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.UUID;

/**
 * Abstract base class for {@link IMapTask}s accepting a filename as the
 * "key" and the file contents as the "value".
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractFileInputMapTask extends
        AbstractMapTask {

    protected AbstractFileInputMapTask(UUID uuid, Object source, int nreduce,
            IHashFunction hashFunction) {

        super(uuid, source, nreduce, hashFunction);

    }
    
    /**
     * Used to read from the {@link #getSource()} when it is a {@link File}.
     * 
     * @param file
     *            The data source.
     * 
     * @throws Exception
     */
    final public void input(File file) throws Exception {

        log.info("Start file: " + file);

        final InputStream is = new BufferedInputStream(new FileInputStream(
                file));

        try {

            input(file, is);

            log.info("Done file : " + file + ", ntuples="
                            + getTupleCount());

        } finally {

            try {

                is.close();

            } catch (Throwable t) {

                log.warn("Problem closing input stream: " + file, t);

            }

        }

    }

    abstract protected void input(File input, InputStream is)
            throws Exception;

}

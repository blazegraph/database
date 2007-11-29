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
package com.bigdata.service.mapred.tasks;

import java.io.File;
import java.io.InputStream;
import java.util.UUID;

import com.bigdata.service.mapred.AbstractFileInputMapTask;
import com.bigdata.service.mapred.IHashFunction;

/**
 * Reads the bytes and throws them away.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReadOnlyMapTask extends AbstractFileInputMapTask {

    /**
     * 
     */
    private static final long serialVersionUID = 2055918237931155126L;

    public ReadOnlyMapTask(UUID uuid, Object source, Integer nreduce, IHashFunction hashFunction) {

        super(uuid, source, nreduce, hashFunction);

    }
    
    public void input(File file, InputStream is) throws Exception {

        while(true) {
            
            int ch = is.read();
            
            if(ch==-1) break;
            
        }
        
    }

}
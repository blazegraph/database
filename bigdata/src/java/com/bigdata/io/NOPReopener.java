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
 * Created on Feb 10, 2009
 */

package com.bigdata.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * Implementation that will not re-open the {@link FileChannel} once it has been
 * closed. This is useful for simple things where you still want the reliability
 * guarentees of {@link FileChannelUtility}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NOPReopener implements IReopenChannel {
    
    private final FileChannel channel;
    
    public NOPReopener(final FileChannel channel) {

        if (channel == null)
            throw new IllegalArgumentException();

        this.channel = channel;
        
    }

    public NOPReopener(final RandomAccessFile raf) {

        if (raf == null)
            throw new IllegalArgumentException();

        this.channel = raf.getChannel();
        
    }
    
    public FileChannel reopenChannel() throws IOException {
    
        if (!channel.isOpen())
            throw new IOException("Channel is closed.");
        
        return channel;
    
    }

}

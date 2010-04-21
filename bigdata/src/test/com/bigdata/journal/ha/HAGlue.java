/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Apr 21, 2010
 */

package com.bigdata.journal.ha;

import java.util.concurrent.RunnableFuture;

import com.bigdata.journal.IRootBlockView;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class HAGlue {

    public interface BufferDescriptor {

    }

    /*
     * write pipeline.
     */
    
    public RunnableFuture<Void> write(long newaddr, long oldaddr, long chk, BufferDescriptor bd) {
        
        throw new UnsupportedOperationException();
        
    }

    /*
     * file extension.
     */
    
    public RunnableFuture<Void> truncate(long extent) {
        
        throw new UnsupportedOperationException();
        
    }

    /*
     * commit protocol.
     */

    public void commitNow(long commitTime) {
     
        throw new UnsupportedOperationException();

    }
    
    public void abort() {
 
        throw new UnsupportedOperationException();

    }
    
    public interface Vote {
        int getYesCount();
        int getNoCount();
    }
    
    RunnableFuture<Vote> prepare2Phase(long commitCounter,BufferDescriptor bd) {

        throw new UnsupportedOperationException();

    }

    RunnableFuture<Void> commit2Phase(long commitCounter) {

        throw new UnsupportedOperationException();

    }

    RunnableFuture<Void> abort2Phase(long commitCounter) {

        throw new UnsupportedOperationException();

    }

    /*
     * @todo Quorum membership
     */
    
}

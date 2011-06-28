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
 * Created on Nov 15, 2008
 */

package com.bigdata.service.proxy;

import java.io.IOException;
import java.rmi.Remote;

import com.bigdata.relation.accesspath.IBuffer;

/**
 * {@link Remote} interface declaring the API of {@link IBuffer} but also
 * declaring that each methods throws {@link IOException} in order to be
 * compatible with {@link Remote} and {@link Exporter}. Of course, this means
 * that this interface can not extend {@link IBuffer}!
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface RemoteBuffer<E> extends Remote {

    public int size() throws IOException;

    public boolean isEmpty() throws IOException;
    
    public void add(E e) throws IOException;
    
    public long flush() throws IOException;

    public void reset() throws IOException;
    
}

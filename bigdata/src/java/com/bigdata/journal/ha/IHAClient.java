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

import com.bigdata.io.ObjectSocketChannelStream;
import com.bigdata.io.WriteCache;
import com.bigdata.journal.ha.SocketMessage.AckMessage;
import com.bigdata.journal.ha.SocketMessage.HAWriteMessage.HAWriteConfirm;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IHAClient {

	ObjectSocketChannelStream getNextSocket();

	WriteCache getWriteCache();

	/**
	 * Retrieves the input stream from which messages are deserialized.  This allows messages to access
	 * their stream when applying their behaviour.
	 */
	ObjectSocketChannelStream getInputSocket();
	
	void setInputSocket(ObjectSocketChannelStream in);

	/**
	 * 
	 * @param extent
	 */
	void truncate(long extent);
}

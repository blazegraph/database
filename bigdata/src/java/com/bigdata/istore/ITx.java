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
 * Created on Oct 26, 2006
 */

package com.bigdata.istore;

/**
 * A transactional object manager.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo The reason to expose prepare separately at the application level is so
 *       that distributed hybrid transactions may be implementation.  However, 
 *       that is a sticky wicket and we are better off with commit implictly
 *       preparing the transaction for now.
 */
public interface ITx extends IOM {
    
//    public void prepare();
  
    /**
     * Commit the transaction.
     */
    public void commit();
    
    /**
     * Abort the transaction.
     */
    public void abort();

    /**
     * The object manager from which the transaction was started.
     * 
     * @return
     */
    public IOM getRootObjectManager();
    
}

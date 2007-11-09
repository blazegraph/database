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
 * Created on Oct 9, 2006
 */

package com.bigdata.istore;

/**
 * The identifier could not be resolved to data in the store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NotFoundException extends StoreException {

    /**
     * 
     */
    private static final long serialVersionUID = 5653932926338246370L;
    private final long id;
    
    /**
     * The persistent identifier that could not be resolved.
     * 
     * @return The persistent identifier.
     */
    public long getId() {return id;}
    
    /**
     * @param id The persistent identifier.
     */
    public NotFoundException(long id) {
        
        super();
        
        this.id = id;
        
    }

    /**
     * @param message
     * @param id The persistent identifier.
     */
    public NotFoundException(String message,long id) {
        
        super(message);
        
        this.id = id;
        
    }

    /**
     * @param cause
     * @param id The persistent identifier.
     */
    public NotFoundException(Throwable cause,long id) {
        
        super(cause);
        
        this.id = id;
        
    }

    /**
     * @param message
     * @param cause
     * @param id The persistent identifier.
     */
    public NotFoundException(String message, Throwable cause,long id) {
        
        super(message, cause);
        
        this.id = id;
        
    }

}

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
 * Created on Aug 19, 2010
 */

package com.bigdata.bop.ap;

import java.io.Serializable;

import com.bigdata.bop.IElement;

/**
 * An element for the test {@link R relation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
class E implements IElement, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    final String name;

    final String value;

    public E(final String name, final String value) {

        if (name == null)
            throw new IllegalArgumentException();

        if (value == null)
            throw new IllegalArgumentException();

        this.name = name;

        this.value = value;

    }

    public String toString() {
        
        return "E{name=" + name + ",value=" + value + "}";
        
    }
    
    public Object get(final int index) {
        switch (index) {
        case 0:
            return name;
        case 1:
            return value;
        }
        throw new IllegalArgumentException();
    }

}

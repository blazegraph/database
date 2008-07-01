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
package com.bigdata.relation.rule;

/**
 * A constant.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
final public class Constant<E> implements IConstant<E> {
    
    final private E value;
    
    final public boolean isVar() {
        
        return false;
        
    }

    final public boolean isConstant() {
        
        return true;
        
    }

    public Constant(E value) {
        
        if (value == null)
            throw new IllegalArgumentException();
        
        this.value = value;
        
    }
    
    public String toString() {
        
        return value.toString();
        
    }
    
    final public boolean equals(IVariableOrConstant<E> o) {
    
        if (o.isConstant() && value.equals(o.get())) {

            return true;
            
        }
        
        return false;
        
    }
    
    final public int hashCode() {
        
//        return (int) (id ^ (id >>> 32));
        return value.hashCode();
        
    }
    
//    public int compareTo(IVariableOrConstant arg0) {
//
//        // order vars before ids
//        if(arg0 instanceof Var) return 1;
//        
//        // @todo ordering only among constants of the same class?
//        Constant o = (Constant)arg0;
//        
//        /*
//         * Note: logic avoids possible overflow of [long] by not computing the
//         * difference between two longs.
//         */
//        
//        int ret = id < o.id ? -1 : id > o.id ? 1 : 0;
//        
//        return ret;
//        
//    }

    public E get() {
        
        return value;
        
    }

    public String getName() {
     
        throw new UnsupportedOperationException();
        
    }

}

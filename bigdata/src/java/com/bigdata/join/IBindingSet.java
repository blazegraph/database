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
 * Created on Jun 19, 2008
 */

package com.bigdata.join;


/**
 * Interface for a set of bindings. The set of named values is extensible and
 * the values are loosely typed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo use {@link Var} instead of name since we have cannonical references for
 *       variables?
 */
public interface IBindingSet {

    public boolean isBound(String name);
    
    public void set(String name,Object val);
    public void setLong(String name,long val);
    public void setInt(String name,int val);
    public void setDouble(String name,double val);
    public void setFloat(String name,float val);
    
    public Object get(String name);
    public long getLong(String name);
    public int getInt(String name);
    public double getDouble(String name);
    public float getFloat(String name);
    
    public void clear(String name);

    public void clearAll();
    
}

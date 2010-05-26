/*

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
 * Created on Jun 19, 2008
 */

package com.bigdata.relation.rule;


/**
 * Implementation of a binding.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 */
public class Binding implements IBinding {

    private static final long serialVersionUID = 1L;
    
    /**
     * The variable.
     */
    protected final IVariable var;
    
    /**
     * The value;
     */
    protected final IConstant val;
    
    
    /**
     * Construct a binding instance.
     */
    public Binding(final IVariable var, final IConstant val) {
        
        if (var == null) {
            throw new IllegalArgumentException();
        }
        
        if (val == null) {
            throw new IllegalArgumentException();
        }
        
        this.var = var;
        this.val = val;
        
    }
    
    /**
     * Get the variable.
     * 
     * @return the variable
     */
    public IVariable getVar() {
        
        return var;
        
    }
    
    /**
     * Get the value.
     * 
     * @return the value
     */
    public IConstant getVal() {
        
        return val;
        
    }
    
    /**
     * True iff the variable and its bound value is the same
     * for the two bindings.
     * 
     * @param o
     *            Another binding.
     */
    public boolean equals(IBinding o) {
        
        if (o == this) {
            return true;
        }
        
        if (o instanceof IBinding) {
            IBinding b = (IBinding) o;
            // variables ok for reference testing
            return var == b.getVar() && val.equals(b.getVal());
        }
        
        return false;
        
    }
    
    private int hashCode = 0;
    public int hashCode() {
        
        if (hashCode == 0) {
            hashCode = toString().hashCode();
        }
        
        return hashCode;
        
    }
    
    public String toString() {
        
        StringBuilder sb = new StringBuilder();
        sb.append(var.toString());
        sb.append('=');
        sb.append(val.toString());
        
        return sb.toString();
        
    }
    
}

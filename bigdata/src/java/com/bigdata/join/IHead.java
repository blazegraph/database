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
 * Created on Jun 24, 2008
 */

package com.bigdata.join;

import com.bigdata.journal.AbstractTask;


/**
 * Extends the interface for the {@link IPredicate} that is the head of a
 * {@link IRule} to include the action that should be applied to the computed
 * solutions for the {@link IRule}. The action is applied to the
 * {@link IRelation} associated with the head {@link IPredicate}.
 * 
 * FIXME All actions in fact write on a buffer, so the question is whether the
 * buffer is for an iterator to read from (a Select) or absorbing writes for
 * efficient ordered writes (insert, remove, or update).
 * <p>
 * Rather than symbolic actions, it may make sense to have a lamba expression
 * that gets executed that implements {@link IBuffer} , but I really want the
 * buffer to be shared across rules running in the same {@link AbstractTask}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IHead<E> extends IPredicate<E> {

    public static enum ActionEnum {
        
        Select,
        Insert,
        Remove,
        Update;
        
    }

    public ActionEnum getAction();
    
}

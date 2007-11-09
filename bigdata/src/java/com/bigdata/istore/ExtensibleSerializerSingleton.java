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
 * Created on Nov 4, 2005
 */
package com.bigdata.istore;

import org.CognitiveWeb.extser.AbstractSingleton;

import com.bigdata.journal.IJournal;

/**
 * <p>
 * Stateless singleton seralizer wrapping the semantics of the {@link
 * OMExtensibleSerializer} serializer. The use of this class prevents multiple
 * copies of the state of the extensible serializer from being written into the
 * store.
 * </p>
 * 
 * @author thompsonbry
 * @version $Id$
 */

public class ExtensibleSerializerSingleton
    extends AbstractSingleton
{

    private static final long serialVersionUID = -374435143615477216L;

    public OMExtensibleSerializer getSerializer( IJournal journal )
        throws IllegalStateException
    {
        
        return (OMExtensibleSerializer) super.getSerializer
            ( journal
              );
        
    }

    public void setSerializer( IJournal journal, OMExtensibleSerializer ser )
        throws IllegalStateException
    {

        super.setSerializer
            ( journal,
              ser
              );
        
    }

}

/**
Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
package com.bigdata.blueprints;

import com.bigdata.blueprints.BigdataGraphAtom.ElementType;



/**
 * Listener interface for a BigdataGraphEmbedded.
 * 
 * @author mikepersonick
 *
 */
public interface BigdataGraphListener {

    void graphEdited(BigdataGraphEdit edit, String raw);

    void transactionBegin();

    void transactionPrepare();

    void transactionCommited(long commitTime);

    void transactionAborted();
    
    public static class BigdataGraphEdit { 
        
        public static enum Action {
            
            Add, Remove;
            
        }

        private final Action action;

        private final BigdataGraphAtom atom;
        
//        private final long timestamp;
        
        public BigdataGraphEdit(final Action action, 
                final BigdataGraphAtom atom) {//, final long timestamp) {
            this.action = action;
            this.atom = atom;
//            this.timestamp = timestamp;
        }

        public Action getAction() {
            return action;
        }

        public BigdataGraphAtom getAtom() {
            return atom;
        }
        
//        public long getTimestamp() {
//            return timestamp;
//        }

        public String getId() {
            return atom.getId();
        }

        public ElementType getType() {
            return atom.getType();
        }

        public String getFromId() {
            return atom.getFromId();
        }

        public String getToId() {
            return atom.getToId();
        }

        public String getLabel() {
            return atom.getLabel();
        }

        public String getKey() {
            return atom.getKey();
        }

        public Object getVal() {
            return atom.getVal();
        }

        @Override
        public String toString() {
            return "BigdataGraphEdit [action=" + action + ", atom=" + atom
                    + "]";
        }

    }
    
}

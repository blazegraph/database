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
 * Created on Jul 2, 2008
 */

package com.bigdata.relation.rule;

/**
 * An {@link IStep} is either a single {@link IRule} or a complex
 * {@link IProgram}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IStep {

    /**
     * The name of the program.
     */
    public String getName();

    /**
     * <code>true</code> iff the step is an {@link IRule} and
     * <code>false</code> iff the step is an {@link IProgram}.
     */
    public boolean isRule();

    /**
     * A human readable representation of the {@link IStep}.
     */
    public String toString();

}
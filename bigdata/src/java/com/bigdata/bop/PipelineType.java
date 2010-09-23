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
 * Created on Sep 21, 2010
 */

package com.bigdata.bop;

/**
 * Return the type of pipelining supported by an operator.
 * <p>
 * Note: bigdata does not support tuple-at-a-time processing. Only vectored and
 * operator-at-a-time processing. Tuple at a time processing is generally very
 * inefficient.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum PipelineType {

    /**
     * Vectored operators stream chunks of intermediate results from one
     * operator to the next using producer / consumer pattern. Each time a set
     * of intermediate results is available for a vectored operator, it is
     * evaluated against those inputs producing another set of intermediate
     * results for its target operator(s). Vectored operators may be evaluated
     * many times during a given query and often have excellent parallelism due
     * to the concurrent evaluation of the different operators on different sets
     * of intermediate results.
     */
    Vectored,

    /**
     * The operator will run exactly once and must wait for all of its inputs to
     * be assembled before it runs.
     * <p>
     * There are some operations for which this is always true, such as SORT.
     * Other operations MAY use operator-at-once evaluation in order to benefit
     * from a combination of more efficient IO patterns and simpler design.
     * However, pipelined operators using large memory blocks have many of the
     * benefits of operator-at-once evaluation. By deferring their evaluation
     * until some minimum number of source data blocks are available, they may
     * be evaluated once or more than once, depending on the data scale.
     */
    OneShot;
    
}

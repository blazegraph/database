/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * An implementation of a Blueprints Graph that implements immortality using
 * the RDR specification.
 * 
 * Modeling vertices
 * <<:v1 rdf:type :Vertex>> :timestamp "t1" .
 * 
 * Modeling element properties
 * <<:v1 :p1 "val1">> :timestamp "t1" .
 * <<:v1 :p1 "val2">> :timestamp "t2" .
 * 
 * Modeling edges

 * Two distinct edges between :v and :v2 with the same edge label.  Totally
 * legal in Blueprints.
 * :v1 :knows :v2 .
 * <<:v1 :knows :v2>> :id :e1 .
 * <<:v2 :knows :v2>> :id :e2 .
 * 
 * Created at different times.
 * :e1 :timestamp "t1" .
 * :e2 :timestamp "t2" .
 * 
 * With different properties.
 * <<:e1 :p1 "foo">> :timestamp "t1" .
 * <<:e2 :p1 "bar">> :timestamp "t2" .
 * 
 * Modeling a deleted property:
 * 
 * <<:v1 :p1 :null>> :timestamp "t3" .
 * 
 * Multiple values for the same property with the same timestamp implies a list:
 * 
 * <<:v2 :p2 "v1">> :timestamp "t1" .
 * <<:v2 :p2 "v2">> :timestamp "t1" .
 * <<:v2 :p2 "v3">> :timestamp "t1" .
 * 
 * Pure append writes - no removal of old property values.
 * 
 * @author mikepersonick
 *
 */
public abstract class ImmortalGraph extends BigdataGraph {

    private static final transient Logger log = Logger.getLogger(ImmortalGraph.class);
    
    public ImmortalGraph(final BlueprintsValueFactory factory) {
        this(factory, new Properties());
    }
    
	public ImmortalGraph(final BlueprintsValueFactory factory,
	        final Properties props) {
	    super(factory, props);
	}
	
}

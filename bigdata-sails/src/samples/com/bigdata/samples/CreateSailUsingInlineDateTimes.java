/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Feb 17, 2011
 */

package com.bigdata.samples;

import java.util.Properties;

import com.bigdata.rdf.sail.BigdataSail;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CreateSailUsingInlineDateTimes extends SampleCode {

    static public void main(String[] args) {

        try {

            CreateSailUsingInlineDateTimes f = new CreateSailUsingInlineDateTimes();

            final String resource = "CreateSailUsingInlineDateTimes.properties";

            final Properties properties = f.loadProperties(resource);

            System.out.println("Read properties from resource: " + resource);
            properties.list(System.out);

            final BigdataSail sail = new BigdataSail(properties);

            sail.initialize();

            try {

                System.out.println("Sail is initialized.");

            } finally {

                sail.shutDown();

            }

        } catch (Throwable t) {

            t.printStackTrace(System.err);

        }

    }

}

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
 * Created on Jan 18, 2011
 */

package com.bigdata.bop.rdf.joinGraph;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.openrdf.sail.memory.model.MemValueFactory;

/**
 * A data set generator. {@link TestJoinGraphOnBarData} uses data sets generated
 * by this class.
 * 
 * @author <a href="mailto:mroycsi@users.sourceforge.net">Matt Roy</a>
 * @version $Id$
 */
public class GenerateBarData {
    
    private static final Random random = new Random(1);

    public static int getUniformInteger(double prob, int low, int high) {
        if (random.nextDouble() <= prob) {
            return low + random.nextInt(high - low);
        }
        return 0;
    }

    /*
     * 
     *  SELECT * WHERE {
        ?a <http://test/bar#beverageType> ?d.                        
        ?value <http://test/bar#orderItems> ?a.
        ?value <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://test/bar#order> .                            
        ?value <http://test/bar#employee> ?b.                         
        ?b <http://test/bar#employeeNum> ?f.
        ?a <http://test/bar#beverageType> """Beer"""^^<http://www.w3.org/2001/XMLSchema#string>.                  
     }
     */
    /**
     * @param args
     * @throws RDFHandlerException 
     */
    public static void main(String[] args) throws IOException, RDFHandlerException {
        if (args.length != 3) {
            System.err.println("usage: #orders #servers #maxItemsPerOrder");
            System.exit(1);
        }
        final int totalOrders = Integer.parseInt(args[0]);
        final int totalServers = Integer.parseInt(args[1]);
        final int maxItemsPerOrder = Integer.parseInt(args[2]);
        final File output = new File("./barData.trig");
        final FileWriter fw = new FileWriter(output);
        try {
            final RDFWriter writer = Rio.createWriter(RDFFormat.TRIG, fw);
            writer.startRDF();
            writer.handleNamespace("bar", "http://test/bar#");
            writer.handleNamespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns");
    
            final MemValueFactory factory = new MemValueFactory();
    
            final URI empType = factory.createURI("http://test/bar#Employee");
            final URI employeeNum = factory.createURI("http://test/bar#employeeNum");
            final URI employee = factory.createURI("http://test/bar#employee");
            final URI orderType = factory.createURI("http://test/bar#Order");
            final URI orderItemType = factory.createURI("http://test/bar#OrderItem");
            final URI orderItems = factory.createURI("http://test/bar#orderItems");
            final URI beverage = factory.createURI("http://test/bar#beverageType");
    
            final Literal drinks[] = new Literal[] { factory.createLiteral("Beer"), factory.createLiteral("Wine"), factory.createLiteral("Water"), factory.createLiteral("Soda"), factory.createLiteral("DietSoda"), factory.createLiteral("Juice") };
            final URI employees[] = new URI[totalServers];
            for (int s = 0; s < totalServers; s++) {
                employees[s] = factory.createURI("http://test/bar#employee_" + s);
                writer.handleStatement(factory.createStatement(employees[s], RDF.TYPE, empType, employees[s]));
                writer.handleStatement(factory.createStatement(employees[s], employeeNum, factory.createLiteral(s), employees[s]));
            }
            int totalItems = 0;
            for (int i = 0; i < totalOrders; i++) {
                final URI orderGraph = factory.createURI("http://test/bar#order_" + UUID.randomUUID().toString());
                writer.handleStatement(factory.createStatement(orderGraph, RDF.TYPE, orderType, orderGraph));
                writer.handleStatement(factory.createStatement(orderGraph, employee, employees[getUniformInteger(1.0, 0, totalServers)], orderGraph));
                final ArrayList<URI> items = new ArrayList<URI>();
                for (int k = 0; k < getUniformInteger(0.90, 1, maxItemsPerOrder); k++) {
                    totalItems++;
                    final URI orderItem = factory.createURI("http://test/bar#orderItem_" + UUID.randomUUID().toString());
                    items.add(orderItem);
                    writer.handleStatement(factory.createStatement(orderGraph, orderItems, orderItem, orderGraph));
                }
                for (URI orderItem : items) {
                    writer.handleStatement(factory.createStatement(orderItem, RDF.TYPE, orderItemType, orderGraph));
                    final Literal drink = drinks[getUniformInteger(1.0, 0, drinks.length)];
                    writer.handleStatement(factory.createStatement(orderItem, beverage, drink, orderGraph));
                }
            }
            System.err.println("totalItems=" + totalItems);
            writer.endRDF();
        } finally {
        fw.close();
        }
    }
}

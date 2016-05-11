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

package com.bigdata.rdf.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.openrdf.model.vocabulary.XMLSchema;

import com.bigdata.Banner;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;
import com.bigdata.rdf.lexicon.Id2TermTupleSerializer;
import com.bigdata.rdf.lexicon.LexiconKeyBuilder;
import com.bigdata.rdf.lexicon.Term2IdTupleSerializer;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;


/**
 * A utility class to update Term2ID indexes (replace xsd:string literals with plain literals).
 */


public class ConvertStringToPlainLiterals {

    /**
     * Utility method to update Term2ID indexes in a local journal (replace xsd:string literals with plain literals).
     * 
     * @param args
     *            <code>[-namespace <i>namespace</i>] [-forceCreate] propertyFile</code>
     *            where
     *            <dl>
     *            <dt>-namespace</dt>
     *            <dd>The namespace of the KB instance.</dd>
     *            <dt>propertyFile</dt>
     *            <dd>The configuration file for the database instance.</dd>
     *            </dl>
     */
	public static void main(final String[] args) throws IOException {
        
        Banner.banner();
        
        String namespace = null;
        
        int i = 0;
        
        while (i < args.length) {
            
            final String arg = args[i];
            
            if (arg.startsWith("-")) {
                
                if (arg.equals("-namespace")) {
                    
                    namespace = args[++i];
                    
                } else {
                    
                    System.err.println("Unknown argument: " + arg);
                    
                    usage();
                    
                }
                
            } else {
                
                break;
                
            }
            
            i++;
            
        }
        
        final int remaining = args.length - i;
        
        if (remaining < 1/*allow run w/o any named files or directories*/) {
            
            System.err.println("Not enough arguments.");
            
            usage();
            
        }
        
        final String propertyFileName = args[i++];
        
        final Properties properties = processProperties(propertyFileName);
        
        File journal = new File(properties.getProperty(Options.FILE));
        
        if (journal.exists()) {
            
            
            System.out.println("Journal: " + properties.getProperty(Options.FILE));
            
            Journal jnl = null;
            
            try {
                
                jnl = new Journal(properties);
                
                System.out.println("Update Term2ID index:");
                
                if (namespace == null) {
                
                    List<String> namespaces = jnl.getGlobalRowStore().getNamespaces(jnl.getLastCommitTime());
                    
                    for (String nm : namespaces) {
                        
                        final AbstractTripleStore kb = (AbstractTripleStore) jnl
                                .getResourceLocator().locate(nm, ITx.UNISOLATED);
                        
                        processNamespace(nm, kb);
                        
                    }
                
                } else {
                    
                        AbstractTripleStore kb = (AbstractTripleStore) jnl
                                .getResourceLocator().locate(namespace, ITx.UNISOLATED);
                        
                        if (kb != null) {
                            
                            processNamespace(namespace, kb);
                        
                        } else { 
                            
                            System.err.println("Namespace " + namespace + " does not exist");
                            
                        }
                        
                }
            
            } catch (Exception e) {
            	
            	e.printStackTrace();
            	
            } finally {
                
                jnl.close();
                
            }
            
        } else {
            
            System.err.println("Journal " + journal + " does not exist");
            
        }

        
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
	private static void processNamespace(String nm, final AbstractTripleStore kb) {
		final IIndex id2Term = kb.getLexiconRelation().getId2TermIndex();

		final IIndex term2Id = kb.getLexiconRelation().getTerm2IdIndex();

		if (id2Term != null) {
		
		    final Id2TermTupleSerializer id2TermSer = (Id2TermTupleSerializer) id2Term
		            .getIndexMetadata().getTupleSerializer();

		    final Term2IdTupleSerializer term2IdSer = (Term2IdTupleSerializer) term2Id
		            .getIndexMetadata().getTupleSerializer();

			final LexiconKeyBuilder term2IdKeyBuilder = term2IdSer.getLexiconKeyBuilder();
		    
		    final Iterator<BigdataValue> id2TermIterator = new Striterator(
		    		id2Term
		                    .rangeIterator(
		                            null/* fromKey */,
		                            null/* toKey */,
		                            0/* capacity */,
		                            IRangeQuery.DEFAULT | IRangeQuery.CURSOR,
		                            // prefix filter.
		                            null))
		            .addFilter(new Resolver() {

		                private static final long serialVersionUID = 1L;

		                /**
		                 * Decode the value, which is the term identifier.
		                 */
		                @Override
		                protected BigdataValue resolve(final Object arg0) {

		                    return id2TermSer.deserialize((ITuple)arg0);

		                }
		            });
     	
		    while (id2TermIterator.hasNext()) {
     	
		        final BigdataValue value = id2TermIterator.next();
				final byte[] plainLiteralKey = term2IdSer.serializeKey(value);
		        
		        if (value instanceof BigdataLiteral && XMLSchema.STRING.equals(((BigdataLiteral)value).getDatatype())) {
		        
		        	// Construct datatyped literal key (could not use serializeKey method as it will concert string literal to plain representation)
		        	byte[] key = term2IdKeyBuilder.datatypeLiteral2key(((BigdataLiteral)value).getDatatype(), value.stringValue());
					
		        	// Look for corresponding term2Id entry
		        	byte[] term = term2Id.lookup(key);
		        	
		        	// if there is a string-datatyped mapping but no plain literal mapping yet
					if (term != null && term2Id.lookup(plainLiteralKey) == null) {
						
						// Remove string-datatyped key
						term2Id.remove(term);
						
						// Insert plain literal key mapping to the same IV
						term2Id.insert(plainLiteralKey, term2IdSer.serializeVal(value.getIV()));

//						System.out.println("Processed Term2ID value: " + value + " "+term);
					}
					

		        }
		        
		    }
		    System.out.println(nm + " - completed");
		    
		
		} else {
		    
		    System.out.println(nm + " -  no Term2ID index");
		    
		}
	}
    
    public static Properties processProperties(final String propertyFileName) throws IOException {
        
        final File propertyFile = new File(propertyFileName);
        
        if (!propertyFile.exists()) {
            
            throw new FileNotFoundException(propertyFile.toString());
            
        }
        
        final Properties properties = new Properties();
        
        final InputStream is = new FileInputStream(propertyFile);
        try {
            properties.load(is);
        } finally {
            if (is != null) {
                is.close();
            }
        }
        
        return properties;
   }
    
    private static void usage() {
        
        System.err.println("usage: [-namespace namespace] propertyFile");
        
        System.exit(1);
        
        
    }
    
}

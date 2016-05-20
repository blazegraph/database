package com.bigdata.rdf.store;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;
import org.openrdf.model.vocabulary.XMLSchema;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.lexicon.ITermIndexCodes;
import com.bigdata.rdf.lexicon.Id2TermTupleSerializer;
import com.bigdata.rdf.model.BigdataValue;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;


public class ConvertStringToPlainLiteralsTest extends TestCase {
	
	public ConvertStringToPlainLiteralsTest() {
		
	}

	public ConvertStringToPlainLiteralsTest(String name) {
		super(name);
	}
	
	private final String JOURNALS_FOLDER = "target/testJournals/";
	
	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void test() throws IOException {
		
		final File journalsFolder = new File(JOURNALS_FOLDER);

		final String[] files = journalsFolder.list(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".properties");
			}
		});
		
		for(final String propertyFile : files ) {
			
			final String jnlPath = JOURNALS_FOLDER + propertyFile;
			
			final String[] args = {jnlPath};
			
			ConvertStringToPlainLiterals.main(args);
			
			final Journal jnl = new Journal(ConvertStringToPlainLiterals.processProperties(jnlPath));
			
			final List<String> namespaces = jnl.getGlobalRowStore().getNamespaces(jnl.getLastCommitTime());
            
            for (final String nm : namespaces) {
                
                final AbstractTripleStore kb = (AbstractTripleStore) jnl
                        .getResourceLocator().locate(nm, ITx.UNISOLATED);

                final IIndex id2Term = kb.getLexiconRelation().getId2TermIndex();

        		if (id2Term != null) {
        		
        		    final Id2TermTupleSerializer id2TermSer = (Id2TermTupleSerializer) id2Term
        		            .getIndexMetadata().getTupleSerializer();

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

        		                	final DataInputBuffer in = ((ITuple)arg0).getValueStream();
        		                	
        		                	BigdataValue value = null;
        		                	
									try {
										
										final byte version = in.readByte();
										
	        		                	final byte type = in.readByte();
	        		                	
	        		                	value = id2TermSer.deserialize((ITuple)arg0);
	        		                	
	        		                	if (type == ITermIndexCodes.TERM_CODE_DTL) {

	        		                		final String datatype = in.readUTF();

	        		                		assertNotEquals("String datatyped literal found in Id2Term index in " + //
	        		                						propertyFile.replace("properties", "jnl") + " literal\"" + value.stringValue() + "\"", //
	        		                						XMLSchema.STRING.stringValue(), datatype);
	        		                		
	        		                	}
									} catch (IOException e) {
										
										throw new RuntimeException(e);
										
									}
									
									return value;

        		                }
        		            });
        		    
        		    while (id2TermIterator.hasNext()) {
             	
        		        id2TermIterator.next();

        		    }
        		    
        		}
        		
            }
            
		}
		
		for(final File file : journalsFolder.listFiles()) {
			
			file.delete();
			
		}
		
		journalsFolder.delete();
		
	}
	
}

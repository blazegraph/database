package com.bigdata.perf.chem2bio2rdf;

import java.io.FileReader;
import java.io.Reader;
import java.util.Properties;

import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;

import com.bigdata.journal.Journal;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.webapp.client.IPreparedGraphQuery;

public class TestQuery2 {

	private static String query =
"PREFIX c2b2r_chembl: <http://chem2bio2rdf.org/chembl/resource/> " +
"PREFIX drugbank: <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/> " +

"SELECT ?alogp ?hha ?hhd ?molformula ?molweight ?mw_freebase ?num_ro5_violations ?psa ?rtb " +
"?affectedOrganism ?biotransformation ?description ?indication ?meltingPoint ?proteinBinding ?toxicity " +

"WHERE {  " +

"GRAPH <file:///home/OPS/develop/openphacts/datasets/chem2bio2rdf/chembl.nt> { " +
"OPTIONAL { <http://chem2bio2rdf.org/chembl/resource/chembl_compounds/276734> c2b2r_chembl:alogp ?alogp } " +
"OPTIONAL { <http://chem2bio2rdf.org/chembl/resource/chembl_compounds/276734> c2b2r_chembl:hha ?hha } " +
"OPTIONAL { <http://chem2bio2rdf.org/chembl/resource/chembl_compounds/276734> c2b2r_chembl:hhd ?hhd } " +
"OPTIONAL { <http://chem2bio2rdf.org/chembl/resource/chembl_compounds/276734> c2b2r_chembl:molformula ?molformula } " +
"OPTIONAL { <http://chem2bio2rdf.org/chembl/resource/chembl_compounds/276734> c2b2r_chembl:molweight ?molweight } " +
"OPTIONAL { <http://chem2bio2rdf.org/chembl/resource/chembl_compounds/276734> c2b2r_chembl:mw_freebase ?mw_freebase } " +
"OPTIONAL { <http://chem2bio2rdf.org/chembl/resource/chembl_compounds/276734> c2b2r_chembl:num_ro5_violations ?num_ro5_violations } " +
"OPTIONAL { <http://chem2bio2rdf.org/chembl/resource/chembl_compounds/276734> c2b2r_chembl:psa ?psa } " +
"OPTIONAL { <http://chem2bio2rdf.org/chembl/resource/chembl_compounds/276734> c2b2r_chembl:rtb ?rtb }  " +
"} " +

"GRAPH <http://linkedlifedata.com/resource/drugbank> { " +
"OPTIONAL {<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00398> drugbank:affectedOrganism ?affectedOrganism } " +
"OPTIONAL {<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00398> drugbank:biotransformation ?biotransformation } " +
"OPTIONAL {<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00398> drugbank:description ?description } " +
"} " +

"OPTIONAL {<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00398> drugbank:indication ?indication } " +
"OPTIONAL {<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00398> drugbank:proteinBinding ?proteinBinding } " +
"OPTIONAL {<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00398> drugbank:toxicity ?toxicity } " +
"OPTIONAL {<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00398> drugbank:meltingPoint ?meltingPoint} " +

"}";
	
	private static String query2 =
"PREFIX c2b2r_chembl: <http://chem2bio2rdf.org/chembl/resource/> " +
"PREFIX drugbank: <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/> " +

"describe <http://chem2bio2rdf.org/chembl/resource/chembl_compounds/276734> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00398> ";
			
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		final String journalProps = "/Users/mikepersonick/Documents/workspace/bigdata-release-1.2/bigdata-perf/chem2bio2rdf/RWStore.properties";
		
		final String journal = "/Users/mikepersonick/Documents/nobackup/chem2bio2rdf/bigdata.RW.journal";
		
		final Reader reader = new FileReader(journalProps);

		final Properties props = new Properties();

		props.load(reader);

		reader.close();
		
		props.setProperty(Journal.Options.FILE, journal); 

		final BigdataSail sail = new BigdataSail(props);
		final BigdataSailRepository repo = new BigdataSailRepository(sail);
		repo.initialize();
		
		BigdataSailRepositoryConnection cxn = null;
		try {
			
			cxn = repo.getReadOnlyConnection();
			
			System.err.println(sail.getDatabase().predicateUsage().toString());
			
			long total = 0;
			
			for (int i = 0; i < 10; i++) {
	
				final long start = System.currentTimeMillis();
				
				final TupleQuery tq = cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
				
				final TupleQueryResult result = tq.evaluate();
				
				int j = 0;
				while (result.hasNext()) {
					j++;
					result.next();
				}
				
				result.close();
				
				final long duration = System.currentTimeMillis() - start;
				
				total += duration;
				
				System.err.println("run " + i + ", " + duration + " millis, " + j + " results");
				
			}
			
			final long average = total / 10;
			
			System.err.println("average time: " + average + " millis");

		} catch (Exception ex) {
			
			ex.printStackTrace();
			
		} finally {
		
			if (cxn != null)
				cxn.close();
			
			repo.shutDown();
			
		}
		
		System.exit(0);
			
	}

}

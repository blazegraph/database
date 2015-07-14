package com.bigdata.perf.chem2bio2rdf;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.TupleQueryResult;

import com.bigdata.BigdataStatics;
import com.bigdata.util.httpd.Config;
import com.bigdata.rdf.sail.webapp.client.DefaultClientConnectionManagerFactory;
import com.bigdata.rdf.sail.webapp.client.IPreparedGraphQuery;
import com.bigdata.rdf.sail.webapp.client.IPreparedTupleQuery;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;

public class TestQuery {

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
		
        final String serviceURL = "http://localhost:" + Config.BLAZEGRAPH_HTTP_PORT
                + BigdataStatics.getContextPath() + "/sparql";

		final HttpClient httpClient = 
			new DefaultHttpClient(DefaultClientConnectionManagerFactory.getInstance().newInstance());
		
		final Executor executor = Executors.newCachedThreadPool();
		
		final RemoteRepository repo = new RemoteRepository(serviceURL, httpClient, executor);

		long total = 0;
		
		for (int i = 0; i < 10; i++) {

			final long start = System.currentTimeMillis();
			
//			final IPreparedTupleQuery tq = repo.prepareTupleQuery(query2);
//			
//			final TupleQueryResult result = tq.evaluate();
			
			final IPreparedGraphQuery gq = repo.prepareGraphQuery(query2);
			
			final GraphQueryResult result = gq.evaluate();
			
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
			
		httpClient.getConnectionManager().shutdown();

		System.exit(0);
			
	}

}

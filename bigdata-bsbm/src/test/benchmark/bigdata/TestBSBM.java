package benchmark.bigdata;

import com.bigdata.rdf.sail.bench.NanoSparqlClient;

public class TestBSBM {
    
    private static final String serviceURL = "http://localhost:8080";
    
    private static final String queryStr =
/*        
        "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> " +
        "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
        "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
        "SELECT DISTINCT ?product " +
        "WHERE { " +
        "    { " +
        "       ?product rdfs:label ?label . " +
        "       ?product rdf:type <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType75> . " +
        "       ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature379> . " +
        "       ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature2248> . " +
        "       ?product bsbm:productPropertyTextual1 ?propertyTextual . " +
        "       ?product bsbm:productPropertyNumeric1 ?p1 . " +
        "       FILTER ( ?p1 > 160 ) " +
        "    } UNION { " +
        "       ?product rdfs:label ?label . " +
        "       ?product rdf:type <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType75> . " +
        "       ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature379> . " +
        "       ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature2251> . " +
        "       ?product bsbm:productPropertyTextual1 ?propertyTextual . " +
        "       ?product bsbm:productPropertyNumeric2 ?p2 . " +
        "       FILTER ( ?p2 > 461 ) " +
        "    } " +
//        "       FILTER ( ?p1 > 160 || ?p2> 461 ) " +
        "} "
//        "ORDER BY ?label " +
//        "OFFSET 5 " +
//        "LIMIT 10"
        ;
*/    
        "construct { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer24/Product1131> ?p ?o . } " +
        "where { " +
        "  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer24/Product1131> ?p ?o . " +
        "}";
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        
        try {

            args = new String[] {
                "-query",
                queryStr,
                serviceURL
            };
            
            NanoSparqlClient.main(args);
            
        } catch (Exception ex) {
            
            ex.printStackTrace();
            
        }
        
    }
}

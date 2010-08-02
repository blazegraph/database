package benchmark.serializer;

import java.util.GregorianCalendar;
import java.util.Iterator;
import benchmark.model.*;
import benchmark.vocabulary.*;
import benchmark.generator.*;

import java.io.*;

public class Turtle implements Serializer {
	private FileWriter dataFileWriter;
	private File dataFile;
	private FileWriter prefixFileWriter;
	private boolean forwardChaining;
	private long nrTriples;
	
	public Turtle(String file, boolean forwardChaining)
	{
		try{
			this.prefixFileWriter = new FileWriter(file);
			
			this.dataFile = File.createTempFile("BSBM", ".data");
			this.dataFile.deleteOnExit();
			this.dataFileWriter = new FileWriter(dataFile);
		} catch(IOException e){
			System.err.println("Could not open File for writing.");
			System.err.println(e.getMessage());
			System.exit(-1);
		}
		
		try {
			prefixFileWriter.append(getNamespaces());
		} catch(IOException e) {
			System.err.println(e.getMessage());
		}
		
		this.forwardChaining = forwardChaining;
		nrTriples = 0l;
		
		TurtleShutdown sd = new TurtleShutdown(this);
		Runtime.getRuntime().addShutdownHook(sd);
	}
	
	/*
	 * Create Namespace prefixes
	 * @see benchmark.serializer.Serializer#gatherData(benchmark.serializer.ObjectBundle)
	 */
	private String getNamespaces() {
		StringBuffer result = new StringBuffer();
		result.append(createPrefixLine(RDF.PREFIX, RDF.NS));
		result.append(createPrefixLine(RDFS.PREFIX, RDFS.NS));
		result.append(createPrefixLine(FOAF.PREFIX, FOAF.NS));
		result.append(createPrefixLine(DC.PREFIX, DC.NS));
		result.append(createPrefixLine(XSD.PREFIX, XSD.NS));
		result.append(createPrefixLine(REV.PREFIX, REV.NS));
		result.append(createPrefixLine(BSBM.PREFIX, BSBM.NS));
		result.append(createPrefixLine(BSBM.INST_PREFIX, BSBM.INST_NS));
		
		return result.toString();
	}
	
	private String createPrefixLine(String prefix, String namespace) {
		StringBuffer result = new StringBuffer();
		result.append("@prefix ");
		result.append(prefix);
		result.append(" ");
		result.append(createURIref(namespace));
		result.append(" .\n");
		
		return result.toString();
	}
	

	public void gatherData(ObjectBundle bundle) {
		Iterator<BSBMResource> it = bundle.iterator();

		try {
			String prefix = getPrefixDefinition(bundle);
			if(prefix!=null)
				prefixFileWriter.append(prefix);
			
			while(it.hasNext())
			{
				BSBMResource obj = it.next();
	
				if(obj instanceof ProductType){
					dataFileWriter.append(convertProductType((ProductType)obj));
				}
				else if(obj instanceof Offer){
					dataFileWriter.append(convertOffer((Offer)obj));
				}
				else if(obj instanceof Product){
					dataFileWriter.append(convertProduct((Product)obj));
				}
				else if(obj instanceof Person){
					dataFileWriter.append(convertPerson((Person)obj, bundle));
				}
				else if(obj instanceof Producer){
					dataFileWriter.append(convertProducer((Producer)obj));
				}
				else if(obj instanceof ProductFeature){
					dataFileWriter.append(convertProductFeature((ProductFeature)obj));
				}
				else if(obj instanceof Vendor){
					dataFileWriter.append(convertVendor((Vendor)obj));
				}
				else if(obj instanceof Review){
					dataFileWriter.append(convertReview((Review)obj, bundle));
				}
			}
			
		}catch(IOException e){
			System.err.println("Could not write into File!");
			System.err.println(e.getMessage());
			System.exit(-1);
		}
	}
	
	/*
	 * Generate Prefix-String
	 */
	private String getPrefixDefinition(ObjectBundle bundle) {
		StringBuffer prefix = new StringBuffer();
		prefix.append("@prefix ");
		String publisher = bundle.getPublisher().toLowerCase();
		
		if(publisher.contains("datafromvendor")) {
			prefix.append("dataFromVendor");
			prefix.append(bundle.getPublisherNum());
			prefix.append(": <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor");
			prefix.append(bundle.getPublisherNum());
			prefix.append("/> .\n");
		}
		else if(publisher.contains("datafromratingsite")) {
			prefix.append("dataFromRatingSite");
			prefix.append(bundle.getPublisherNum());
			prefix.append(": <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite");
			prefix.append(bundle.getPublisherNum());
			prefix.append("/> .\n");
		}
		else if(publisher.contains("datafromproducer")) {
			prefix.append("dataFromProducer");
			prefix.append(bundle.getPublisherNum());
			prefix.append(": <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer");
			prefix.append(bundle.getPublisherNum());
			prefix.append("/> .\n");
		}
		else
			return null;
		
		return prefix.toString();
	}

	/*
	 * Converts the ProductType Object into an TriG String
	 * representation.
	 */
	private String convertProductType(ProductType pType)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject

		result.append(pType.getPrefixed()); 
		result.append("\n");

		//rdf:type
		result.append(createTriplePO(
						RDF.prefixed("type"),
						BSBM.prefixed("ProductType")));
		
		//rdfs:label
		result.append(createTriplePO(
				RDFS.prefixed("label"),
				createLiteral(pType.getLabel())));
		
		
		
		//rdfs:subClassOf
		if(pType.getParent()!=null)
		{
			String parentURIREF = BSBM.INST_PREFIX + "ProductType" + pType.getParent().getNr();
			result.append(createTriplePO(
					RDFS.prefixed("subClassOf"),
					parentURIREF));
		}
		
		//rdfs:comment
		result.append(createTriplePO(
				RDFS.prefixed("comment"),
				createLiteral(pType.getComment())));

		//dc:publisher
		result.append(createTriplePO(
				DC.prefixed("publisher"),
				createURIref(BSBM.getStandardizationInstitution(pType.getPublisher()))));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(pType.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(createTriplePOEnd(
				DC.prefixed("date"),
				createDataTypeLiteral(dateString, XSD.prefixed("date"))));
		
		return result.toString();
	}
	
	/*
	 * Converts the Offer Object into an TriG String
	 * representation.
	 */
	private String convertOffer(Offer offer)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		
		result.append(offer.getPrefixed());
		result.append("\n");

		//rdf:type
		result.append(createTriplePO(
				RDF.prefixed("type"),
				BSBM.prefixed("Offer")));
		
		//bsbm:product
		int productNr = offer.getProduct();
		int producerNr = Generator.getProducerOfProduct(productNr); 
		result.append(createTriplePO(
				BSBM.prefixed("product"),
				Product.getPrefixed(productNr, producerNr)));
		
		//bsbm:vendor
		result.append(createTriplePO(
				BSBM.prefixed("vendor"),
				Vendor.getPrefixed(offer.getVendor())));
		
		//bsbm:price
		result.append(createTriplePO(
				BSBM.prefixed("price"),
				createDataTypeLiteral(offer.getPriceString(),BSBM.prefixed("USD"))));
		
		//bsbm:validFrom
		GregorianCalendar validFrom = new GregorianCalendar();
		validFrom.setTimeInMillis(offer.getValidFrom());
		String validFromString = DateGenerator.formatDateTime(validFrom);
		result.append(createTriplePO(
				BSBM.prefixed("validFrom"),
				createDataTypeLiteral(validFromString, XSD.prefixed("dateTime"))));
		
		//bsbm:validTo
		GregorianCalendar validTo = new GregorianCalendar();
		validTo.setTimeInMillis(offer.getValidTo());
		String validToString = DateGenerator.formatDateTime(validTo);
		result.append(createTriplePO(
				BSBM.prefixed("validTo"),
				createDataTypeLiteral(validToString, XSD.prefixed("dateTime"))));
		
		//bsbm:deliveryDays
		result.append(createTriplePO(
				BSBM.prefixed("deliveryDays"),
				createDataTypeLiteral(offer.getDeliveryDays().toString(), XSD.prefixed("integer"))));
		
		//bsbm:offerWebpage
		result.append(createTriplePO(
				BSBM.prefixed("offerWebpage"),
				createURIref(offer.getOfferWebpage())));
		
		//dc:publisher
		result.append(createTriplePO(
				DC.prefixed("publisher"),
				Vendor.getPrefixed(offer.getPublisher())));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(offer.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(createTriplePOEnd(
				DC.prefixed("date"),
				createDataTypeLiteral(dateString, XSD.prefixed("date"))));
		
		return result.toString();
	}
	
	/*
	 * Converts the Product Object into an TriG String
	 * representation.
	 */
	private String convertProduct(Product product)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		
		result.append(Product.getPrefixed(product.getNr(), product.getProducer()));
		result.append("\n");

		//rdf:type
		result.append(createTriplePO(
						RDF.prefixed("type"),
						BSBM.prefixed("Product")));
		
		//rdfs:label
		result.append(createTriplePO(
				RDFS.prefixed("label"),
				createLiteral(product.getLabel())));
		
		//rdfs:comment
		result.append(createTriplePO(
				RDFS.prefixed("comment"),
				createLiteral(product.getComment())));
		
		//bsbm:productType
		if(forwardChaining) {
			ProductType pt = product.getProductType();
			while(pt!=null) {
				result.append(createTriplePO(
						RDF.prefixed("type"),
						pt.getPrefixed()));
				pt = pt.getParent();
			}
		}
		else {
			result.append(createTriplePO(
					RDF.prefixed("type"),
					product.getProductType().getPrefixed()));
		}
		
		//bsbm:productPropertyNumeric
		Integer[] ppn = product.getProductPropertyNumeric();
		for(int i=0,j=1;i<ppn.length;i++,j++)
		{
			Integer value = ppn[i];
			if(value!=null)
				result.append(createTriplePO(
						BSBM.getProductPropertyNumericPrefix(j),
						createDataTypeLiteral(value.toString(), XSD.prefixed("integer"))));
		}

		//bsbm:productPropertyTextual
		String[] ppt = product.getProductPropertyTextual();
		for(int i=0,j=1;i<ppt.length;i++,j++)
		{
			String value = ppt[i];
			if(value!=null)
				result.append(createTriplePO(
						BSBM.getProductPropertyTextualPrefix(j),
						createDataTypeLiteral(value, XSD.prefixed("string"))));
		}
		
		//bsbm:productFeature
		Iterator<Integer> pf = product.getFeatures().iterator();
		while(pf.hasNext())
		{
			Integer value = pf.next();
			result.append(createTriplePO(
					BSBM.prefixed("productFeature"),
					ProductFeature.getPrefixed(value)));
		}
		
		//bsbm:producer
		result.append(createTriplePO(
				BSBM.prefixed("producer"),
				Producer.getPrefixed(product.getProducer())));
		
		//dc:publisher
		result.append(createTriplePO(
				DC.prefixed("publisher"),
				Producer.getPrefixed(product.getPublisher())));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(product.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(createTriplePOEnd(
				DC.prefixed("date"),
				createDataTypeLiteral(dateString, XSD.prefixed("date"))));
		
		return result.toString();
	}
	
	/*
	 * Converts the Person Object into an TriG String
	 * representation.
	 */
	private String convertPerson(Person person, ObjectBundle bundle)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		
		result.append(Person.getPrefixed(person.getNr(), bundle.getPublisherNum()));
		result.append("\n");

		//rdf:type
		result.append(createTriplePO(
						RDF.prefixed("type"),
						FOAF.prefixed("Person")));
		
		//foaf:name
		result.append(createTriplePO(
				FOAF.prefixed("name"),
				createLiteral(person.getName())));
		
		//foaf:mbox_sha1sum
		result.append(createTriplePO(
				FOAF.prefixed("mbox_sha1sum"),
				createLiteral(person.getMbox_sha1sum())));
		
		//bsbm:country
		result.append(createTriplePO(
				BSBM.prefixed("country"),
				createURIref(ISO3166.find(person.getCountryCode()))));
		
		//dc:publisher
		result.append(createTriplePO(
				DC.prefixed("publisher"),
				RatingSite.getPrefixed(person.getPublisher())));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(person.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(createTriplePOEnd(
				DC.prefixed("date"),
				createDataTypeLiteral(dateString, XSD.prefixed("date"))));
		
		return result.toString();
	}
	
	/*
	 * Converts the Producer Object into an TriG String
	 * representation.
	 */
	private String convertProducer(Producer producer)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		
		result.append(Producer.getPrefixed(producer.getNr()));
		result.append("\n");

		//rdf:type
		result.append(createTriplePO(
						RDF.prefixed("type"),
						BSBM.prefixed("Producer")));
		
		//rdfs:label
		result.append(createTriplePO(
				RDFS.prefixed("label"),
				createLiteral(producer.getLabel())));
		
		//rdfs:comment
		result.append(createTriplePO(
				RDFS.prefixed("comment"),
				createLiteral(producer.getComment())));

		//foaf:homepage
		result.append(createTriplePO(
				FOAF.prefixed("homepage"),
				createURIref(producer.getHomepage())));
		
		//bsbm:country
		result.append(createTriplePO(
				BSBM.prefixed("country"),
				createURIref(ISO3166.find(producer.getCountryCode()))));
		
		//dc:publisher
		result.append(createTriplePO(
				DC.prefixed("publisher"),
				Producer.getPrefixed(producer.getPublisher())));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(producer.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(createTriplePOEnd(
				DC.prefixed("date"),
				createDataTypeLiteral(dateString, XSD.prefixed("date"))));
		
		return result.toString();
	}
	
	/*
	 * Converts the ProductFeature Object into an TriG String
	 * representation.
	 */
	private String convertProductFeature(ProductFeature pf)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		
		result.append(ProductFeature.getPrefixed(pf.getNr()));
		result.append("\n");
		
		//rdf:type
		result.append(createTriplePO(
						RDF.prefixed("type"),
						BSBM.prefixed("ProductFeature")));
		
		//rdfs:label
		result.append(createTriplePO(
				RDFS.prefixed("label"),
				createLiteral(pf.getLabel())));
		
		//rdfs:comment
		result.append(createTriplePO(
				RDFS.prefixed("comment"),
				createLiteral(pf.getComment())));
		
		//dc:publisher
		result.append(createTriplePO(
				DC.prefixed("publisher"),
				createURIref(BSBM.getStandardizationInstitution(pf.getPublisher()))));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(pf.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(createTriplePOEnd(
				DC.prefixed("date"),
				createDataTypeLiteral(dateString, XSD.prefixed("date"))));

		return result.toString();
	}
	
	/*
	 * Converts the Vendor Object into an TriG String
	 * representation.
	 */
	private String convertVendor(Vendor vendor)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		
		result.append(Vendor.getPrefixed(vendor.getNr()));
		result.append("\n");

		//rdf:type
		result.append(createTriplePO(
						RDF.prefixed("type"),
						BSBM.prefixed("Vendor")));
		
		//rdfs:label
		result.append(createTriplePO(
				RDFS.prefixed("label"),
				createLiteral(vendor.getLabel())));
		
		//rdfs:comment
		result.append(createTriplePO(
				RDFS.prefixed("comment"),
				createLiteral(vendor.getComment())));

		//foaf:homepage
		result.append(createTriplePO(
				FOAF.prefixed("homepage"),
				createURIref(vendor.getHomepage())));
		
		//bsbm:country
		result.append(createTriplePO(
				BSBM.prefixed("country"),
				createURIref(ISO3166.find(vendor.getCountryCode()))));	
		
		//dc:publisher
		result.append(createTriplePO(
				DC.prefixed("publisher"),
				Vendor.getPrefixed(vendor.getPublisher())));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(vendor.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(createTriplePOEnd(
				DC.prefixed("date"),
				createDataTypeLiteral(dateString, XSD.prefixed("date"))));
		
		return result.toString();
	}
	
	
	/*
	 * Converts the Review Object into an TriG String
	 * representation.
	 */
	private String convertReview(Review review, ObjectBundle bundle)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		
		result.append(Review.getPrefixed(review.getNr(), bundle.getPublisherNum()));
		result.append("\n");

		//rdf:type
		result.append(createTriplePO(
						RDF.prefixed("type"),
						BSBM.prefixed("Review")));

		//bsbm:reviewFor
		result.append(createTriplePO(
				BSBM.prefixed("reviewFor"),
				Product.getPrefixed(review.getProduct(), review.getProducerOfProduct())));
		
		//rev:reviewer
		result.append(createTriplePO(
				REV.prefixed("reviewer"),
				Person.getPrefixed(review.getPerson(), review.getPublisher())));
		
		//dc:title
		result.append(createTriplePO(
				DC.prefixed("title"),
				createLiteral(review.getTitle())));
		
		//rev:text
		result.append(createTriplePO(
				REV.prefixed("text"),
				createLanguageLiteral(review.getText(),ISO3166.language[review.getLanguage()])));
		
		//bsbm:ratingX
		Integer[] ratings = review.getRatings();
		for(int i=0,j=1;i<ratings.length;i++,j++)
		{
			Integer value = ratings[i];
			if(value!=null)
				result.append(createTriplePO(
						BSBM.getRatingPrefix(j),
						createDataTypeLiteral(value.toString(), XSD.prefixed("integer"))));
		}
		
		//bsbm:reviewDate
		GregorianCalendar reviewDate = new GregorianCalendar();
		reviewDate.setTimeInMillis(review.getReviewDate());
		String reviewDateString = DateGenerator.formatDateTime(reviewDate);
		result.append(createTriplePO(
				BSBM.prefixed("reviewDate"),
				createDataTypeLiteral(reviewDateString, XSD.prefixed("dateTime"))));
		
		//dc:publisher
		result.append(createTriplePO(
				DC.prefixed("publisher"),
				RatingSite.getPrefixed(review.getPublisher())));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(review.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(createTriplePOEnd(
				DC.prefixed("date"),
				createDataTypeLiteral(dateString, XSD.prefixed("date"))));
		
		return result.toString();
	}
	

	
	//Create Literal
	private String createLiteral(String value)
	{
		StringBuffer result = new StringBuffer();
		result.append("\"");
		result.append(value);
		result.append("\"");
		return result.toString();
	}
	
	//Create typed literal
	private String createDataTypeLiteral(String value, String datatypeURI)
	{
		StringBuffer result = new StringBuffer();
		result.append("\"");
		result.append(value);
		result.append("\"^^");
		result.append(datatypeURI);
		return result.toString();
	}
	
	//Create language tagged literal
	private String createLanguageLiteral(String text, String languageCode)
	{
		StringBuffer result = new StringBuffer();
		result.append("\"");
		result.append(text);
		result.append("\"@");
		result.append(languageCode);
		return result.toString();
	}

	
	/*
	 * Create an abbreviated triple consisting of predicate and object; end with ";"
	 */
	private String createTriplePO(String predicate, String object)
	{
		StringBuffer result = new StringBuffer();
		result.append("    ");
		result.append(predicate);
		result.append(" ");
		result.append(object);
		result.append(" ;\n");
		
		nrTriples++;
		
		return result.toString();
	}
	
	/*
	 * Create an abbreviated triple consisting of predicate and object; end with "."
	 */
	private String createTriplePOEnd(String predicate, String object)
	{
		StringBuffer result = new StringBuffer();
		result.append("    ");
		result.append(predicate);
		result.append(" ");
		result.append(object);
		result.append(" .\n");
		
		nrTriples++;
		
		return result.toString();
	}
	
	
	
//	//Create URIREF from namespace and element
//	private String createURIref(String namespace, String element)
//	{
//		StringBuffer result = new StringBuffer();
//		result.append("<");
//		result.append(namespace);
//		result.append(element);
//		result.append(">");
//		return result.toString();
//	}
	
	//Create URIREF from URI
	private String createURIref(String uri)
	{
		StringBuffer result = new StringBuffer();
		result.append("<");
		result.append(uri);
		result.append(">");
		return result.toString();
	}
	
	

	public void serialize() {
		//Close files
		try {
			dataFileWriter.flush();
			dataFileWriter.close();
			
			prefixFileWriter.append("\n");
			
			FileReader data = new FileReader(dataFile);
			char[] buf = new char[100];
			int len = 0;
			while((len=data.read(buf))!=-1) {
				prefixFileWriter.write(buf, 0, len);
			}
			
			prefixFileWriter.flush();
			prefixFileWriter.close();
			data.close();
			
			dataFile.delete();
		} catch(IOException e) {
			System.err.println(e.getMessage());
			System.exit(-1);
		}
	}

	public Long triplesGenerated() {
		return nrTriples;
	}
	
	class TurtleShutdown extends Thread {
		Turtle serializer;
		TurtleShutdown(Turtle t) {
			serializer = t;
		}
		
		public void run() {
			try {
			serializer.dataFileWriter.flush();
			serializer.dataFileWriter.close();
			} catch(IOException e) {}
			serializer.dataFile.delete();
		}
	}
}


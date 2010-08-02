package benchmark.serializer;

import java.util.Iterator;
import benchmark.model.*;
import benchmark.vocabulary.*;
import benchmark.generator.*;
import java.io.*;
import java.util.*;

public class NTriples implements Serializer {
	private FileWriter fileWriter;
	private boolean forwardChaining;
	private long nrTriples;
	
	public NTriples(String file, boolean forwardChaining)
	{
		try{
			fileWriter = new FileWriter(file);
		} catch(IOException e){
			System.err.println("Could not open File");
			System.exit(-1);
		}
		
		this.forwardChaining = forwardChaining;
		nrTriples = 0l;
	}
	
	public void gatherData(ObjectBundle bundle) {
		Iterator<BSBMResource> it = bundle.iterator();

		while(it.hasNext())
		{
			BSBMResource obj = it.next();
			try{
				if(obj instanceof ProductType){
					fileWriter.append(convertProductType((ProductType)obj));
				}
				else if(obj instanceof Offer){
					fileWriter.append(convertOffer((Offer)obj));
				}
				else if(obj instanceof Product){
					fileWriter.append(convertProduct((Product)obj));
				}
				else if(obj instanceof Person){
					fileWriter.append(convertPerson((Person)obj));
				}
				else if(obj instanceof Producer){
					fileWriter.append(convertProducer((Producer)obj));
				}
				else if(obj instanceof ProductFeature){
					fileWriter.append(convertProductFeature((ProductFeature)obj));
				}
				else if(obj instanceof Vendor){
					fileWriter.append(convertVendor((Vendor)obj));
				}
				else if(obj instanceof Review){
					fileWriter.append(convertReview((Review)obj));
				}
			}
			catch(IOException e){
				System.err.println("Could not write into File!");
				System.err.println(e.getMessage());
				System.exit(-1);
			}
		}
	}
	
	/*
	 * Converts the ProductType Object into an N-Triples String
	 * representation.
	 */
	private String convertProductType(ProductType pType)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		String subjectURIREF = pType.toString(); 

		//rdf:type
		result.append(createTriple(
						subjectURIREF,
						createURIref(RDF.type),
						createURIref(BSBM.ProductType)));
		
		//rdfs:label
		result.append(createTriple(
				subjectURIREF,
				createURIref(RDFS.label),
				createLiteral(pType.getLabel())));
		
		//rdfs:comment
		result.append(createTriple(
				subjectURIREF,
				createURIref(RDFS.comment),
				createLiteral(pType.getComment())));
		
		//rdfs:subClassOf
		if(pType.getParent()!=null)
		{
			String parentURIREF = createURIref(BSBM.INST_NS, "ProductType"+pType.getParent().getNr());
			result.append(createTriple(
					subjectURIREF,
					createURIref(RDFS.subClassOf),
					parentURIREF));
		}
		
		//dc:publisher
		result.append(createTriple(
				subjectURIREF,
				createURIref(DC.publisher),
				createURIref(BSBM.getStandardizationInstitution(1))));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(pType.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(createTriple(
				subjectURIREF,
				createURIref(DC.date),
				createDataTypeLiteral(dateString, createURIref(XSD.Date))));
		
		return result.toString();
	}
	
	/*
	 * Converts the Offer Object into an N-Triples String
	 * representation.
	 */
	private String convertOffer(Offer offer)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		String subjectURIREF = offer.toString(); 

		//rdf:type
		result.append(createTriple(
						subjectURIREF,
						createURIref(RDF.type),
						createURIref(BSBM.Offer)));
		
		//bsbm:product
		int productNr = offer.getProduct();
		int producerNr = Generator.getProducerOfProduct(productNr); 
		result.append(createTriple(
				subjectURIREF,
				createURIref(BSBM.product),
				Product.getURIref(productNr, producerNr)));
		
		//bsbm:vendor
		result.append(createTriple(
				subjectURIREF,
				createURIref(BSBM.vendor),
				Vendor.getURIref(offer.getVendor())));
		
		//bsbm:price
		result.append(createTriple(
				subjectURIREF,
				createURIref(BSBM.price),
				createDataTypeLiteral(offer.getPriceString(),createURIref(BSBM.USD))));
		
		//bsbm:validFrom
		GregorianCalendar validFrom = new GregorianCalendar();
		validFrom.setTimeInMillis(offer.getValidFrom());
		String validFromString = DateGenerator.formatDateTime(validFrom);
		result.append(createTriple(
				subjectURIREF,
				createURIref(BSBM.validFrom),
				createDataTypeLiteral(validFromString, createURIref(XSD.DateTime))));
		
		//bsbm:validTo
		GregorianCalendar validTo = new GregorianCalendar();
		validTo.setTimeInMillis(offer.getValidTo());
		String validToString = DateGenerator.formatDateTime(validTo);
		result.append(createTriple(
				subjectURIREF,
				createURIref(BSBM.validTo),
				createDataTypeLiteral(validToString, createURIref(XSD.DateTime))));
		
		//bsbm:deliveryDays
		result.append(createTriple(
				subjectURIREF,
				createURIref(BSBM.deliveryDays),
				createDataTypeLiteral(offer.getDeliveryDays().toString(), createURIref(XSD.Integer))));
		
		//bsbm:offerWebpage
		result.append(createTriple(
				subjectURIREF,
				createURIref(BSBM.offerWebpage),
				createURIref(offer.getOfferWebpage())));
		
		//dc:publisher
		result.append(createTriple(
				subjectURIREF,
				createURIref(DC.publisher),
				Vendor.getURIref(offer.getVendor())));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(offer.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(createTriple(
				subjectURIREF,
				createURIref(DC.date),
				createDataTypeLiteral(dateString, createURIref(XSD.Date))));
		
		return result.toString();
	}
	
	/*
	 * Converts the Product Object into an N-Triples String
	 * representation.
	 */
	private String convertProduct(Product product)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		String subjectURIREF = product.toString(); 

		//rdf:type
		result.append(createTriple(
						subjectURIREF,
						createURIref(RDF.type),
						createURIref(BSBM.Product)));
		
		//rdfs:label
		result.append(createTriple(
				subjectURIREF,
				createURIref(RDFS.label),
				createLiteral(product.getLabel())));
		
		//rdfs:comment
		result.append(createTriple(
				subjectURIREF,
				createURIref(RDFS.comment),
				createLiteral(product.getComment())));
		
		//bsbm:productType
		if(forwardChaining) {
			ProductType pt = product.getProductType();
			while(pt!=null) {
				result.append(createTriple(
						subjectURIREF,
						createURIref(RDF.type),
						pt.toString()));
				pt = pt.getParent();
			}
		}
		else {
			result.append(createTriple(
					subjectURIREF,
					createURIref(RDF.type),
					product.getProductType().toString()));
		}
		
		//bsbm:producer
		result.append(createTriple(
				subjectURIREF,
				createURIref(BSBM.producer),
				Producer.getURIref(product.getProducer())));
		
		//bsbm:productPropertyNumeric
		Integer[] ppn = product.getProductPropertyNumeric();
		for(int i=0,j=1;i<ppn.length;i++,j++)
		{
			Integer value = ppn[i];
			if(value!=null)
				result.append(createTriple(
						subjectURIREF,
						createURIref(BSBM.getProductPropertyNumeric(j)),
						createDataTypeLiteral(value.toString(), createURIref(XSD.Integer))));
		}
		
		//bsbm:productPropertyTextual
		String[] ppt = product.getProductPropertyTextual();
		for(int i=0,j=1;i<ppt.length;i++,j++)
		{
			String value = ppt[i];
			if(value!=null)
				result.append(createTriple(
						subjectURIREF,
						createURIref(BSBM.getProductPropertyTextual(j)),
						createDataTypeLiteral(value, createURIref(XSD.String))));
		}
		
		//bsbm:productFeature
		Iterator<Integer> pf = product.getFeatures().iterator();
		while(pf.hasNext())
		{
			Integer value = pf.next();
			result.append(createTriple(
					subjectURIREF,
					createURIref(BSBM.productFeature),
					ProductFeature.getURIref(value)));
		}
		
		//dc:publisher
		result.append(createTriple(
				subjectURIREF,
				createURIref(DC.publisher),
				Producer.getURIref(product.getProducer())));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(product.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(createTriple(
				subjectURIREF,
				createURIref(DC.date),
				createDataTypeLiteral(dateString, createURIref(XSD.Date))));
		
		return result.toString();
	}
	
	/*
	 * Converts the Person Object into an N-Triples String
	 * representation.
	 */
	private String convertPerson(Person person)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		String subjectURIREF = person.toString();
		
		//rdf:type
		result.append(createTriple(
						subjectURIREF,
						createURIref(RDF.type),
						createURIref(FOAF.Person)));
		
		//foaf:name
		result.append(createTriple(
				subjectURIREF,
				createURIref(FOAF.name),
				createLiteral(person.getName())));
		
		//foaf:mbox_sha1sum
		result.append(createTriple(
				subjectURIREF,
				createURIref(FOAF.mbox_sha1sum),
				createLiteral(person.getMbox_sha1sum())));
		
		//bsbm:country
		result.append(createTriple(
				subjectURIREF,
				createURIref(BSBM.country),
				createURIref(ISO3166.find(person.getCountryCode()))));
		
		//dc:publisher
		result.append(createTriple(
				subjectURIREF,
				createURIref(DC.publisher),
				RatingSite.getURIref(person.getPublisher())));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(person.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(createTriple(
				subjectURIREF,
				createURIref(DC.date),
				createDataTypeLiteral(dateString, createURIref(XSD.Date))));
		
		return result.toString();
	}
	
	/*
	 * Converts the Producer Object into an N-Triples String
	 * representation.
	 */
	private String convertProducer(Producer producer)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		String subjectURIREF = producer.toString(); 

		//rdf:type
		result.append(createTriple(
						subjectURIREF,
						createURIref(RDF.type),
						createURIref(BSBM.Producer)));
		
		//rdfs:label
		result.append(createTriple(
				subjectURIREF,
				createURIref(RDFS.label),
				createLiteral(producer.getLabel())));
		
		//rdfs:comment
		result.append(createTriple(
				subjectURIREF,
				createURIref(RDFS.comment),
				createLiteral(producer.getComment())));
		
		//foaf:homepage
		result.append(createTriple(
				subjectURIREF,
				createURIref(FOAF.homepage),
				createURIref(producer.getHomepage())));
		
		//bsbm:country
		result.append(createTriple(
				subjectURIREF,
				createURIref(BSBM.country),
				createURIref(ISO3166.find(producer.getCountryCode()))));
		
		//dc:publisher
		result.append(createTriple(
				subjectURIREF,
				createURIref(DC.publisher),
				producer.toString()));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(producer.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(createTriple(
				subjectURIREF,
				createURIref(DC.date),
				createDataTypeLiteral(dateString, createURIref(XSD.Date))));
		
		return result.toString();
	}
	
	/*
	 * Converts the ProductFeature Object into an N-Triples String
	 * representation.
	 */
	private String convertProductFeature(ProductFeature pf)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		String subjectURIREF = createURIref(BSBM.INST_NS, "ProductFeature"+pf.getNr()); 

		//rdf:type
		result.append(createTriple(
						subjectURIREF,
						createURIref(RDF.type),
						createURIref(BSBM.ProductFeature)));
		
		//rdfs:label
		result.append(createTriple(
				subjectURIREF,
				createURIref(RDFS.label),
				createLiteral(pf.getLabel())));
		
		//rdfs:comment
		result.append(createTriple(
				subjectURIREF,
				createURIref(RDFS.comment),
				createLiteral(pf.getComment())));
		
		//dc:publisher
		result.append(createTriple(
				subjectURIREF,
				createURIref(DC.publisher),
				createURIref(BSBM.getStandardizationInstitution(pf.getPublisher()))));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(pf.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(createTriple(
				subjectURIREF,
				createURIref(DC.date),
				createDataTypeLiteral(dateString, createURIref(XSD.Date))));
		
		return result.toString();
	}
	
	/*
	 * Converts the Vendor Object into an N-Triples String
	 * representation.
	 */
	private String convertVendor(Vendor vendor)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		String subjectURIREF = vendor.toString(); 

		//rdf:type
		result.append(createTriple(
						subjectURIREF,
						createURIref(RDF.type),
						createURIref(BSBM.Vendor)));
		
		//rdfs:label
		result.append(createTriple(
				subjectURIREF,
				createURIref(RDFS.label),
				createLiteral(vendor.getLabel())));
		
		//rdfs:comment
		result.append(createTriple(
				subjectURIREF,
				createURIref(RDFS.comment),
				createLiteral(vendor.getComment())));
		
		//foaf:homepage
		result.append(createTriple(
				subjectURIREF,
				createURIref(FOAF.homepage),
				createURIref(vendor.getHomepage())));
		
		//bsbm:country
		result.append(createTriple(
				subjectURIREF,
				createURIref(BSBM.country),
				createURIref(ISO3166.find(vendor.getCountryCode()))));
		
		//dc:publisher
		result.append(createTriple(
				subjectURIREF,
				createURIref(DC.publisher),
				vendor.toString()));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(vendor.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(createTriple(
				subjectURIREF,
				createURIref(DC.date),
				createDataTypeLiteral(dateString, createURIref(XSD.Date))));
		
		return result.toString();
	}
	
	
	/*
	 * Converts the Review Object into an N-Triples String
	 * representation.
	 */
	private String convertReview(Review review)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		String subjectURIREF = review.toString(); 

		//rdf:type
		result.append(createTriple(
						subjectURIREF,
						createURIref(RDF.type),
						createURIref(REV.Review)));
		
		//bsbm:reviewFor
		result.append(createTriple(
				subjectURIREF,
				createURIref(BSBM.reviewFor),
				Product.getURIref(review.getProduct(), review.getProducerOfProduct())));
		
		//rev:reviewer
		result.append(createTriple(
				subjectURIREF,
				createURIref(REV.reviewer),
				Person.getURIref(review.getPerson(), review.getPublisher())));
		
		//bsbm:reviewDate
		GregorianCalendar reviewDate = new GregorianCalendar();
		reviewDate.setTimeInMillis(review.getReviewDate());
		String reviewDateString = DateGenerator.formatDateTime(reviewDate);
		result.append(createTriple(
				subjectURIREF,
				createURIref(BSBM.reviewDate),
				createDataTypeLiteral(reviewDateString, createURIref(XSD.DateTime))));
		
		//dc:title
		result.append(createTriple(
				subjectURIREF,
				createURIref(DC.title),
				createLiteral(review.getTitle())));
		
		//rev:text
		result.append(createTriple(
				subjectURIREF,
				createURIref(REV.text),
				createLanguageLiteral(review.getText(),ISO3166.language[review.getLanguage()])));
		
		//bsbm:ratingX
		Integer[] ratings = review.getRatings();
		for(int i=0,j=1;i<ratings.length;i++,j++)
		{
			Integer value = ratings[i];
			if(value!=null)
				result.append(createTriple(
						subjectURIREF,
						createURIref(BSBM.getRating(j)),
						createDataTypeLiteral(value.toString(), createURIref(XSD.Integer))));
		}
		
		//dc:publisher
		result.append(createTriple(
				subjectURIREF,
				createURIref(DC.publisher),
				RatingSite.getURIref(review.getPublisher())));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(review.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(createTriple(
				subjectURIREF,
				createURIref(DC.date),
				createDataTypeLiteral(dateString, createURIref(XSD.Date))));
		
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

	//Creates a triple
	private String createTriple(String subject, String predicate, String object)
	{
		StringBuffer result = new StringBuffer();
		result.append(subject);
		result.append(" ");
		result.append(predicate);
		result.append(" ");
		result.append(object);
		result.append(" .\n");
		
		nrTriples++;
		
		return result.toString();
	}
	
	//Create URIREF from namespace and element
	private String createURIref(String namespace, String element)
	{
		StringBuffer result = new StringBuffer();
		result.append("<");
		result.append(namespace);
		result.append(element);
		result.append(">");
		return result.toString();
	}
	
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
		//Close File
		try {
			fileWriter.flush();
			fileWriter.close();
		} catch(IOException e) {
			System.err.println(e.getMessage());
			System.exit(-1);
		}
	}

	public Long triplesGenerated() {
		return nrTriples;
	}
}

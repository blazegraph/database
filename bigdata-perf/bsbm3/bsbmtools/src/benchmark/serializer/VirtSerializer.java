package benchmark.serializer;

import java.io.FileWriter;
import java.io.IOException;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.io.File;

import benchmark.generator.DateGenerator;
import benchmark.generator.Generator;
import benchmark.model.BSBMResource;
import benchmark.model.Offer;
import benchmark.model.Person;
import benchmark.model.Producer;
import benchmark.model.Product;
import benchmark.model.ProductFeature;
import benchmark.model.ProductType;
import benchmark.model.Review;
import benchmark.model.Vendor;
import benchmark.vocabulary.ISO3166;

public class VirtSerializer implements Serializer {
	private File outputDir;
	private boolean forwardChaining;
	private long nrTriples;
	private SQLTables tables;
	private static final int insertNumber = 1; //Number of insert tuples per insert operation
	
	public VirtSerializer (String directory, boolean forwardChaining) {
		outputDir = new File(directory);
		outputDir.mkdirs();
		
		this.forwardChaining = forwardChaining;
		nrTriples = 0l;
		
		initTables();
	}
	
	public void gatherData(ObjectBundle bundle) {
		Iterator<BSBMResource> it = bundle.iterator();
	
		try {
			while(it.hasNext())
			{
				BSBMResource obj = it.next();
	
				if(obj instanceof ProductType){
					convertProductType((ProductType)obj);
				}
				else if(obj instanceof Offer){
					convertOffer((Offer)obj);
				}
				else if(obj instanceof Product){
					convertProduct((Product)obj);
				}
				else if(obj instanceof Person){
					convertPerson((Person)obj);
				}
				else if(obj instanceof Producer){
					convertProducer((Producer)obj);
				}
				else if(obj instanceof ProductFeature){
					convertProductFeature((ProductFeature)obj);
				}
				else if(obj instanceof Vendor){
					convertVendor((Vendor)obj);
				}
				else if(obj instanceof Review){
					convertReview((Review)obj);
				}
			}
		}catch(IOException e){
			System.err.println("Could not write into File!");
			System.err.println(e.getMessage());
			System.exit(-1);
		}
	}

	/*
	 * Converts the ProductType Object into an N-Triples String
	 * representation.
	 */
	private void convertProductType(ProductType pType) throws IOException
	{
		StringBuffer values = getBuffer(tables.productTypeInsertCounter++, "ProductType");
		values.append("(");
		//nr
		values.append(pType.getNr());
		values.append(",");
		
		//rdfs:label
		values.append("'");
		values.append(pType.getLabel());
		values.append("',");
		
		//rdfs:comment
		values.append("'");
		values.append(pType.getComment());
		values.append("',");
		
		//rdfs:subClassOf
		if(pType.getParent()!=null) {
			values.append(pType.getParent().getNr());
			values.append(",");
		}
		else
			values.append("null,");
		
		//dc:publisher
		values.append(pType.getPublisher());
		values.append(",");
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(pType.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		values.append("cast ('");
		values.append(dateString);
		values.append("' as date))");
		
		if(tables.productTypeInsertCounter>=insertNumber) {
			tables.productTypeInsertCounter = 0;
			values.append(";\n");
		}
		tables.productTypeDump.append(values);
	}
	
	/*
	 * Converts the Offer Object into an N-Triples String
	 * representation.
	 */
	private void convertOffer(Offer offer) throws IOException
	{
		  	StringBuffer values = getBuffer(tables.offerInsertCounter++, "Offer");
		  	values.append("(");
		  
//		  	nr
			values.append(offer.getNr());
			values.append(",");
			
//			product
			values.append(offer.getProduct());
			values.append(",");

//			producer
			values.append(Generator.getProducerOfProduct(offer.getProduct()));
			values.append(",");

//			vendor
			values.append(offer.getVendor());
			values.append(",");
			
//			price
			values.append(offer.getPriceString());
			values.append(",");
			
//			validFrom
			GregorianCalendar validFrom = new GregorianCalendar();
			validFrom.setTimeInMillis(offer.getValidFrom());
			String validFromString = DateGenerator.formatDate(validFrom);
			values.append("cast ('" + validFromString + "' as dateTime),");
			
			
//			validTo
			GregorianCalendar validTo = new GregorianCalendar();
			validTo.setTimeInMillis(offer.getValidTo());
			String validToString = DateGenerator.formatDate(validTo);
			values.append("cast ('" + validToString + "' as dateTime),");
			
//			deliverDays
			values.append(offer.getDeliveryDays());
			values.append(",");
			
//			offerWebpage
			values.append("'" + offer.getOfferWebpage() + "',");
			
//		 	dc:publisher
			values.append(offer.getPublisher());
			values.append(",");
			
			//dc:date
			GregorianCalendar date = new GregorianCalendar();
			date.setTimeInMillis(offer.getPublishDate());
			String dateString = DateGenerator.formatDate(date);
			values.append("cast ('");
			values.append(dateString);
			values.append("' as date))");
			
			if(tables.offerInsertCounter>=insertNumber) {
				tables.offerInsertCounter = 0;
				values.append(";\n");
			}
			tables.offerDump.append(values);
	}
	
	/*
	 * Converts the Product Object into an N-Triples String
	 * representation.
	 */
	private void convertProduct(Product product)throws IOException
	{
		StringBuffer values = getBuffer(tables.productInsertCounter++, "Product");
		
		values.append("(");
		
		//nr
		values.append(product.getNr());
		values.append(",");
		
		//label
		values.append("'");
		values.append(product.getLabel());
		values.append("',");
		
		//comment
		values.append("'");
		values.append(product.getComment());
		values.append("',");
		
		//producer
		values.append(product.getProducer());
		values.append(",");
	
		//rdf:type for product types 
		if(forwardChaining) {
			ProductType pt = product.getProductType();
			while(pt!=null) {
				StringBuffer valuesPTP = getBuffer(tables.productTypeProductInsertCounter++, "ProductTypeProduct");
				
				valuesPTP.append("(" + product.getNr() + ",");
				valuesPTP.append(Integer.valueOf(pt.getNr()).toString());
				valuesPTP.append(")");
				
				if(tables.productTypeProductInsertCounter>=insertNumber) {
					tables.productTypeProductInsertCounter = 0;
					valuesPTP.append(";\n");
				}
				tables.productTypeProductDump.append(valuesPTP);
				
				pt = pt.getParent();
			}
		}
		else {
			StringBuffer valuesPTP = getBuffer(tables.productTypeProductInsertCounter++, "ProductTypeProduct");
			
			valuesPTP.append("(" + product.getNr() + ",");
			valuesPTP.append(Integer.valueOf(product.getProductType().getNr()).toString());
			valuesPTP.append(")");
			
			if(tables.productTypeProductInsertCounter>=insertNumber) {
				tables.productTypeProductInsertCounter = 0;
				valuesPTP.append(";\n");
			}
			tables.productTypeProductDump.append(valuesPTP);
		}
		
	
		//propertyNum
		Integer[] ppn = product.getProductPropertyNumeric();
		for(Integer i=0,j=1;i<ppn.length;i++,j++)
		{
			Integer value = ppn[i];
			if(value!=null)
				values.append(value.toString() + ",");
			else
				values.append("null,");
		}
		
		//propertyTex
		String[] ppt = product.getProductPropertyTextual();
		for(Integer i=0,j=1;i<ppt.length;i++,j++)
		{
			String value = ppt[i];
			if(value!=null)
				values.append("'" + value + "',");
			else
				values.append("null,");
		}
		
		//productFeatureProduct
		Iterator<Integer> pf = product.getFeatures().iterator();
		while(pf.hasNext())
		{
			StringBuffer valuesPFP = getBuffer(tables.productFeatureProductInsertCounter++, "ProductFeatureProduct");
			valuesPFP.append("(");
			Integer value = pf.next();

			valuesPFP.append(product.getNr());
			valuesPFP.append("," + value);
			
			valuesPFP.append(")");
			if(tables.productFeatureProductInsertCounter>=insertNumber) {
				tables.productFeatureProductInsertCounter = 0;
				valuesPFP.append(";\n");
			}
			tables.productFeatureProductDump.append(valuesPFP);

		}
		
		//dc:publisher
		values.append(Integer.valueOf(product.getProducer()).toString());
		values.append(",");
			
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(product.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		values.append("cast ('" + dateString + "' as date))");

		if(tables.productInsertCounter>=insertNumber) {
			tables.productInsertCounter = 0;
			values.append(";\n");
		}
		tables.productDump.append(values);
	}
	
	/*
	 * Converts the Person Object into an N-Triples String
	 * representation.
	 */
	private void convertPerson(Person person)throws IOException
	{
		StringBuffer values = getBuffer(tables.personInsertCounter++, "Person");
		values.append("(");
	
		//nr
		values.append(person.getNr());
		values.append(",");
		
		//name
		values.append("'");
		values.append(person.getName());
		values.append("',");
		
		//mbox_sha1sum
		values.append("'");
		values.append(person.getMbox_sha1sum());
		values.append("',");
		
		//country
		values.append("'");
		values.append(person.getCountryCode());
		values.append("',");
		
		//dc:publisher
		values.append(person.getPublisher());
		values.append(",");
			
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(person.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		values.append("cast ('" + dateString + "' as date))");
		
		if(tables.personInsertCounter>=insertNumber) {
			tables.personInsertCounter = 0;
			values.append(";\n");
		}
		tables.personDump.append(values);
	}
	
	/*
	 * Converts the Producer Object into an N-Triples String
	 * representation.
	 */
	private void convertProducer(Producer producer) throws IOException
	{
		StringBuffer values = getBuffer(tables.producerInsertCounter++, "Producer");
		values.append("(");
	
		//nr
		values.append(producer.getNr());
		values.append(",");
		
		//label
		values.append("'");
		values.append(producer.getLabel());
		values.append("',");
		
		//comment
		values.append("'");
		values.append(producer.getComment());
		values.append("',");
		
		//homepage
		values.append("'");
		values.append(producer.getHomepage());
		values.append("',");
		
		//country
		values.append("'");
		values.append(producer.getCountryCode());
		values.append("',");
		
		//dc:publisher
		values.append(producer.getPublisher());
		values.append(",");
			
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(producer.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		values.append("cast ('" + dateString + "' as date))");
		
		if(tables.producerInsertCounter>=insertNumber) {
			tables.producerInsertCounter = 0;
			values.append(";\n");
		}
		tables.producerDump.append(values);
	}
	
	/*
	 * Converts the ProductFeature Object into an N-Triples String
	 * representation.
	 */
	private void convertProductFeature(ProductFeature pf) throws IOException
	{
		StringBuffer values = getBuffer(tables.productFeatureInsertCounter++, "ProductFeature");
		values.append("(");
	
		//nr
		values.append(pf.getNr());
		values.append(",");
		
		//rdfs:label
		values.append("'");
		values.append(pf.getLabel());
		values.append("',");
		
		//rdfs:comment
		values.append("'");
		values.append(pf.getComment());
		values.append("',");
		
		//dc:publisher
		values.append(pf.getPublisher());
		values.append(",");
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(pf.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		values.append("cast ('");
		values.append(dateString);
		values.append("' as date))");
		
		if(tables.productFeatureInsertCounter>=insertNumber) {
			tables.productFeatureInsertCounter = 0;
			values.append(";\n");
		}
		tables.productFeatureDump.append(values);
	}
	
	/*
	 * Converts the Vendor Object into an N-Triples String
	 * representation.
	 */
	private void convertVendor(Vendor vendor) throws IOException
	{
		StringBuffer values = getBuffer(tables.vendorInsertCounter++, "Vendor");
		values.append("(");
	
		//nr
		values.append(vendor.getNr());
		values.append(",");
		
		//label
		values.append("'");
		values.append(vendor.getLabel());
		values.append("',");
		
		//comment
		values.append("'");
		values.append(vendor.getComment());
		values.append("',");
		
		//homepage
		values.append("'");
		values.append(vendor.getHomepage());
		values.append("',");
		
		//country
		values.append("'");
		values.append(vendor.getCountryCode());
		values.append("',");
		
		//dc:publisher
		values.append(vendor.getPublisher());
		values.append(",");
			
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(vendor.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		values.append("cast ('" + dateString + "' as date))");
		
		if(tables.vendorInsertCounter>=insertNumber) {
			tables.vendorInsertCounter = 0;
			values.append(";\n");
		}
		tables.vendorDump.append(values);
	}
	
	
	/*
	 * Converts the Review Object into an N-Triples String
	 * representation.
	 */
	private void convertReview(Review review) throws IOException
	{
		StringBuffer values = getBuffer(tables.reviewInsertCounter++, "Review");
		values.append("(");
	
		//nr
		values.append(review.getNr());
		values.append(",");
		
		//product
		values.append(review.getProduct());
		values.append(",");

		//producer
		values.append(review.getProducerOfProduct());
		values.append(",");

		//person
		values.append(review.getPerson());
		values.append(",");
		
		//reviewDate
		GregorianCalendar reviewDate = new GregorianCalendar();
		reviewDate.setTimeInMillis(review.getReviewDate());
		String reviewDateString = DateGenerator.formatDate(reviewDate);
		values.append("cast ('" + reviewDateString + "' as dateTime),");
		
		//title
		values.append("'");
		values.append(review.getTitle());
		values.append("',");
		
		//title
		values.append("'");
		values.append(review.getText());
		values.append("',");
		
		//language
		values.append("'");
		values.append(ISO3166.language[review.getLanguage()]);
		values.append("',");
		
		//ratings
		Integer[] ratings = review.getRatings();
		for(int i=0;i<ratings.length;i++)
		{
			Integer value = ratings[i];
			values.append(value);
			values.append(",");
		}
		
		//dc:publisher
		values.append(review.getPublisher());
		values.append(",");
			
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(review.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		values.append("cast ('" + dateString + "' as date))");
		
		if(tables.reviewInsertCounter>=insertNumber) {
			tables.reviewInsertCounter = 0;
			values.append(";\n");
		}
		tables.reviewDump.append(values);
	}
	
	public void serialize() {
		//Finish files and close
		try {
			tables.productTypeDump.flush();
			tables.productTypeDump.close();
			
			tables.productFeatureDump.flush();
			tables.productFeatureDump.close();
			
			tables.producerDump.flush();
			tables.producerDump.close();
			
			tables.productDump.flush();
			tables.productDump.close();
			
			tables.productTypeProductDump.flush();
			tables.productTypeProductDump.close();
			
			tables.productFeatureProductDump.flush();
			tables.productFeatureProductDump.close();
			
			tables.vendorDump.flush();
			tables.vendorDump.close();
			
			tables.offerDump.flush();
			tables.offerDump.close();
			
			tables.personDump.flush();
			tables.personDump.close();
			
			tables.reviewDump.flush();
			tables.reviewDump.close();
			
		} catch(IOException e) {
			System.err.println(e.getMessage());
			System.exit(-1);
		}
	}

	public Long triplesGenerated() {
		return nrTriples;
	}

	private static class SQLTables {
		FileWriter offerDump;
		FileWriter vendorDump;
		FileWriter productFeatureDump;
		FileWriter productDump;
		FileWriter producerDump;
		FileWriter productTypeProductDump;
		FileWriter personDump;
		FileWriter productTypeDump;
		FileWriter reviewDump;
		FileWriter productFeatureProductDump;
		
		int offerInsertCounter;
		int vendorInsertCounter;
		int productFeatureInsertCounter;
		int productInsertCounter;
		int producerInsertCounter;
		int productTypeProductInsertCounter;
		int personInsertCounter;
		int productTypeInsertCounter;
		int reviewInsertCounter;
		int productFeatureProductInsertCounter;
		
		SQLTables() {
		}
	}
	
	private void initTables() {
		tables = new SQLTables();
		tables.offerInsertCounter=0;
		tables.vendorInsertCounter=0;
		tables.productFeatureInsertCounter=0;
		tables.productInsertCounter=0;
		tables.producerInsertCounter=0;
		tables.productTypeProductInsertCounter=0;
		tables.personInsertCounter=0;
		tables.productTypeInsertCounter=0;
		tables.reviewInsertCounter=0;
		tables.productFeatureProductInsertCounter=0;
		
		try {
		tables.offerDump = new FileWriter(new File(outputDir, "08Offer.sql"));
		tables.vendorDump = new FileWriter(new File(outputDir, "07Vendor.sql"));
		tables.productFeatureDump = new FileWriter(new File(outputDir, "01ProductFeature.sql"));
		tables.productDump = new FileWriter(new File(outputDir, "04Product.sql"));
		tables.producerDump = new FileWriter(new File(outputDir, "03Producer.sql"));
		tables.productTypeProductDump = new FileWriter(new File(outputDir, "05ProductTypeProduct.sql"));
		tables.personDump = new FileWriter(new File(outputDir, "09Person.sql"));
		tables.productTypeDump = new FileWriter(new File(outputDir, "02ProductType.sql"));
		tables.reviewDump = new FileWriter(new File(outputDir, "10Review.sql"));
		tables.productFeatureProductDump = new FileWriter(new File(outputDir, "06ProductFeatureProduct.sql"));
				
		} catch(IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	private StringBuffer getBuffer(int counter, String tableName) {
		StringBuffer sb = new StringBuffer();
		if(counter==0)
			sb.append("INSERT INTO " + tableName + " VALUES ");
		else
			sb.append(",");
		
		return sb;
	}
}

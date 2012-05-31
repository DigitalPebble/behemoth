package com.digitalpebble.behemoth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

public class DocumentFilterTest extends TestCase {

	protected void setUp() throws Exception {
		super.setUp();
	}

	public void testPositiveFilter() {
		Configuration config = BehemothConfiguration.create();
		config.set(DocumentFilter.DocumentFilterParamNamePrefixKeep + "lang",
				"en");

		DocumentFilter filter = DocumentFilter.getFilters(config);

		BehemothDocument doc = new BehemothDocument();
		doc.getMetadata(true).put(new Text("lang"), new Text("en"));
		boolean kept = filter.filter(doc);
		assertEquals(true, kept);

		doc.getMetadata(true).put(new Text("lang"), new Text("fr"));
		kept = filter.filter(doc);
		assertEquals(false, kept);

		// negative filters

		config = BehemothConfiguration.create();
		config.set(DocumentFilter.DocumentFilterParamNamePrefixSkip + "lang",
				".+");

		filter = DocumentFilter.getFilters(config);
		kept = filter.filter(doc);
		assertEquals(false, kept);

		doc = new BehemothDocument();
		kept = filter.filter(doc);
		assertEquals(true, kept);

	}

	public void testNegativeFilter() {
		Configuration config = BehemothConfiguration.create();
		config.set(DocumentFilter.DocumentFilterParamNamePrefixSkip + "lang",
				".+");

		DocumentFilter filter = DocumentFilter.getFilters(config);
		BehemothDocument doc = new BehemothDocument();
		doc.getMetadata(true).put(new Text("lang"), new Text("en"));
		boolean kept = filter.filter(doc);
		assertEquals(false, kept);

		doc = new BehemothDocument();
		kept = filter.filter(doc);
		assertEquals(true, kept);

	}

}

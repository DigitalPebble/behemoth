package com.digitalpebble.behemoth.util.archive;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Enumeration;

import org.junit.Before;
import org.junit.Test;

import com.digitalpebble.behemoth.BehemothDocument;

public class ArchiveReaderTest {

	String testDataDir;

	private void checkResults(Enumeration<BehemothDocument>  e) {
		assertTrue(e.nextElement().getUrl().equals("docs/droitshomme.txt"));
		assertTrue(e.nextElement().getUrl().equals("docs/BP.html"));
		assertTrue(e.nextElement().getUrl().equals("docs/spending-cuts.html"));
	}
	 
	@Before
	public void setUp() throws Exception {
		String tempTestDataDir = System.getProperty("test.build.data");
		testDataDir = tempTestDataDir != null ? tempTestDataDir : "modules/core/src/test/data";
	}
	
	@Test
	public void testTar() throws Exception {
		checkResults(new ArchiveReader(new File(testDataDir, "docs.tar")));
	}
	
	@Test
	public void testTgz() throws Exception {
		checkResults(new ArchiveReader(new File(testDataDir, "docs.tgz")));
	}
	
	@Test
	public void testBzip() throws Exception {
		checkResults(new ArchiveReader(new File(testDataDir, "docs.tar.bz2")));
	}
	
	@Test
	public void testZip() throws Exception {
		checkResults(new ArchiveReader(new File(testDataDir, "docs.zip")));
	}
}

	
package com.digitalpebble.behemoth.cli;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.gate.GATECorpusGenerator;
import com.digitalpebble.behemoth.gate.GATEDriver;
import com.digitalpebble.behemoth.io.nutch.NutchSegmentConverterJob;
import com.digitalpebble.behemoth.io.warc.WARCConverterJob;
import com.digitalpebble.behemoth.mahout.SparseVectorsFromBehemoth;
import com.digitalpebble.behemoth.solr.SOLRIndexerJob;
import com.digitalpebble.behemoth.tika.TikaDriver;
import com.digitalpebble.behemoth.uima.UIMADriver;
import com.digitalpebble.behemoth.util.CorpusGenerator;
import com.digitalpebble.behemoth.util.CorpusReader;

/**
 * Provides a simplified command line interface to Behemoth so it is possible to 
 * call a task directly rather than have to type the qualified package name.
 * Also provides unified help for the different tasks. 
 */
public class Behemoth {

	public final static String USAGE = "Scalable document processing with Hadoop";

	private Map<String, Class<? extends Tool>> actions = new HashMap<String, Class<? extends Tool>>();

	private ActionCliProcessor cli;

	public Behemoth() {
	}

	/**
	 * Add an action.
	 * 
	 * @param clazz The class that implements CliAction.
	 * @throws Exception Thrown if the class cannot be instantiated.
	 */
	void addAction(Class<? extends Tool> clazz, String usage) throws Exception {
		String name = cli.addAction(clazz.getSimpleName(), usage);
		actions.put(name, clazz);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.digitalpebble.behemoth.CliAction#run(java.lang.String[])
	 */
	public int run(String[] args) throws Exception {
		cli = new ActionCliProcessor(Behemoth.class.getSimpleName(), USAGE);
		addAction(CorpusGenerator.class, CorpusGenerator.USAGE);
		addAction(CorpusReader.class, CorpusReader.USAGE);
		addAction(GATEDriver.class, GATEDriver.USAGE);
		addAction(GATECorpusGenerator.class, GATECorpusGenerator.USAGE);
		addAction(NutchSegmentConverterJob.class, NutchSegmentConverterJob.USAGE);
		addAction(WARCConverterJob.class, WARCConverterJob.USAGE);
		addAction(SparseVectorsFromBehemoth.class, SparseVectorsFromBehemoth.USAGE);
		addAction(SOLRIndexerJob.class, SOLRIndexerJob.USAGE);
		addAction(TikaDriver.class, TikaDriver.USAGE);
		addAction(UIMADriver.class, UIMADriver.USAGE);
		
		try {
			cli.parse(args);
		} catch (ParseException me) {
			return -1;
		}

		if (cli.printComprehensiveHelp()) {
			cli.showUsage();
			
			// Print comprehensive help for each module
			
			for (String actionName : actions.keySet()) {
				System.out.println();
				String[] newargs = { actionName };
				mainAction(newargs);
			}
			return 0;
		} else {
			try {
				// Call the module
				return mainAction(args);
			} catch (Exception e) {
				return -1;
			}
		}
	}
	
	/**
	 * Calls the correct module
	 * 
	 * @param args The current command line arguments.
	 * @return Was the command successful?
	 * @throws Exception Thrown if there is a problem initializing the module via reflection.
	 */
	private int mainAction(String[] args) throws Exception {
		Class<? extends Tool> clazz = actions.get(args[0]);
		String[] newargs = new String[args.length - 1];
		System.arraycopy(args, 1, newargs, 0, args.length - 1);
		Tool tool = clazz.getConstructor().newInstance();
		return ToolRunner.run(BehemothConfiguration.create(),
                tool, newargs);
	}
	
	public static void main(String[] args) throws Exception {
		new Behemoth().run(args);
	}
}

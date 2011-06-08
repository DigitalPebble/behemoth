package com.digitalpebble.behemoth.cli;

import java.util.Collection;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.WordUtils;

/**
 * Command line processor that supports a list
 * of actions of a list of modules.  
 */
public class ActionCliProcessor extends CliProcessor {
	
	OptionGroup actions;
	
	boolean printComprehensiveHelp = false;
	
	/**
	 * Constructor.
	 * 
	 * @param name The task name.
	 * @param usage The task description.
	 */
	public ActionCliProcessor(String name, String description) throws Exception {
		super(name, description);
		actions = new OptionGroup();
		actions.setRequired(true);
	}
	
	/**
	 * Used to call the different tasks, CorpusGenerator, GATEDriver etc.
	 * 
	 * @param actionName The task name.
	 * @param description The description of the task. 
	 * @return The task name.
	 */
	public String addAction(String actionName, String description) {
		Option opt = new Option(actionName, false, description);
		opt.setRequired(false);
		actions.addOption(opt);
		return actionName;
	}
	
	/* (non-Javadoc)
	 * @see com.digitalpebble.behemoth.cli.CliProcessor#showUsage()
	 */
	public void showUsage() {
		Options actionOptions = new Options();
		actionOptions.addOptionGroup(actions);
		System.out.println(WordUtils.wrap(name + ": " + description, 80));
		helpFormatter.setOptPrefix("");
		helpFormatter.printHelp(name + " <action> -h, --help ", actionOptions, false);
	}
	
	/**
	 * Was comprehensive help requested?
	 * 
	 * @return Comprehensive help?
	 */
	public boolean printComprehensiveHelp() {
		return printComprehensiveHelp;
	}
	
	/* (non-Javadoc)
	 * @see com.digitalpebble.behemoth.cli.CliProcessor#parse(java.lang.String[])
	 */
	public void parse(String[] args) throws ParseException {
		if (args.length == 0) {
			showUsage();
			throw new ParseException("No specified action.");
		}
		Collection<Option> optionList = (Collection<Option>) actions.getOptions();
		boolean found = false;
		for (Option o : optionList) {
			if (o.getOpt().equals(args[0])) {
				found = true;
			}
		}
		String helpOpt = addOption("h", "help", "Print comprehensive help", false);
		if (!found) {
			super.parse(args);
			if (cli.hasOption(helpOpt))
				printComprehensiveHelp = true;
		}
	}
}

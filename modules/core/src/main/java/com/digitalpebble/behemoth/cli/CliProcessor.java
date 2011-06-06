package com.digitalpebble.behemoth.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.WordUtils;


/**
 * A basic command line interface using Apache Commons CLI.
 */
public class CliProcessor {

	Options options = new Options();

	String name;

	String description;

	CommandLine cli = null;
	
	HelpFormatter helpFormatter = new HelpFormatter();

	/**
	 * Constructor.
	 * 
	 * @param name The task name.
	 * @param usage The task description.
	 */
	public CliProcessor(String name, String usage) {
		this.name = name;
		this.description = usage;
	}

	/**
	 * Parse the command line arguments
	 * 
	 * @param args The command line arguments
	 * @throws ParseException Thrown if the command line cannot be parsed e.g.
	 *             is missing a required option or argument value.
	 */
	public void parse(String[] args) throws ParseException {
		CommandLineParser parser = new GnuParser();
		try {
			cli = parser.parse(options, args);
		} catch (ParseException e) {
			showUsage();
			throw e;
		}
	}
			
	/**
	 * Print the usage information on this command to System.out.
	 */
	public void showUsage() {
		System.out.println(WordUtils.wrap(name + ": " + description, 80));
		helpFormatter.printHelp(name, options, true);
	}

	/**
	 * Get an argument value of an option.
	 * 
	 * @param optname The short name of the option.
	 * @return The argument value.
	 */
	public String getOptionValue(String optname) {
		return cli.getOptionValue(optname);
	}

	/**
	 * Has this option been set at the command line?
	 * 
	 * @param optname The short name of the option.
	 * @return Has the option been set?
	 */
	public boolean hasOption(String optname) {
		return cli.hasOption(optname);
	}

	/**
	 * Get the long name of the option.
	 * 
	 * @param optname The long name of the option.
	 * @return The long name.
	 */
	public String getLongOpt(String optname) {
		return options.getOption(optname).getLongOpt();
	}

	/**
	 * Add a required option.
	 * 
	 * @param shortName The short name of the option.
	 * @param longName The long name of the option.
	 * @param description The option description.
	 * @param hasArgument Does the option have an argument?
	 * @return The option short name.
	 */
	public String addRequiredOption(String shortName, String longName,
			String description, boolean hasArgument) {
		return addOption(shortName, longName, description, hasArgument, true,
				null);
	}

	/**
	 * Add an option.
	 * 
	 * @param shortName The short name of the option.
	 * @param longName The long name of the option.
	 * @param description The option description.
	 * @param hasArgument Does the option have an argument?
	 * @return The option short name.
	 */
	public String addOption(String shortName, String longName,
			String description, boolean hasArgument) {
		return addOption(shortName, longName, description, hasArgument, false,
				null);
	}

	/**
	 * Add a required option.
	 * 
	 * @param shortName The short name of the option.
	 * @param longName The long name of the option.
	 * @param description The option description.
	 * @param hasArgument Does the option have an argument?
	 * @param argName The argument name.
	 * @return The option short name.
	 */
	public String addRequiredOption(String shortName, String longName,
			String description, boolean hasArgument, String argName) {
		return addOption(shortName, longName, description, hasArgument, true,
				argName);
	}

	private String addOption(String shortName, String longName,
			String description, boolean hasArgument, boolean required,
			String argname) {
		Option opt = new Option(shortName, hasArgument, description);
		opt.setLongOpt(longName);
		opt.setRequired(required);
		if (argname != null) {
			opt.setArgName(argname);
		} else {
			opt.setArgName(longName);
		}
		options.addOption(opt);
		return shortName;
	}

	/**
	 * Get an argument value as an integer.
	 * 
	 * @param optname The option short name.
	 * @param defaultvalue The default integer value.
	 * @return The argument value as an integer.
	 * @throws ParseArgumentException Thrown if the argument was not parseable
	 *             as an integer.
	 */
	public int getIntArgument(String optname, int defaultvalue)
			throws ParseArgumentException {
		int result = defaultvalue;
		String longoptname = options.getOption(optname).getLongOpt();
		String value = cli.getOptionValue(optname);
		if (value != null) {
			try {
				result = Integer.parseInt(value);
			} catch (NumberFormatException e) {
				throw new ParseArgumentException(longoptname, value,
						e.getMessage());
			}
		}
		return result;
	}
}

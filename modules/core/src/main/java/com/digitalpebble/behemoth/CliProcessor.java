package com.digitalpebble.behemoth;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.WordUtils;

public class CliProcessor {

	Options options = new Options();

	String name;
	
	String description;

	CommandLine cli = null;

	public CliProcessor(String name, String description) {
		this.name = name;
		this.description = description;
	}

	public void parse(String[] args) throws ParseException {
		CommandLineParser parser = new GnuParser();
		try {
			cli = parser.parse(options, args);
		} catch (ParseException e) {
			showUsage();
			throw e;
		}
	}

	public void showUsage() {
		System.out.println( WordUtils.wrap(name + ": " + description, 80));
		StringBuilder use = new StringBuilder();
		use.append(name);
		List<String> requiredOptions = new ArrayList<String>(
				options.getRequiredOptions());
		for (String required : requiredOptions) {
			use.append(" -" + required + " <"
					+ options.getOption(required).getLongOpt() + ">");
		}
		if (options.getOptions().size() > requiredOptions.size()) {
			use.append(" [");
			List<Option> optionList = new ArrayList<Option>(
					options.getOptions());
			for (Option option : optionList) {
				if (!requiredOptions.contains(option.getOpt())) {
					use.append(" -" + option.getOpt() + " <"
							+ option.getLongOpt() + ">");
				}
			}
			use.append(" ]");
		}
		new HelpFormatter().printHelp(use.toString(), options);
	}
	
	public String getOptionValue(String optname) {
		return cli.getOptionValue(optname);
	}
	
	public boolean hasOption(String optname) {
		return cli.hasOption(optname);
	}
	
	public String getLongOpt(String optname) {
		return options.getOption(optname).getLongOpt();
	}

	public String addRequiredOption(String shortName, String longName,
			String description, boolean hasArgument) {
		return addOption(shortName, longName, description, hasArgument, true,
				null);
	}

	public String addOption(String shortName, String longName,
			String description, boolean hasArgument) {
		return addOption(shortName, longName, description, hasArgument, false,
				null);
	}

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

	public int getIntArgument(String optname, int defaultvalue)
			throws ParseArgumentException {
		int result = defaultvalue;
		String longoptname = options.getOption(optname).getLongOpt();
		String value = cli.getOptionValue(optname); 
		if (value != null) {
			try {
				result = Integer.parseInt(value);
			} catch (NumberFormatException e) {
				throw new ParseArgumentException(longoptname, value, e.getMessage());
			}
		}
		return result;
	}
}

package com.digitalpebble.behemoth;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A command line processor that includes input and output options intended to
 * be used for paths on HDFS.
 */
public class InputOutputReplaceCliProcessor extends CliProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(InputOutputReplaceCliProcessor.class);

	String inputOpt;

	String outputOpt;

	String replaceOpt;

	/**
	 * Constructor.
	 * 
	 * @param name The command name.
	 * @param description The command description.
	 */
	public InputOutputReplaceCliProcessor(String name, String description) {
		super(name, description);
		inputOpt = addRequiredOption("i", "input", "Input path on HDFS", true);
		outputOpt = addOption("o", "output", "Output directory on HDFS", true);
		replaceOpt = addOption("r", "replace",
				"Replace input file with output", false);
	}

	@Override
	List<String> getRequiredOptions() {
		// either output or replace are required.
		List<String> requiredOptions = super.getRequiredOptions();
		requiredOptions.add(outputOpt);
		requiredOptions.add(replaceOpt);
		return requiredOptions;
	}

	@Override
	public void parse(String[] args) throws ParseException {
		CommandLineParser parser = new GnuParser();
		try {
			cli = parser.parse(options, args);
		} catch (ParseException e) {
			showUsage();
			throw e;
		}
		if (!cli.hasOption(outputOpt) && !cli.hasOption(replaceOpt)) {
			showUsage();
			throw new ParseException(
					"Missing either output path or replace option");
		}
	}

	/**
	 * Get the input path.
	 * 
	 * @return The input path.
	 */
	public String getInputValue() {
		return cli.getOptionValue(inputOpt);
	}

	/**
	 * Get the output path.
	 * 
	 * @return The output path.
	 */
	public String getOutputValue() {
		if (hasOption(outputOpt)) {
			return cli.getOptionValue(outputOpt);
		} else {
			return cli.getOptionValue(inputOpt) + "_tmp_output";
		}
	}

	/**
	 * Replace the input file with the output file on HDFS if the replace option
	 * is set.
	 * 
	 * @param config The Hadoop configuration.
	 * @throws IOException Thrown if any part of the operation fails.
	 */
	public void replaceInputFile(Configuration config) throws IOException {
		if (hasOption(replaceOpt)) {
			FileSystem hdfs = null;
			Path inputPath = new Path(getInputValue());
			Path outputPath = new Path(getOutputValue());
			try {
				hdfs = FileSystem.get(config);
			} catch (IOException ie) {
				LOG.error("Could not access HDFS to replace input file with output file.");
				throw ie;
			}
			boolean isDeleted;
			try {
				isDeleted = hdfs.delete(inputPath, true);
			} catch (IOException ie) {
				LOG.error("Could not delete the input file on HDFS.");
				throw ie;
			}
			if (isDeleted) {
				try {
					hdfs.rename(outputPath, inputPath);
				} catch (IOException ie) {
					LOG.error("Could not rename the output file to the input file on HDFS.");
					throw ie;
				}
			}
		}
	}

}

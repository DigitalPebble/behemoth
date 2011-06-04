package com.digitalpebble.behemoth.cli;


/**
 * A command line processor that includes input and output options intended to
 * be used for paths on HDFS.
 */
public class InputOutputCliProcessor extends CliProcessor {

	String inputOpt;

	String outputOpt;

	/**
	 * Constructor.
	 * 
	 * @param name The task name.
	 * @param usage The task description.
	 */
	public InputOutputCliProcessor(String name, String usage) {
		super(name, usage);
		inputOpt = addRequiredOption("i", "input", "Input path on HDFS", true);
		outputOpt = addRequiredOption("o", "output",
				"Output directory on HDFS", true);
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
		return cli.getOptionValue(outputOpt);
	}

}

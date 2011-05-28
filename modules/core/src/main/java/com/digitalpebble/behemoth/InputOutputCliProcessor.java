package com.digitalpebble.behemoth;

public class InputOutputCliProcessor extends CliProcessor {
	
	String inputOpt;
	
	String outputOpt;
	
	public InputOutputCliProcessor(Class clazz, String description) {
		super(clazz, description);
		inputOpt = addRequiredOption("i", "input",
				"Input path on HDFS", true);
		outputOpt = addRequiredOption("o", "output",
				"Output directory on HDFS", true);
	}
	
	public String getInputValue() {
		return cli.getOptionValue(inputOpt);
	}
	
	public String getOutputValue() {
		return cli.getOptionValue(outputOpt);
	}

}

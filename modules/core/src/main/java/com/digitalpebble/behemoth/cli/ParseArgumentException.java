package com.digitalpebble.behemoth.cli;

import org.apache.commons.cli.ParseException;

/** Thrown if there is a problem parsing an argument at the command line*/
public class ParseArgumentException extends ParseException {

	private static final long serialVersionUID = 8450342378557677079L;

	private String option;
	
	private String value;
	
	/**
	 * Constructor.
	 * 
	 * @param option The option with the value that could not be parsed.
	 * @param value The value.
	 * @param message The exception message. 
	 */
	public ParseArgumentException(String option, String value, String message) {
		super(message);
		this.option = option;
		this.value = value;
	}
	
	/**
	 * Get the option with the argument that could not parsed.
	 * 
	 * @return The option.
	 */
	public String getOption() {
		return option;
	}
	
	/**
	 * Get the value that could not be parsed.
	 * 
	 * @return The value.
	 */
	public String getValue() {
		return value;
	}
 	
}

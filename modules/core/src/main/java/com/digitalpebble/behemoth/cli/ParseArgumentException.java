package com.digitalpebble.behemoth.cli;

import org.apache.commons.cli.ParseException;

public class ParseArgumentException extends ParseException {

	private static final long serialVersionUID = 8450342378557677079L;

	private String option;
	
	private String value;
	
	public ParseArgumentException(String option, String value, String message) {
		super(message);
		this.option = option;
		this.value = value;
	}
	
	public String getOption() {
		return option;
	}
	
	public String getValue() {
		return value;
	}
 	
}

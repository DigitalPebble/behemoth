package com.digitalpebble.behemoth;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.mapred.Reporter;

/** Interface for the GATE or UIMA processors **/

public interface DocumentProcessor extends Configurable {

	/** Returns one or more processed documents**/
	public BehemothDocument[] process(BehemothDocument inputDoc, Reporter reporter);
	
	/** Closes all resources held by the processor **/
	public void close();

}

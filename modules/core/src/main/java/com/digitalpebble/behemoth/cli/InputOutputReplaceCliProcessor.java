package com.digitalpebble.behemoth.cli;

import java.io.IOException;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A command line interface that provides input and output options for paths on
 * HDFS, and a replace option.
 */
public class InputOutputReplaceCliProcessor extends CliProcessor {

    private static final Logger LOG = LoggerFactory
            .getLogger(InputOutputReplaceCliProcessor.class);

    String inputOpt;

    String outputOpt;

    String replaceOpt;

    OptionGroup group = new OptionGroup();

    /**
     * Constructor.
     * 
     * @param name
     *            The task name.
     * @param usage
     *            The task description.
     */
    public InputOutputReplaceCliProcessor(String name, String usage) {
        super(name, usage);
        inputOpt = addRequiredOption("i", "input", "Input path on HDFS", true);
        outputOpt = addGroupOpt("o", "output", true, "Output directory on HDFS");
        replaceOpt = addGroupOpt("r", "replace", false,
                "Replace input file with output");
        group.setRequired(true);
        options.addOptionGroup(group);
    }

    private String addGroupOpt(String shortname, String longname,
            boolean hasarg, String description) {
        Option replaceOpt = new Option(shortname, longname, hasarg, description);
        replaceOpt.setArgName(longname);
        group.addOption(replaceOpt);
        return replaceOpt.getOpt();
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
        return hasOption(outputOpt) ? cli.getOptionValue(outputOpt) : cli
                .getOptionValue(inputOpt) + "_tmp_output";
    }

    /**
     * Replace the input file with the output file on HDFS if the replace option
     * is set.
     * 
     * @param config
     *            The Hadoop configuration.
     * @throws IOException
     *             Thrown if any part of the operation fails.
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

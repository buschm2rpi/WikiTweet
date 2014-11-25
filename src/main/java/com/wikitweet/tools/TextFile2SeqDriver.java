package com.wikitweet.tools;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class TextFile2SeqDriver {
	public static void main(String[] args) throws IOException,
    InterruptedException, ClassNotFoundException {
		
		String URI=args[0];
		String OUTNAME=args[1];

		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("Convert Text");
		job.setJarByClass(Mapper.class);

		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);

		// increase if you need sorting or a special number of files
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);

		TextInputFormat.addInputPath(job, new Path(URI));
		SequenceFileOutputFormat.setOutputPath(job, new Path(OUTNAME));

		// submit and wait for completion
		job.waitForCompletion(true);
	}
}

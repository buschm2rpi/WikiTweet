package com.wikitweet.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.VectorWritable;

public class Cat2SeqDriver {

	public static void main(String[] args) throws IOException,
    InterruptedException, ClassNotFoundException {
		
		String URI=args[0];
		String OUTNAME=args[1];

		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("Convert Categories to Walk Vectors");
		job.setJarByClass(Cat2SeqDriver.class);

		job.setMapperClass(Cat2SeqMapper.class);
		job.setReducerClass(Reducer.class);

		// increase if you need sorting or a special number of files
		job.setNumReduceTasks(0);

		job.setOutputValueClass(VectorWritable.class);
		job.setOutputKeyClass(Text.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		SequenceFileInputFormat.addInputPath(job, new Path(URI));
		SequenceFileOutputFormat.setOutputPath(job, new Path(OUTNAME));

		// submit and wait for completion
		job.waitForCompletion(true);
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.out.println("Done");
	}
}

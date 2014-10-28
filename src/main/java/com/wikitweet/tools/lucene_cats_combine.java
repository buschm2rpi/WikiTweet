/*
 * Copyright (c) 2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wikitweet.tools;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.*;

/**
 * adopted from http://www.chimpler.com
 */
public class lucene_cats_combine {
	
	// for performance, load all of the category data into memory (category data shouldn't be too big)
	public static Map<String, String> readCategories(Configuration conf, Path categoriesPath) {
		Map<String, String> categories = new HashMap<String, String>();
		for (Pair<Text, Text> pair : new SequenceFileIterable<Text, Text>(categoriesPath, true, conf)) {
			categories.put(pair.getFirst().toString(), pair.getSecond().toString()); //
		}
		return categories;
	}
	
	public static void main(String args[]) throws Exception {
		if (args.length != 3) {
			System.err.println("Arguments: [input lucene seq file] [input categories seq file] [output sequence file]");
			return;
		}
		String luceneFileName = args[0];
		String catFileName = args[1];
		String outputDirName = args[2];
		Configuration hconf = new Configuration();
		FileSystem fs = FileSystem.get(hconf);
		
		// Initialize the Sequence file writer
		Writer writer = new SequenceFile.Writer(fs, hconf, new Path(outputDirName + "/chunk-0"),
				Text.class, VectorWritable.class);
		
		// Load the id : categories map
		Map<String, String> categories = readCategories(hconf, new Path(catFileName));
		
		// Read Sequence File;
		SequenceFile.Reader docreader = new SequenceFile.Reader(fs, new Path(luceneFileName), hconf);
		//SequenceFile.Reader catreader = new SequenceFile.Reader(fs, new Path(catpath), hconf);

		// Write new sequence file
		LongWritable key = new LongWritable();
		VectorWritable docValue = new VectorWritable();
		Text newKey = new Text();
		while (docreader.next(key, docValue)) {
			NamedVector entry= (NamedVector) docValue.get();
			String docId = entry.getName().toString();
		    String[] category = categories.get(docId).split(","); 
		    for (int i=0; i < category.length; i++) {
		      if (category[i].equals("")){
		    	  category[i] = "UncategorizedArticle";
		      }
		      newKey.set("/" + category[i] + "/" + docId);
		      writer.append(newKey, docValue);
		      //System.out.println(newKey);
		    }
		}
		docreader.close();
		//catreader.close();
		writer.close();
		System.out.println("Done");
	}
}
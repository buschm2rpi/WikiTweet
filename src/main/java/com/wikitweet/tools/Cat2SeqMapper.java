package com.wikitweet.tools;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.Vector;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Cat2SeqMapper extends Mapper<LongWritable,Text,Text,VectorWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] st = value.toString().toLowerCase().split("\\>",1000);
        if(st.length < 2){System.out.print("parse error: ");System.out.println(value.toString());return;}
        //try{
    	String[] subcatpairs = st[1].split(",",1000);
        //} catch (ArrayIndexOutOfBoundsException e){ System.out.println(value);return;}
    	Vector data = new DenseVector(subcatpairs.length);
        for(int i=0 ; i < subcatpairs.length-1; i++){
        		String[] catsplit = subcatpairs[i].split("[\\s]");
        		//System.out.println(subcatpairs[i]);
        		if(catsplit.length >=3){
        			try{
        				data.set(i,Double.parseDouble(catsplit[2]));
        			} catch (NumberFormatException e) {
        			    //e.printStackTrace();
        			    //System.out.println(catsplit[2]);
        			    //System.out.println(data);
        			    return;
        			}
        		}
        }
        Text keyout = new Text(st[0]);
        //System.out.println(data);
        VectorWritable valueout = new VectorWritable(data);
        context.write(keyout, valueout);
    }
    
    public static void main(String[] args){
    	String value = "Agriculture> Agriculture: 0, Arts: 6, Belief: 10, Business: 6, Chronology: 11, Culture: 7, Education: 7, Environment: 4, Geography: 5, Health: 7, History: 10, Humanities: 7, Language: 9, Law: 9, Life: 5, Mathematics: 8, Nature: 6, People: 12, Politics: 8, Science: 5, Society: 4, Technology: 8,";
    	String[] st = value.toString().toLowerCase().split(">",1000);
    	String[] subcatpairs = st[1].split(",",1000);
        Vector data = new DenseVector(subcatpairs.length);
        for(int i=0 ; i < subcatpairs.length; i++){
        	if(subcatpairs[i].length() > 0){
        		String[] catsplit = subcatpairs[i].split("[\\s]");
        		data.set(i,Double.parseDouble(catsplit[2]));
        		//System.out.println(catsplit[2]);
        	}
        }
        VectorWritable outvalue = new VectorWritable(data);
        //System.out.println(outvalue);
        return;
    	
    }

}

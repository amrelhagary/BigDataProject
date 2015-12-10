package com.crystalball.pair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class PairMain {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Pair");
	    job.setJarByClass(PairMain.class);
	    job.setMapperClass(PairMapper.class);
//	    job.setCombinerClass(PairReducer.class);
	    job.setReducerClass(PairReducer.class);
	    job.setOutputKeyClass(Pair.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

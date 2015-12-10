package com.crystalball.stripe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StripeMain {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Stripe");
	    job.setJarByClass(StripeMain.class);
	    job.setMapperClass(StripeMapper.class);
//	    job.setCombinerClass(PairReducer.class);
	    job.setReducerClass(StripeReducer.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(MapWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    // Delete output if exists
 		FileSystem hdfs = FileSystem.get(conf);
 		if (hdfs.exists(new Path(args[1])))
 			hdfs.delete(new Path(args[1]), true);
 		
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

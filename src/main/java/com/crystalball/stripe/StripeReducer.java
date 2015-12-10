package com.crystalball.stripe;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import com.crystalball.pair.Util;

public class StripeReducer  extends Reducer<Text,MapWritable,Text,Text>{
	int total = 0;
	 public void reduce(Text key, Iterable<MapWritable> stripes,
             Context context
             ) throws IOException, InterruptedException {
		 
		 MapWritable hf = new MapWritable();
		 double total = 0.0;
		 // get the sum
		 Iterator<MapWritable> s = stripes.iterator();
		 while(s.hasNext()) {
			 MapWritable map = s.next();
			 for(Entry<Writable,Writable> entry : map.entrySet()){
				 Text k = (Text) entry.getKey();
				 int v = ((IntWritable) entry.getValue()).get();
				 
				 if(!hf.containsKey(k))
					 hf.put(k, new DoubleWritable(v));
				 else{
					 double value = ((DoubleWritable) hf.get(k)).get();					 
					 hf.put(k, new DoubleWritable(v+value));
				 }
				 total = total + v;
			 }
		 }
		 
		 // calculate relative frequency
		 for(Entry<Writable,Writable> entry : hf.entrySet()){
			 Text k = (Text) entry.getKey();
			 double v =  ((DoubleWritable) hf.get(k)).get();
			 double f = v/total;
			 
			 hf.put(k,new DoubleWritable(f));
		 }
		 
		 // emit
		 context.write(key,Util.MapWritableToText(hf));
	 }
}

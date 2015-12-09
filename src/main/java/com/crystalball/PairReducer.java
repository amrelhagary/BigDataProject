package com.crystalball;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PairReducer extends Reducer<Pair,IntWritable,Pair,FloatWritable>{
    private FloatWritable result = new FloatWritable();
    private int total = 0;
    
    public void reduce(Pair key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
    	  sum += val.get();
      }
      
      if(key.getNeighbour().equals("0")){
    	  total = sum;
      }else{
    	  result.set(sum/total);
    	  context.write(key,result);
      }
    }
}

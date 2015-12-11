package com.crystalball.pair;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.crystalball.common.Pair;

public class PairReducer extends Reducer<Pair,IntWritable,Pair,FloatWritable>{
    private FloatWritable result = new FloatWritable();
    private int total = 0;
    final static Logger logger = Logger.getLogger(PairReducer.class);
    public void reduce(Pair key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
    	  sum += val.get();
      }
      
      if(("0").equals(key.getNeighbour())){

    	  total = sum;
      }else{
    	  result.set(sum/total);
    	  float ret = (float)sum/total;
    	  context.write(key,new FloatWritable(ret));
      }
    }
}

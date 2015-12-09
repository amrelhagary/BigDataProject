package com.crystalball;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PairMapper extends Mapper<Object, Text, Pair, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
//		StringTokenizer itr = new StringTokenizer(value.toString());
//		
//	      while (itr.hasMoreTokens()) {
//	        word.set(itr.nextToken());
//	        context.write(word, one);
//	      }
		
		String[] input = value.toString().split(" ");
		ArrayList<String> neighbours;
		for(int i = 0 ; i < input.length ; i++){
			// find neighbors
			neighbours = Util.getNeighbours(i, input);
			// emit key value
			for(int j=0 ; j < neighbours.size() ; j++){
				context.write(new Pair(input[i],"0"),one);
				context.write(new Pair(input[i],neighbours.get(j)),one);
			}
		}
	}
}

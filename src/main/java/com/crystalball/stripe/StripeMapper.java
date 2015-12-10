package com.crystalball.stripe;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.crystalball.pair.Util;

public class StripeMapper extends Mapper<Object, Text, Text, MapWritable> {

	@Override
	public void map(Object key, Text doc, Context context) throws IOException, InterruptedException{
		String[] input = doc.toString().split(" ");
		ArrayList<String> neighbours;
		
		for(int i = 0 ; i < input.length ; i++ ){
			neighbours = Util.getNeighbours(i, input);
			MapWritable h = new MapWritable();
			for(int j=0 ; j < neighbours.size() ; j++){
				Text neighbour = new Text(neighbours.get(j));
				if(!h.containsKey(neighbours.get(j))){
					h.put(neighbour, new IntWritable(1));
				}else{
					int cnt = ((IntWritable)h.get(neighbour)).get();
					cnt =+ cnt;
					h.put(neighbour, new IntWritable(cnt));
				}
			}
			context.write(new Text(input[i]), h);
		}
	}
}

package com.crystalball.hybrid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.crystalball.common.Pair;
import com.crystalball.common.Util;

public class HybridMapper extends Mapper<Object, Text, Pair, IntWritable>{
	
	private static Map<Pair,Integer> map;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		map = new HashMap<Pair,Integer>();
	}
	
	@Override
	public void map(Object key, Text doc, Context context){
		
		String[] lines = doc.toString().split(System.getProperty("line.separator"));
		for (String line : lines) {
			String[] input = line.toString().split(" ");
			ArrayList<String> neighbours;

			for (int i = 0; i < input.length - 1; i++) {
				// find neighbors
				neighbours = Util.getNeighbours(i, input);
				// emit key value
				for (int j = 0; j < neighbours.size(); j++) {

					Pair p = new Pair(input[i], neighbours.get(j));
					if (!map.containsKey(p))
						map.put(p, 1);
					else {
						int v = map.get(p);
						map.put(p, v += 1);
					}
				}
			}
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		
		for(Pair p : map.keySet()){
			context.write(p,new IntWritable(map.get(p)));
		}
		
		super.cleanup(context);
	}
}

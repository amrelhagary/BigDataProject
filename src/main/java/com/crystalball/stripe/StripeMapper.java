package com.crystalball.stripe;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StripeMapper extends Mapper<Object, Text, Text, MapWritable> {

	@Override
	public void map(Object key, Text doc, Context context) throws IOException, InterruptedException{

		String[] lines = doc.toString().split(System.getProperty("line.separator"));
		for (String line : lines) {
			String[] input = line.toString().split(" ");			

			for (int i = 0; i < input.length - 1; i++) {
				String currentTerm = input[i];
				MapWritable stripes = new MapWritable();
				for (int j = i+1; j < input.length; j++) {
					if (currentTerm.equals(input[j]))
						break;
					Text curNeighbor = new Text(input[j]);
					if (stripes.containsKey(curNeighbor)) {
						int counter = ((IntWritable)stripes.get(curNeighbor)).get();
						counter++;
						stripes.put(curNeighbor, new IntWritable(counter));
					} else {
						stripes.put(curNeighbor, new IntWritable(1));
					}
				}
				context.write(new Text(currentTerm), stripes);
			}

		}
	}
}
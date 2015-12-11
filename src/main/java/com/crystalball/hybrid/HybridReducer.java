package com.crystalball.hybrid;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.crystalball.common.Pair;
import com.crystalball.common.Util;

public class HybridReducer extends Reducer<Pair, IntWritable, Text, Text> {

	private static MapWritable map;
	private int marginal;
	private String currentTerm;
	final static Logger logger = Logger.getLogger(HybridReducer.class);

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		map = new MapWritable();
		marginal = 0;
		currentTerm = null;
	}

	public void reduce(Pair pair, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException {
		// check the current term
		if (currentTerm == null)
			currentTerm = pair.getWord();
		else if (!currentTerm.equals(pair.getWord())) {
				for (Writable p : map.keySet()) {
					int value = ((IntWritable) map.get(p)).get();
					map.put(p, new DoubleWritable((double) value / marginal));
				}
				context.write(new Text(currentTerm), Util.MapWritableToText(map));
				// reset for new term
				marginal = 0;
				map = new MapWritable();
				currentTerm = pair.getWord();
		}
		
		if(!map.containsKey(pair.getNeighbour())){
			map.put(new Text(pair.getNeighbour()), new IntWritable(0));
		}

		for (IntWritable c : counts){			
			int	value = ((IntWritable) map.get(new Text(pair.getNeighbour()))).get() + c.get();
			map.put(new Text(pair.getNeighbour()), new IntWritable(value));
			marginal = marginal + c.get();
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		
		for (Writable p : map.keySet()) {
			int value = ((IntWritable) map.get(p)).get();
			map.put(p, new DoubleWritable((double) value / marginal));
		}
		context.write(new Text(currentTerm), Util.MapWritableToText(map));
		
		super.cleanup(context);
	}
}
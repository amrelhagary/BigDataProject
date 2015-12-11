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

import com.crystalball.common.Util;

public class StripeReducer  extends Reducer<Text,MapWritable,Text,Text>{		 
	 @Override
	public void reduce(Text term, Iterable<MapWritable> stripesList, Context context)
			throws IOException, InterruptedException {
		MapWritable listTermNeighbor = new MapWritable();
		Iterator<MapWritable> listStripes = stripesList.iterator();
		double stripeTotal = 0.0;
		while (listStripes.hasNext()) {
			MapWritable stripe = listStripes.next();

			for (Entry<Writable, Writable> entry : stripe.entrySet()) {
				Text curNeighbor = (Text)entry.getKey();
				if (listTermNeighbor.containsKey(curNeighbor)) {				
					double counter = ((DoubleWritable)listTermNeighbor.get(curNeighbor)).get();
					stripeTotal += counter;
					counter++;
					listTermNeighbor.put(curNeighbor, new DoubleWritable(counter));
				} else {
					int curVal = ((IntWritable)entry.getValue()).get();
					listTermNeighbor.put(curNeighbor, new DoubleWritable(curVal));
					stripeTotal += curVal;
				}
			}
		}

		for (Entry<Writable,Writable> entry : listTermNeighbor.entrySet()) {
			double curVal = ((DoubleWritable)entry.getValue()).get();
			double frequencies = curVal/stripeTotal;			
			entry.setValue(new DoubleWritable(frequencies));
		}

		// emit
		context.write(term, Util.MapWritableToText(listTermNeighbor));
	}

}
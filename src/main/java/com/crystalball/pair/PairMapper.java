package com.crystalball.pair;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class PairMapper extends Mapper<Object, Text, Pair, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    final static Logger logger = Logger.getLogger(PairMapper.class);

	@Override
	public void map(Object key, Text doc, Context context) throws IOException, InterruptedException{
		
//		String[] lines = doc.toString().split(System.getProperty("line.seperator"));
//		logger.info(lines.toString());
//		for(String line : lines){
			String[] input = doc.toString().split(" ");
			ArrayList<String> neighbours;
			
			for(int i = 0 ; i < input.length-1 ; i++){
				
				// find neighbors
				neighbours = Util.getNeighbours(i, input);
				// emit key value
				for(int j=0 ; j < neighbours.size() ; j++){
					context.write(new Pair(input[i],"0"),one);
					context.write(new Pair(input[i],neighbours.get(j)),one);
				}
			}
//		}
	}
}

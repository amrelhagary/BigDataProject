package com.crystalball.common;

import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Util {
	public static ArrayList<String> getNeighbours(int index,String[] items){
		// get the index for the term
		ArrayList<String> neighbours = new ArrayList<String>();
		for(int i = index+1 ; i < items.length ; i++){
			if(!items[i].equals(items[index]))
				neighbours.add(items[i]);
			else
				break;
		}
		return neighbours;
		
	}
	
	public static Text MapWritableToText(MapWritable map){
		String str = "[";
		 for(Entry<Writable,Writable> entry : map.entrySet()){
			 Text k = (Text) entry.getKey();
			 double v =  ((DoubleWritable) map.get(k)).get();
			 str += "("+k +" , "+ v +"),";
		 }
		 str += "]";
		 
		Text t = new Text(str);
		return t;
	}
}

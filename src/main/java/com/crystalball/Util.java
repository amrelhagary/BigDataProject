package com.crystalball;

import java.util.*;

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
}

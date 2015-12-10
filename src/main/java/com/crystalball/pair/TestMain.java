package com.crystalball.pair;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class TestMain {
	public static void main(String[] args){
		String[] list = {"34","56","29","12","34","56","92","29","34","12","",""};
		ArrayList<String> neighbours;
		Map<Pair,Integer> output = new TreeMap<Pair,Integer>();
		for(int i = 0 ; i < list.length-1 ; i++){
			// find neighbors
			neighbours = Util.getNeighbours(i, list);
			// emit key value
			for(int j=0 ; j < neighbours.size() ; j++){
				output.put(new Pair(list[i],"0"),1);
				output.put(new Pair(list[i],neighbours.get(j)),1);
			}
		}
		
		for(Map.Entry<Pair,Integer> entry : output.entrySet()){
			System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
		}
	}
}

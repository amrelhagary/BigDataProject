package com.crystalball;

import java.util.*;

public class Pair {
	private String word;
	private String neighbour;
	
	public Pair(String w , String n){
		this.word = w;
		this.neighbour = n;
	}
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}
	public String getNeighbour() {
		return neighbour;
	}
	public void setNeighbour(String neighbour) {
		this.neighbour = neighbour;
	}
}

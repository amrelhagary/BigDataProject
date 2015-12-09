package com.crystalball;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair>,Writable{
	
	private String word;
	private String neighbour;
	
	public Pair(){ }
	
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
	
	
	public void readFields(DataInput in) throws IOException {
		this.word = in.readLine();
		this.neighbour = in.readLine();
		
	}
	public void write(DataOutput out) throws IOException {
		out.writeChars(word);
		out.writeChars(neighbour);
		
	}
	public int compareTo(Pair o) {
		int compare = this.word.compareTo(o.word);
		if( compare == 0)
			return this.neighbour.compareTo(o.neighbour);
		else
			return compare;
	}
	
	@Override
	public String toString(){
		return word+" , "+neighbour;
	}
}

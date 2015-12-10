package com.crystalball.pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

public class Pair implements WritableComparable<Pair>{
	
	private String word;
	private String neighbour;
	final static Logger logger = Logger.getLogger(PairMapper.class);
	
	public Pair(){
	}
	
	public Pair(String w , String n) {
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
		this.word = in.readUTF();
		this.neighbour = in.readUTF();
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(word);
		out.writeUTF(neighbour);
		
	}
	public int compareTo(Pair o) {
		if( this.word.equals(o.word)){
			return this.neighbour.compareTo(o.neighbour);
		}
		else
			return this.word.compareTo(o.word);
		
	}
	@Override
	public String toString(){
		return word+" , "+neighbour;
	}
}

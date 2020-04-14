package Akanksha_Verma;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 Mapper function 2:
 Reads the input from the first set of Map Reduce function
 Tokenizes each line and inputs them , and
 * Maps the Drug-Drug as Key to Enzyme as Value
 * 
 **/
@SuppressWarnings("deprecation")
public class DrugEnzyme_Mapper2 extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>
{
	
	
	@Override
	//Map function passes the Key , Value and OutputCollector pair
	public void map(LongWritable Key, Text Value,
			OutputCollector<Text, IntWritable> Output, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		
		String line = Value.toString(); //Line to store input read from input
		//Text 
		Text EnzymeId= new Text(); 
		Text DrugPair=new Text();
		
		//tokenizes the string
		StringTokenizer tokenizer = new StringTokenizer(line,"\t");
		IntWritable one = new IntWritable(1);
		
		/*
		 * While there are token A pair of token in a given line
		 * is initialized in as Drug and corresponding enzyme
		 */
		
		while(tokenizer.hasMoreTokens()){ //Loops through the tokenized line
		 DrugPair.set(tokenizer.nextToken()); //reads off the DrugPair
		 EnzymeId.set(tokenizer.nextToken()); //reads of the corresponding enzyme
		}
		
		//Final pair is collected by mapper for further processing 
		Output.collect(DrugPair, one);
		
		}
	}





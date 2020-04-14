package Akanksha_Verma;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


/**
 * Reducer # 2 Function
 *Takes input processed from the mapper function
 *iterates through them and then reduces the data
 *to unique key present and counts and aggrgates the common number of enzyme
 *output: Drug-Drug <-> # of common enzyme shared by each drug in the drug pair
 */
public class DrugEnzyme_Reducer2 extends MapReduceBase implements Reducer <Text, IntWritable, Text, IntWritable>
{

	@Override
	public void reduce(Text DrugPairs, Iterator<IntWritable> EnzymeCount,
            OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		int Common_Enzymes_Count =0; //Counter to keep count of common enzymes 
		
		while(EnzymeCount.hasNext()){ //loops till all the common enzyme for a drug drug pair are found
			IntWritable Count =  (IntWritable)EnzymeCount.next(); // assigns the count to the variable
			Common_Enzymes_Count = Common_Enzymes_Count + Count.get(); //increments count for each common enzyme
		} 
		
		//Collects the unique Drug Drug pair and total number of enzymes shared
		output.collect(DrugPairs, new IntWritable(Common_Enzymes_Count)); 
		
		//CALCULATING THE # OF EDGES
		Grapher.total++; //Adds up the number of drug-drug pairs 
		
	
	}

}

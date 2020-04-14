package Akanksha_Verma;

import java.io.IOException;
import java.util.*;	

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


//Class with main
public class DrugEnzyme {

/*
 * Mapper class : Takes output from Project 2 part 1 and maps it
 * with EnzymeId as key and all the drugs that react with it
 * (Enzymeid, DrugId) pairs mapped
 */
	public static class DrugEnzyme_Mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>
	{	//Assigning Text type variables
		private Text EnzymeId = new Text();
		private Text DrugId = new Text();
		
		@Override
		//Mapper function
		public void map(LongWritable key, Text DrugId,
				OutputCollector<Text,Text> Output, Reporter reporter)
				throws IOException {
				String line = DrugId.toString(); //Read input into string
				 
				//tokenizes the string into Drug and Enzyme id by ":" Delimiter
				StringTokenizer tokenizer = new StringTokenizer(line,":"); 
				
				//While loop to iterate through the strings and tokenize into Drug
				//and enzyme id
				while(tokenizer.hasMoreTokens()){
					DrugId.set(tokenizer.nextToken());
					EnzymeId.set(tokenizer.nextToken());
					
					//Adds to a global Grapher class to keep count of Drug Nodes
					Grapher.list.add(DrugId.toString()); 
				}
				
				
				Output.collect(EnzymeId,DrugId);//Collects and maps the Drug and Enzyme pair
				
				
			}

	}
			
/*Reducer Function 1:
 * Function to Reduce the mapped pairs from Mapper 1
 * Removes redundant data 
 * Reduces and appends into a string all the Drugs that react with one enzyme
 * Those drugs are then paired using a nested for loop to make all possible combination 
 * of drugs.
 */
@SuppressWarnings("deprecation")
public static class DrugEnzyme_Reducer extends MapReduceBase implements Reducer<Text,Text,Text,Text>
{
	@Override
	public void reduce(Text Key, Iterator<Text> DrugId,OutputCollector<Text, Text > Output, Reporter reporter) throws IOException {
		
			// TODO Auto-generated method stub
		String temp = new String();
		StringBuilder associatedDrugsId = new StringBuilder(); //String to hold all the drugs
		Text EnzymeId = Key; //Assigning the key value to EnzymeId variable
		Text DrugPair = new Text();  //Creates new text in memory
		
		
			 while(DrugId.hasNext()){ //While loop to iterate through drug values as mapped
				 Text inputdrugid = DrugId.next(); //Initialized the next drug in list
				 associatedDrugsId.append(inputdrugid.toString()); //appends to list of drugs against 1 enzyme
				 
			}
			 
			 String[] Drugtokens = associatedDrugsId.toString().split(" "); //Splits the list of drugs by spaces
			
			 /*
			  * Double loop to make all possible drug pair combinations
			  */
			for(int i=0;i<Drugtokens.length;i++){ 
				for(int k=i+1;k<Drugtokens.length;k++){
					temp=Drugtokens[i]+"<->"+Drugtokens[k]; //Creates the drug pair string
					Output.collect(new Text(temp),EnzymeId); //Collects and remaps the reduces Drug-drug pair as key and EnzymeId as value
					
				}
			}
			
			
	}
	
	
}

/*
 * Main Function:
 * Calls the Jobs:
 * Job(1) Call 1st set of MapReduce
 * Job(2) Call 2nd Set of Mapreduce
 * Output the number of Drug nodes and edges
 */
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
	
		
		if(args.length == 0 || args.length<2){
			System.out.println("Usage: java -jar Name_of_file.jar <path to input> <path to output>");
		}
		
		
	//Job: Map Reduce Step 1	
		JobConf conf = new JobConf(DrugEnzyme.class); //initalize the Job to class with main
		conf.setJobName("DrugEnzymeMapReduce"); //naming the job
		
		//Specifies the Type of input being read and outputted
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setJarByClass(DrugEnzyme.class);
		conf.setMapperClass(DrugEnzyme_Mapper.class); //specifies the mapper class
		conf.setReducerClass(DrugEnzyme_Reducer.class); //specifies the reducer class
		
		conf.setInputFormat(TextInputFormat.class); //specifies the input format of data
		conf.setOutputFormat(TextOutputFormat.class); //specifies output format of data
		
		FileInputFormat.setInputPaths(conf, new Path(args[0])); //Initializes input path with command line arg
		
		// Establishes a Filesystem object to access various files being used
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path("temp"))){ //Checks if file is already present
			fs.delete(new Path("temp"),true); //Deletes file if file is already present
		}
		
		FileOutputFormat.setOutputPath(conf, new Path("temp"));  //Creates new file
		JobClient.runJob(conf); //runs first job 
		
//Job: MapReduce Step 2 : from Drug-Drug Enzyme to Unique Drug-drug enzymes

		JobConf conf2 = new JobConf(); //Initializes a new Job
		conf2.setMapperClass(DrugEnzyme_Mapper2.class); // Initializes the mapper function
		conf2.setReducerClass(DrugEnzyme_Reducer2.class); //initializes a reduceer function
		
		conf2.setMapOutputKeyClass(Text.class); //Data type for Output Key
		conf2.setMapOutputValueClass(IntWritable.class); //Data type for output Value
		
		
		FileInputFormat.setInputPaths(conf2, new Path("temp")); //Input read from a temp folder
		
		if(fs.exists(new Path(args[1]))){ //checks if output folder is present
			fs.delete(new Path(args[1])); //deletes it to create new one
		}
		FileOutputFormat.setOutputPath(conf2, new Path(args[1])); //Creates output folder
		conf.setJarByClass(DrugEnzyme.class);
		JobClient.runJob(conf2); //runs 2nd part of Map and reduce
		
		/*
		 * Output: The number of edges and drugnodes mapped and reduced
		 */
		System.out.println("Summary Output: #Nodes, #Edges");
		System.out.println("---------------------------------");
		System.out.println("# of DrugNodes: "+Akanksha_Verma.Grapher.list.size());
		System.out.println("# of Edges: "+Akanksha_Verma.Grapher.total+"\n");
		
	
		}
	}



		
	

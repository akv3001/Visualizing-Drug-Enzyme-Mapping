/**
 * 
 */
package Akanksha_Verma;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;

/**
 *Class structure to keep track of # of Drug Nodes 
 *and Number of Edges present  when the data is organized
 */
public class Grapher {
	//Stores each unique Drug node : avoids repeats by using a hashset
	public static Set<String> list = new HashSet<String>();
	
	//global int keeps track on the number of drug pairs ( used in the DrugEnzyme_Reducer2)
	public static int total = 0;
}

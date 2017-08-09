import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AuthorCountReducer extends Reducer<Text,Text,Text,Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		ArrayList<String> authors = new ArrayList<String>();
		ArrayList<String> words = new ArrayList<String>();
		
		//count the number of authors and store unique words
		for(Text value: values)
		{
			String[] vals = value.toString().split(" ");
			if(!authors.contains(vals[1]))
				authors.add(vals[1]);
			if(!words.contains(vals[0]))
				words.add(vals[0]);
		}
		
		//print output in format <unique word> <total # of authors>
		for(int i = 0; i < words.size(); i++)
		{
			String temp = (" " + authors.size()) + " AUTHCOUNT";
			context.write(new Text(words.get(i)), new Text(temp));
		}
	}
}

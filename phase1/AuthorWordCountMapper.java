import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class AuthorWordCountMapper extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		//variables for author and the sentence.
		String author;
		String sentence;
		
		//split the line into author date and sentence fields.
		String[] lineInfo = value.toString().split("<===>");
		
		//extract authors last name
		String authorFull = lineInfo[0];
		authorFull = authorFull.trim();
		String[] authTemp = authorFull.split(" ");
		author = authTemp[(authTemp.length) -1].trim();
		author = author.toLowerCase();
		
		//prepare and split the sentence for parsing.
		sentence = lineInfo[2];
		sentence = sentence.trim();
		String[] words = sentence.split(" ");
		
		//iterate through each word and send the key/value pair.
		for(int i = 0; i < words.length; i++)
		{
			String word = words[i];
			word = word.trim();
			word = word.replaceAll("[^A-Za-z0-9]", "");
			word = word.toLowerCase();
			if(word.length() != 0)
			{
				context.write(new Text(word), new Text(author));
			}
		}
	}
}
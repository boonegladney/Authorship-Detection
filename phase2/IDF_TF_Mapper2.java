import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class IDF_TF_Mapper2 extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		//split the value
		String[] values = value.toString().split(" ");
				
		// clean up the values
		for(int i = 0; i < values.length; i++)
		{
			values[i].replace("\t", "");
			values[i] = values[i].trim();
		}
		
		// send key/value pair to reducer
		String out = values[1] + " " + values[2] + " " + values[3];
		context.write(new Text(values[0]), new Text(out));
	}

}

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MainClass {
	public static void main(String[] args) throws IOException, ClassNotFoundException,
	InterruptedException {
		
		Configuration conf = new Configuration();
		
		//JOB 1
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(MainClass.class);
		
		job1.setMapperClass(IDF_AC_Mapper.class);
		job1.setMapperClass(IDF_AWC_Mapper.class);
		job1.setReducerClass(IDF_Reducer.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		//job1.setInputFormatClass(TextInputFormat.class);
		//job1.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, IDF_AC_Mapper.class);
		MultipleInputs.addInputPath(job1, new Path(args[2]), TextInputFormat.class, IDF_AWC_Mapper.class);
		FileOutputFormat.setOutputPath(job1, new Path(args[3]));
		
		job1.waitForCompletion(true);
		
		//JOB 2
		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(MainClass.class);
		
		job2.setMapperClass(MaxIndexMapper.class);
		job2.setReducerClass(MaxIndexReducer.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job2, new Path(args[3]));
		FileOutputFormat.setOutputPath(job2, new Path(args[4]));
		
		job2.waitForCompletion(true);
		
		//JOB 3
		Job job3 = Job.getInstance(conf);
		job3.setJarByClass(MainClass.class);
		
		job3.setMapperClass(IDF_TF_Mapper1.class);
		job3.setMapperClass(IDF_TF_Mapper2.class);
		job3.setReducerClass(IDF_TF_Reducer.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		
		//job1.setInputFormatClass(TextInputFormat.class);
		//job1.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleInputs.addInputPath(job3, new Path(args[0]), TextInputFormat.class, IDF_TF_Mapper1.class);
		MultipleInputs.addInputPath(job3, new Path(args[4]), TextInputFormat.class, IDF_TF_Mapper2.class);
		FileOutputFormat.setOutputPath(job3, new Path(args[5]));
		
		job3.waitForCompletion(true);
		
		//JOB 4
		
		Job job4 = Job.getInstance(conf);
		job4.setJarByClass(MainClass.class);
		
		job4.setMapperClass(AAVMapper.class);
		job4.setReducerClass(AAVReducer.class);
		
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job4, new Path(args[5]));
		FileOutputFormat.setOutputPath(job4, new Path(args[6]));
		
		job4.waitForCompletion(true);
		System.exit(1);
	}
}

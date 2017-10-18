import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Q4 {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		HashSet<String> bus_info = new HashSet<String>();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String business_info = conf.get("business"); 
			Path info = new Path(business_info);
			
			FileSystem file_sys = FileSystem.get(conf);
			FileStatus[] file_stat = file_sys.listStatus(info);
			for (FileStatus status : file_stat) {
				Path info2 = status.getPath();

				BufferedReader buf = new BufferedReader(new InputStreamReader(file_sys.open(info2)));
				String input = buf.readLine();
				while (input != null) {

					String[] str = input.split("::");
					if (str.length > 2 && str[1].contains("Palo Alto")) {

						bus_info.add(str[0]);
					}
					input = buf.readLine();
				}

			}
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

			String[] line = value.toString().split("::");
			if (line.length > 3 && bus_info.contains(line[2])) {

				context.write(new Text(line[1]), new Text(line[3]));
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); 
		if (otherArgs.length < 2) {
			System.err.println("error");
			System.exit(2);
		}
		
		conf.set("business", otherArgs[1]);

		Job job = new Job(conf, "User rating");
		job.setJarByClass(Q4.class);

		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[2]))) {
			fs.delete(new Path(args[2]), true);
		}

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	
}
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




public class TopTen {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {

			String bus_rev[] = value.toString().split("::");
		
			context.write(new Text(bus_rev[2]), new Text(bus_rev[3]));

		}
	} 
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
			
			
		private TreeMap<String, Float> map1 = new TreeMap<>();
				
		private RComparator rComparator = new RComparator(map1);
			
		private TreeMap<String, Float> map2 = new TreeMap<String, Float>(rComparator);
			
			@Override
			protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

				float sumOfRatings = 0;
				float countOfRatings = 0;
			
				for (Text value : values) {
					sumOfRatings += Float.parseFloat(value.toString());
					countOfRatings++;
				}
			
				float avgerageRating = new Float(sumOfRatings / countOfRatings);
				map1.put(key.toString(), avgerageRating);
			}

			class RComparator implements Comparator<String> {

				TreeMap<String, Float> treemap1;

				public RComparator(TreeMap<String, Float> treemap2) {
					this.treemap1 = treemap2;
				}

				public int compare(String a, String b) {
					if (treemap1.get(a) >= treemap1.get(b)) {
						return -1;
					} else {
						return 1;
					}
				}
			}

			@Override
			protected void cleanup(Context context) throws IOException, InterruptedException {
				map2.putAll(map1);

				int count = 10;
				for (Entry<String, Float> e1 : map2.entrySet()) {
					if (count == 0) {
						break;
					}
					context.write(new Text(e1.getKey()),new Text(String.valueOf(e1.getValue())));
					count--;
				}
			}
	}  
	
	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {

			String data = value.toString().trim();
			String[] info = data.split("\t");
			String b_id = info[0].trim();
			String b_rate = info[1].trim();
			context.write(new Text(b_id), new Text("T1|" + b_id + "|" + b_rate));

		}
	} 
	
	public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String b_data[] = value.toString().split("::");
			context.write(new Text(b_data[0].trim()), new Text("T2|"
					+ b_data[0].trim() + "|" + b_data[1].trim()
					+ "|" + b_data[2].trim()));

		}
	} 	
	
		public static class Reduce2 extends Reducer<Text, Text, Text, Text> {

		private ArrayList<String> top_ten = new ArrayList<String>();
		private ArrayList<String> b_info = new ArrayList<String>();
		private static String delim = "\\|";

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			for (Text line : values) {
				String val = line.toString();
				if (val.startsWith("T1")) {
					top_ten.add(val.substring(3));
				} else {
					b_info.add(val.substring(3));
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (String top_bus : top_ten) {
				for (String info : b_info) {
					String[] t1 = top_bus.split(delim);
					String t1_bid = t1[0].trim();

					String[] t2 = info.split(delim);
					String t2_bid = t2[0].trim();

					if (t1_bid.equals(t2_bid)) {
						context.write(new Text(t1_bid), new Text(
								t2[1] + "\t" + t2[2] + "\t"
										+ t1[1]));
						break;
					}
				}
			}
		}
	} 
	public static void main(String[] args) throws IOException,ClassNotFoundException, InterruptedException {

		Configuration config1 = new Configuration();
		String[] otherArgs = new GenericOptionsParser(config1, args).getRemainingArgs();

		if (otherArgs.length != 4) {
			System.err.println("Arguments insufficient");
			System.exit(0);
		}

		Job job1 = Job.getInstance(config1, "JOB1");
		job1.setJarByClass(TopTen.class);
		job1.setMapperClass(Map.class);
		job1.setReducerClass(Reduce.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));

		boolean iscomplete = job1.waitForCompletion(true);

		if (iscomplete) {
			Configuration config2 = new Configuration();
			Job job2 = Job.getInstance(config2, "JOB2");
			job2.setJarByClass(TopTen.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);

			MultipleInputs.addInputPath(job2, new Path(args[2]),
		    TextInputFormat.class, Map2.class);
	        MultipleInputs.addInputPath(job2, new Path(args[1]),
		    TextInputFormat.class, Map3.class);

	        job2.setReducerClass(Reduce2.class);
	        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

	        job2.waitForCompletion(true);
		}
	}


}
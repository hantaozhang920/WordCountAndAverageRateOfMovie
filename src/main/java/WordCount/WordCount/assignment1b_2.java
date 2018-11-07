package WordCount.WordCount;
//MovieAvgRating.java


//import FloatTupleWritable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.*;
import java.util.Map.Entry;
public class assignment1b_2 {
	 public static class Map extends Mapper<LongWritable, Text, Text,FloatTupleWritable> {

	        @Override
	        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	            String delims = "^";
	            String[] businessData = StringUtils.split(value.toString(), delims);

	            if (businessData.length == 4) {
	                context.write(new Text(businessData[1]), new FloatTupleWritable(
	                        new FloatWritable(0),
	                        new FloatWritable(Float.valueOf(businessData[2]))));
	            }
	        }
	    }
	 public static class Reduce extends Reducer<Text,FloatTupleWritable,Text,FloatWritable> {
	        static private java.util.Map<Text, FloatWritable> countMap = new HashMap<Text, FloatWritable>();
	        static int topn = 0;

	        @Override
	        protected void setup(Context context) throws IOException, InterruptedException {
	            Configuration config = context.getConfiguration();
	            Reduce.topn = config.getInt("assignment1b_2.topn", 10);
	        }

	        @Override
	        public void reduce(Text key, Iterable<FloatTupleWritable> values,Context context)
	                throws IOException, InterruptedException {
	            int count = 0;
	            float total = 0;
	            for(FloatTupleWritable t : values) {
	                count += t.getFst().get();
	                total += t.getSnd().get();
	            }
	            Reduce.countMap.put(new Text(key), new FloatWritable(total / count));
	        }
	        @Override
	        protected void cleanup(Context context) throws IOException, InterruptedException {
	            java.util.Map<Text, FloatWritable> sortedMap = sortByValues(Reduce.countMap);
	            int counter = 0;
	            for (Text key : sortedMap.keySet()) {
	                if (counter++ == Reduce.topn)
	                    break;
	                context.write(key, sortedMap.get(key));
	            }
	        }
	    }

	    /**
	     * The combiner retrieves every word and puts it into a Map: if the word already exists in the
	     * map, increments its value, otherwise sets it to 1.
	     */
	 public static class Combiner extends Reducer<Text,FloatTupleWritable,Text,FloatTupleWritable> {
	        @Override
	        public void reduce(Text key, Iterable<FloatTupleWritable> values,Context context)
	                throws IOException, InterruptedException {
	            int count = 0;
	            float total = 0;
	            for(FloatTupleWritable t : values) {
	                count += t.getFst().get();
	                total += t.getSnd().get();
	            }
	            context.write(new Text(key), new FloatTupleWritable(
	                    new FloatWritable(count), new FloatWritable(total)));
	        }
	    }
	 /*
	    * sorts the map by values. Taken from:
	    * http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html
	    */
	 
	    private static <K extends Comparable, V extends Comparable> java.util.Map<K, V> sortByValues(java.util.Map<K, V> map) {
	        List<java.util.Map.Entry<K, V>> entries = new LinkedList<Entry<K, V>>(map.entrySet());

	        Collections.sort(entries, new Comparator<java.util.Map.Entry<K, V>>() {

	            public int compare(java.util.Map.Entry<K, V> o1, java.util.Map.Entry<K, V> o2) {
	                return o2.getValue().compareTo(o1.getValue());
	            }
	        });

	        //LinkedHashMap will keep the keys in the order they are inserted
	        //which is currently sorted on natural ordering
	        java.util.Map<K, V> sortedMap = new LinkedHashMap<K, V>();

	        for (java.util.Map.Entry<K, V> entry : entries) {
	            sortedMap.put(entry.getKey(), entry.getValue());
	        }

	        return sortedMap;
	    }
	    //
	    //
	    //
	    //FloatTupleWritable//
	    //
	    //
	    //
	    public static class FloatTupleWritable implements Writable {

	        private FloatWritable fst;
	        private FloatWritable snd;

	        public FloatTupleWritable() {
	            set(new FloatWritable(), new FloatWritable());
	        }
	  

			public FloatTupleWritable(FloatWritable fst, FloatWritable snd) {
	            this.fst = fst;
	            this.snd = snd;
	        }


	       	public FloatWritable getSnd() {
				// TODO Auto-generated method stub
				
				return snd;
			}

			public FloatWritable getFst() {
				// TODO Auto-generated method stub
				
				return fst;
			}

	   

	        public void set(FloatWritable fst, FloatWritable snd) {
	            this.fst = fst;
	            this.snd = snd;
	        }

	        public void write(DataOutput out) throws IOException {
	            fst.write(out);
	            snd.write(out);
	        }

	        public void readFields(DataInput in) throws IOException {
	            fst.readFields(in);
	            snd.readFields(in);
	        }

	        @Override
	        public int hashCode() {
	            return fst.hashCode() * 163 + snd.hashCode();
	        }

	        @Override
	        public boolean equals(Object obj) {
	            if (obj instanceof FloatTupleWritable) {
	                FloatTupleWritable it = (FloatTupleWritable) obj;
	                return fst.equals(it.fst) && snd.equals(it.snd);
	            }
	            return false;
	        }

	        @Override
	        public String toString() {
	            return  fst + "\t" + snd;
	        }
	    }


	 // Driver program
	    public static void main(String[] args) throws Exception {
	        Configuration conf = new Configuration();
	        
	        //conf.addResource("configuration/configuration-2.xml");

	        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
	        if (otherArgs.length != 2) {
	            System.err.println("Usage: assignment1b.assigment1b_2 <in> <out>");
	            System.exit(2);
	        }

	        Job job = Job.getInstance(conf, "ReviewCount");
	        job.setJarByClass(assignment1b_2.class);

	        job.setMapperClass(Map.class);
	        job.setReducerClass(Reduce.class);
	        job.setCombinerClass(Combiner.class);

	        // set output key type
	        job.setOutputKeyClass(Text.class);

	        // set output value type
	        job.setMapOutputValueClass(FloatTupleWritable.class);
	        job.setOutputValueClass(Text.class);


	        //set the HDFS path of the input data
	        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	        // set the HDFS path for the output
	        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	        //Wait till job completion
	        System.exit(job.waitForCompletion(true) ? 0 : 1);
	    }
}

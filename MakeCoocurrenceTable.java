import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * CS61c project 1, Fall 2012.
 * Reminder:  DO NOT SHARE CODE OR ALLOW ANOTHER STUDENT TO READ YOURS.
 * EVEN FOR DEBUGGING. THIS MEANS YOU.
 */
public class Proj1 {

    /** An Example Writable which contains two String Objects. */
    public static class StringPair implements Writable {
        /** The String objects I wrap. */
        private String a, b;

        /** Initializes me to contain empty strings. */
        public StringPair() {
	        a = b = "";
        }
	
        /** Initializes me to contain A, B. */
        public StringPair(String a, String b) {
            this.a = a;
	        this.b = b;
        }

        /** Serializes object - needed for Writable. */
        public void write(DataOutput out) throws IOException {
            new Text(a).write(out);
	        new Text(b).write(out);
        }

        /** Deserializes object - needed for Writable. */
        public void readFields(DataInput in) throws IOException {
	        Text tmp = new Text();
	        tmp.readFields(in);
	        a = tmp.toString();
	    
	        tmp.readFields(in);
	        b = tmp.toString();
        }

	    /** Returns A. */
	    public String getA() {
	        return a;
	    }
	    /** Returns B. */
	    public String getB() {
	        return b;
	    }
    }

    /** An Example Writable which contains two Double Objects. */
    public static class DoublePair implements Writable {
        /** The String objects I wrap. */
        private Double a, b;

        /** Initializes me to contain empty strings. */
        public DoublePair() {
            a = b = 0.0;
        }
    
        /** Initializes me to contain A, B. */
        public DoublePair(Double a, Double b) {
            this.a = a;
            this.b = b;
        }

        /** Serializes object - needed for Writable. */
        public void write(DataOutput out) throws IOException {
            new DoubleWritable(a).write(out);
            new DoubleWritable(b).write(out);
        }

        /** Deserializes object - needed for Writable. */
        public void readFields(DataInput in) throws IOException {
            DoubleWritable tmp = new DoubleWritable();
            tmp.readFields(in);
            a = tmp.get();
        
            tmp.readFields(in);
            b = tmp.get();
        }

        /** Returns A. */
        public Double getA() {
            return a;
        }
        /** Returns B. */
        public Double getB() {
            return b;
        }
    }

  /**
   * Inputs a set of (docID, document contents) pairs.
   * Outputs a set of (Text, Text) pairs.
   */
    public static class Map1 extends Mapper<WritableComparable, Text, Text, DoublePair> {
        /** Regex pattern to find words (alphanumeric + _). */
        final static Pattern WORD_PATTERN = Pattern.compile("\\w+");

        private String targetGram = null;
	    private int funcNum = 0;

        /*
         * Setup gets called exactly once for each mapper, before map() gets called the first time.
         * It's a good place to do configuration or setup that can be shared across many calls to map
         */
        @Override
        public void setup(Context context) {
            targetGram = context.getConfiguration().get("targetGram").toLowerCase();
	        try {
		        funcNum = Integer.parseInt(context.getConfiguration().get("funcNum"));
	        } catch (NumberFormatException e) {
		        /* Do nothing. */
	        }
        }

        @Override
        public void map(WritableComparable docID, Text docContents, Context context)
                throws IOException, InterruptedException {
	        Func func = funcFromNum(funcNum);

            Integer targetSize = targetGram.trim().split("\\s+").length;
            String regex = "[ \\t\\n\\x0B\\f\\r\\p{Punct}]+";
            String[] wordArray = docContents.toString().trim().toLowerCase().split(regex);
            ArrayList<String> gramList = new ArrayList<String>();
            ArrayList<Integer> targetLocations = new ArrayList<Integer>();
            for (Integer i = 0; i < wordArray.length - targetSize + 1; i += 1) {
                String s = wordArray[i];
                for (Integer j = 1; j < targetSize; j += 1) {
                        s = s + " " + wordArray[i + j];
                }
                gramList.add(s);
                if (s.equals(new String(targetGram))) {
                    targetLocations.add(i);
                }
            }
            System.out.println(gramList);
            //System.out.println(targetLocations);        
            int below = -1;
            int above = targetLocations.isEmpty() ? -1 : targetLocations.get(0); 
            //System.out.println(above);
            for (int i = 0, j = 1; i < gramList.size(); i += 1) {
                String gram = gramList.get(i);
                if (gram.equals(new String(targetGram))) {
                    below = above;
                    if (targetLocations.size() >= j + 1) {
                        above = targetLocations.get(j);
                    } else {
                        above = -1;
                    }
                    j += 1;
                    continue;
                }
                double distance = 1;
                if (below == -1 && above != -1) {
                    distance = above - i;
                } else if (below != -1 && above == -1) {
                    distance = i - below;
                } else if (below != -1 && above != -1) {
                    distance = (above - i >= i - below) ? above - i : i - below;
                } else if (below == -1 && above == -1) {
                    distance = Double.POSITIVE_INFINITY;
                }
                //if (distance != Double.POSITIVE_INFINITY) {System.out.println(distance + " " + func.f(distance));}
                DoublePair pair = new DoublePair(func.f(distance), 1.0);
                context.write(new Text(gram), pair);
            }
	    }

	/** Returns the Func corresponding to FUNCNUM*/
	private Func funcFromNum(int funcNum) {
	    Func func = null;
	    switch (funcNum) {
	    case 0:	
		func = new Func() {
			public double f(double d) {
			    return d == Double.POSITIVE_INFINITY ? 0.0 : 1.0;
			}			
		    };	
		break;
	    case 1:
		func = new Func() {
			public double f(double d) {
			    return d == Double.POSITIVE_INFINITY ? 0.0 : 1.0 + 1.0 / d;
			}			
		    };
		break;
	    case 2:
		func = new Func() {
			public double f(double d) {
			    return d == Double.POSITIVE_INFINITY ? 0.0 : Math.sqrt(d);
			}			
		    };
		break;
	    }
	    return func;
	}
    }

    /** Here's where you'll be implementing your combiner. It must be non-trivial for you to receive credit. */
    public static class Combine1 extends Reducer<Text, DoublePair, Text, DoublePair> {

      @Override
      public void reduce(Text key, Iterable<DoublePair> values,
              Context context) throws IOException, InterruptedException {
          double sum = 0, count = 0;
          for (DoublePair value : values) {
              sum += value.getA();
              count += 1;
          }
          if(sum != 0) {System.out.println(sum);}
	      context.write(key, new DoublePair(sum, count));
      }
    }


    public static class Reduce1 extends Reducer<Text, DoublePair, DoubleWritable, Text> {
        @Override
        public void reduce(Text key, Iterable<DoublePair> sumsAndCounts,
			   Context context) throws IOException, InterruptedException {
            double totalSum = 0, totalCount = 0;
            for (DoublePair sumAndCount : sumsAndCounts) {
                totalSum += sumAndCount.getA();
                totalCount += sumAndCount.getB();
            }
            double nonZeroRate = totalSum * Math.pow(Math.log(totalSum), 3) / totalCount;
            double rate = totalSum > 0 ? nonZeroRate : 0;
            context.write(new DoubleWritable(-1 * rate), key);   
        }
    }

    public static class Map2 extends Mapper<DoubleWritable, Text, DoubleWritable, Text> {
        @Override
        public void map(DoubleWritable value, Text gram, Context context)
                throws IOException, InterruptedException {
            context.write(value, gram);
        }
    }

    public static class Reduce2 extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

      int n = 0;
      static int N_TO_OUTPUT = 100;

      /*
       * Setup gets called exactly once for each reducer, before reduce() gets called the first time.
       * It's a good place to do configuration or setup that can be shared across many calls to reduce
       */
      @Override
      protected void setup(Context c) {
        n = 0;
      }

        @Override
        public void reduce(DoubleWritable value, Iterable<Text> grams,
                Context context) throws IOException, InterruptedException {
            DoubleWritable endVal = new DoubleWritable(Math.abs(value.get()));
            for (Text gram : grams) {
                context.write(endVal, gram);
            }
        }
    }

    /*
     *  If you set combiner to false, neither combiner will run. This is also
     *  intended as a debugging aid. Turning on and off the combiner shouldn't alter
     *  your results. Since the framework doesn't make promises about when it'll
     *  invoke combiners, it's an error to assume anything about how many times
     *  values will be combined.
     */
    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        boolean runJob2 = conf.getBoolean("runJob2", true);
        boolean combiner = conf.getBoolean("combiner", false);

        if(runJob2)
          System.out.println("running both jobs");
        else
          System.out.println("for debugging, only running job 1");

        if(combiner)
          System.out.println("using combiner");
        else
          System.out.println("NOT using combiner");

        Path inputPath = new Path(args[0]);
        Path middleOut = new Path(args[1]);
        Path finalOut = new Path(args[2]);
        FileSystem hdfs = middleOut.getFileSystem(conf);
        int reduceCount = conf.getInt("reduces", 32);

        if(hdfs.exists(middleOut)) {
          System.err.println("can't run: " + middleOut.toUri().toString() + " already exists");
          System.exit(1);
        }
        if(finalOut.getFileSystem(conf).exists(finalOut) ) {
          System.err.println("can't run: " + finalOut.toUri().toString() + " already exists");
          System.exit(1);
        }

        {
            Job firstJob = new Job(conf, "wordcount+co-occur");

            firstJob.setJarByClass(Map1.class);

	    /* You may need to change things here */
            firstJob.setMapOutputKeyClass(Text.class);
            firstJob.setMapOutputValueClass(DoublePair.class);
            firstJob.setOutputKeyClass(DoubleWritable.class);
            firstJob.setOutputValueClass(Text.class);
	    /* End region where we expect you to perhaps need to change things. */

            firstJob.setMapperClass(Map1.class);
            firstJob.setReducerClass(Reduce1.class);
            firstJob.setNumReduceTasks(reduceCount);


            if(combiner)
              firstJob.setCombinerClass(Combine1.class);

            firstJob.setInputFormatClass(SequenceFileInputFormat.class);
            if(runJob2)
              firstJob.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.addInputPath(firstJob, inputPath);
            FileOutputFormat.setOutputPath(firstJob, middleOut);

            firstJob.waitForCompletion(true);
        }

        if(runJob2) {
            Job secondJob = new Job(conf, "sort");

            secondJob.setJarByClass(Map1.class);
	    /* You may need to change things here */
            secondJob.setMapOutputKeyClass(DoubleWritable.class);
            secondJob.setMapOutputValueClass(Text.class);
            secondJob.setOutputKeyClass(DoubleWritable.class);
            secondJob.setOutputValueClass(Text.class);
	    /* End region where we expect you to perhaps need to change things. */

            secondJob.setMapperClass(Map2.class);
            if(combiner)
              secondJob.setCombinerClass(Reduce2.class);
            secondJob.setReducerClass(Reduce2.class);

            secondJob.setInputFormatClass(SequenceFileInputFormat.class);
            secondJob.setOutputFormatClass(TextOutputFormat.class);
            secondJob.setNumReduceTasks(1);


            FileInputFormat.addInputPath(secondJob, middleOut);
            FileOutputFormat.setOutputPath(secondJob, finalOut);

            secondJob.waitForCompletion(true);
        }
    }

}

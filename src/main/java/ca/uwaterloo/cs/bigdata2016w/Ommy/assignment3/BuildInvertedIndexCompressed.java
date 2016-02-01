package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment3;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.*;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

    private static class MyMapper extends Mapper<LongWritable, Text, PairOfStringLong, IntWritable> {
        private static final PairOfStringLong PAIR = new PairOfStringLong();
        private static final Object2IntFrequencyDistribution<String> COUNTS =
                new Object2IntFrequencyDistributionEntry<String>();
        private static final IntWritable INT = new IntWritable();

        @Override
        public void map(LongWritable docno, Text doc, Context context)
                throws IOException, InterruptedException {
            String text = doc.toString();
            // Tokenize line.
            List<String> tokens = new ArrayList<String>();
            StringTokenizer itr = new StringTokenizer(text);
            while (itr.hasMoreTokens()) {
                String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
                if (w.length() == 0) continue;
                tokens.add(w);
            }

            // Build a histogram of the terms.
            COUNTS.clear();
            for (String token : tokens) {
                COUNTS.increment(token);
            }

            // Emit postings.
            for (PairOfObjectInt<String> e : COUNTS) {
                PAIR.set(e.getLeftElement(), docno.get());
                INT.set(e.getRightElement());
                context.write(PAIR, INT);
            }
        }
    }

    private static class MyReducer extends
            Reducer<PairOfStringLong, IntWritable, Text, PairOfWritables<VIntWritable, BytesWritable>> {
        private final static VIntWritable DF = new VIntWritable();
        private static BytesWritable bW = new BytesWritable();
        private static String prev;
        private static int gap = -1;
        private static final ArrayList<PairOfLongInt> P = new ArrayList<>();
        private static final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        private static final DataOutputStream dataOutStream = new DataOutputStream(byteArrayOutputStream);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            prev = null;
        }

        @Override
        public void reduce(PairOfStringLong key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            Iterator<IntWritable> iter = values.iterator();

            if (prev != null && ! key.getLeftElement().contentEquals(prev)) {
                int previousDoc = 0;
                for (PairOfLongInt tf: P) {
                    if (gap == -1) {
                        gap = (int) tf.getLeftElement();
                    } else {
                        gap = ((int) tf.getLeftElement()) - previousDoc;
                    }
                    previousDoc = (int)tf.getLeftElement();
                    WritableUtils.writeVInt(dataOutStream, gap); // gap
                    WritableUtils.writeVInt(dataOutStream, tf.getRightElement());
                }
                DF.set(P.size());
                bW = new BytesWritable(byteArrayOutputStream.toByteArray());
                context.write(new Text(prev), new PairOfWritables<>(DF, bW));
                byteArrayOutputStream.reset();
                dataOutStream.flush();
                P.clear();
                gap = -1;
            }

            int f;
            while (iter.hasNext()) {
                f = iter.next().get();
                P.add(new PairOfLongInt(key.getRightElement(), f));
            }
            prev = key.getLeftElement();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int previousDoc = 0;
            for (PairOfLongInt tf: P) {
                if (gap == -1) {
                    gap = (int) tf.getLeftElement();
                } else {
                    gap = ((int) tf.getLeftElement()) - previousDoc;
                }
                previousDoc = (int)tf.getLeftElement();
                WritableUtils.writeVInt(dataOutStream, gap); // gap
                WritableUtils.writeVInt(dataOutStream, tf.getRightElement());
            }
            DF.set(P.size());
            bW = new BytesWritable(byteArrayOutputStream.toByteArray());
            context.write(new Text(prev), new PairOfWritables<>(DF, bW));
            byteArrayOutputStream.reset();
            dataOutStream.flush();
            P.clear();
            gap = -1;
        }
    }

    private static class CompressionPartitioner extends Partitioner<PairOfStringLong, IntWritable> {

        @Override
        public int getPartition(PairOfStringLong pairOfStringLong, IntWritable intWritable, int numReduceTasks) {
            return (pairOfStringLong.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    private BuildInvertedIndexCompressed() {
    }

    public static class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        public String input;

        @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
        public String output;

        @Option(name = "-reducers", metaVar = "[path]", required = false, usage = "number of reducers")
        public int reducers;
    }

    /**
     * Runs this tool.
     */
    public int run(String[] argv) throws Exception {
        Args args = new Args();
        CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return -1;
        }

        LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);

        Job job = Job.getInstance(getConf());
        job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
        job.setJarByClass(BuildInvertedIndexCompressed.class);

        job.setNumReduceTasks(args.reducers);

        FileInputFormat.setInputPaths(job, new Path(args.input));
        FileOutputFormat.setOutputPath(job, new Path(args.output));

        job.setMapOutputKeyClass(PairOfStringLong.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairOfWritables.class);
        job.setOutputFormatClass(MapFileOutputFormat.class);
        job.setPartitionerClass(CompressionPartitioner.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(args.output);
        FileSystem.get(getConf()).delete(outputDir, true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BuildInvertedIndexCompressed(), args);
    }
}

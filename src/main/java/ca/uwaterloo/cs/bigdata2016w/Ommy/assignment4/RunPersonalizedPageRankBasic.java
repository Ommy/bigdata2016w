package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment4;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.array.ArrayListOfIntsWritable;

import com.google.common.base.Preconditions;

/**
 * <p>
 * Main driver program for running the basic (non-Schimmy) implementation of
 * PageRank.
 * </p>
 * <p>
 * <p>
 * The starting and ending iterations will correspond to paths
 * <code>/base/path/iterXXXX</code> and <code>/base/path/iterYYYY</code>. As a
 * example, if you specify 0 and 10 as the starting and ending iterations, the
 * driver program will start with the graph structure stored at
 * <code>/base/path/iter0000</code>; final results will be stored at
 * <code>/base/path/iter0010</code>.
 * </p>
 *
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class RunPersonalizedPageRankBasic extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(RunPersonalizedPageRankBasic.class);

    private static enum PageRank {
        nodes, edges, massMessages, massMessagesSaved, massMessagesReceived, missingStructure
    }

    ;

    // Mapper, no in-mapper combining.
    private static class MapClass extends
            Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {

        // The neighbor to which we're sending messages.
        private static final IntWritable neighbor = new IntWritable();

        // Contents of the messages: partial PageRank mass.
        private static final PageRankNode intermediateMass = new PageRankNode();

        // For passing along node structure.
        private static final PageRankNode intermediateStructure = new PageRankNode();

        private static int numSources;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numSources = context.getConfiguration().getInt("numSources", 1);
            intermediateMass.setSourcesSize(numSources);
            intermediateStructure.setSourcesSize(numSources);
        }

        @Override
        public void map(IntWritable nid, PageRankNode node, Context context)
                throws IOException, InterruptedException {
            // Pass along node structure.
            intermediateStructure.setNodeId(node.getNodeId());
            intermediateStructure.setType(PageRankNode.Type.Structure);
            intermediateStructure.setAdjacencyList(node.getAdjacenyList());

            context.write(nid, intermediateStructure);

            int massMessages = 0;

            // Distribute PageRank mass to neighbors (along outgoing edges).
            if (node.getAdjacenyList().size() > 0) {
                // Each neighbor gets an equal share of PageRank mass.
                ArrayListOfIntsWritable list = node.getAdjacenyList();
                float[] masses = new float[numSources];
                for (int i = 0; i < numSources; i++) {
                    masses[i] = node.getPageRank(i) - (float) StrictMath.log(list.size());
                }

                context.getCounter(PageRank.edges).increment(list.size());

                // Iterate over neighbors.
                for (int i = 0; i < list.size(); i++) {
                    neighbor.set(list.get(i));
                    intermediateMass.setSourcesSize(numSources);
                    intermediateMass.setNodeId(list.get(i));
                    intermediateMass.setType(PageRankNode.Type.Mass);
                    for (int j = 0; j < numSources; j++) {
                        intermediateMass.setPageRank(masses[j], j);
                    }
                    // Emit messages with PageRank mass to neighbors.
                    context.write(neighbor, intermediateMass);
                    massMessages++;
                }
            }

            // Bookkeeping.
            context.getCounter(PageRank.nodes).increment(1);
            context.getCounter(PageRank.massMessages).increment(massMessages);
        }
    }

    // Reduce: sums incoming PageRank contributions, rewrite graph structure.
    private static class ReduceClass extends
            Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {
        // For keeping track of PageRank mass encountered, so we can compute missing PageRank mass lost
        // through dangling nodes.
        private static ArrayListOfFloatsWritable massPerSource;
        private static float[] localMass;
        private static float[] totalMass;
        private static int numSources;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numSources = context.getConfiguration().getInt("numSources", 1);
            massPerSource = new ArrayListOfFloatsWritable(numSources);
            localMass = new float[numSources];
            totalMass = new float[numSources];
            for (int i = 0; i < numSources; i++) {
                totalMass[i] = Float.NEGATIVE_INFINITY;
            }
        }

        @Override
        public void reduce(IntWritable nid, Iterable<PageRankNode> iterable, Context context)
                throws IOException, InterruptedException {
            Iterator<PageRankNode> values = iterable.iterator();

            // Create the node structure that we're going to assemble back together from shuffled pieces.
            PageRankNode node = new PageRankNode();
            node.setSourcesSize(numSources);

            for (int i = 0; i < numSources; i++) {
                localMass[i] = Float.NEGATIVE_INFINITY;
            }

            node.setType(PageRankNode.Type.Complete);
            node.setNodeId(nid.get());

            int massMessagesReceived = 0;
            int structureReceived = 0;

            while (values.hasNext()) {
                PageRankNode n = values.next();
                if (n.getType().equals(PageRankNode.Type.Structure)) {
                    // This is the structure; update accordingly.
                    ArrayListOfIntsWritable list = n.getAdjacenyList();
                    structureReceived++;
                    node.setAdjacencyList(list);
                } else {
                    for (int i = 0; i < numSources; i++) {
                        // This is a message that contains PageRank mass; accumulate.
                        localMass[i] = sumLogProbs(localMass[i], n.getPageRank(i));
                    }
                    massMessagesReceived++;
                }
            }
            for (int i = 0; i < numSources; i++) {
                // Update the final accumulated PageRank mass.
                node.setPageRank(localMass[i], i);
            }
            context.getCounter(PageRank.massMessagesReceived).increment(massMessagesReceived);

            // Error checking.
            if (structureReceived == 1) {
                // Everything checks out, emit final node structure with updated PageRank value.
                context.write(nid, node);

                // Keep track of total PageRank mass.
                for (int i = 0; i < numSources; i++) {
                    totalMass[i] = sumLogProbs(totalMass[i], localMass[i]);
                }
            } else if (structureReceived == 0) {
                // We get into this situation if there exists an edge pointing to a node which has no
                // corresponding node structure (i.e., PageRank mass was passed to a non-existent node)...
                // log and count but move on.
                context.getCounter(PageRank.missingStructure).increment(1);
                LOG.warn("No structure received for nodeid: " + nid.get() + " mass: "
                        + massMessagesReceived);
                // It's important to note that we don't add the PageRank mass to total... if PageRank mass
                // was sent to a non-existent node, it should simply vanish.
            } else {
                // This shouldn't happen!
                throw new RuntimeException("Multiple structure received for nodeid: " + nid.get()
                        + " mass: " + massMessagesReceived + " struct: " + structureReceived
                        + " numSources: " + numSources + " NODE: " + node.toString());
            }
        }

        @Override
        public void cleanup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String taskId = conf.get("mapred.task.id");
            String path = conf.get("PageRankMassPath");

            Preconditions.checkNotNull(taskId);
            Preconditions.checkNotNull(path);

            // Write to a file the amount of PageRank mass we've seen in this reducer.
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);
            for (int i = 0; i < totalMass.length; i++) {
                massPerSource.add(totalMass[i]);
            }
            massPerSource.write(out);
            out.close();
        }
    }

    // Mapper that distributes the missing PageRank mass (lost at the dangling nodes) and takes care
    // of the random jump factor.
    private static class MapPageRankMassDistributionClass extends
            Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {
        private float[] missingMass;
        private int nodeCnt = 0;
        private static int numSources;
        private static String[] sources;

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();

            String[] massStr = conf.get("MissingMass").replace("[", "").replace("]", "").split(",");
            numSources = conf.getInt("numSources", 1);
            missingMass = new float[massStr.length];
            for (int i = 0; i < massStr.length; i++) {
                missingMass[i] = Float.parseFloat(massStr[i]);
            }
            nodeCnt = conf.getInt("NodeCount", 0);
            sources = conf.get("sources").split(",");
        }

        private boolean isSourceNode(int id) {
            for (String src : sources) {
                if (Integer.parseInt(src.trim()) == id) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void map(IntWritable nid, PageRankNode node, Context context)
                throws IOException, InterruptedException {
            float jump = (float) Math.log(ALPHA);
            float link;
            for (int i = 0; i < numSources; i++) {
                float p = node.getPageRank(i);
                if (isSourceNode(nid.get())) {
                    link = (float) Math.log(1.0f - ALPHA) + sumLogProbs(p, (float) Math.log(missingMass[i]));
                    p = sumLogProbs(jump, link);
                } else {
                    p = p + (float) Math.log(1.0f - ALPHA);
                }
                node.setPageRankAtIndex(p, i);
            }
            context.write(nid, node);
        }
    }

    // Random jump factor.
    private static float ALPHA = 0.15f;
    private static NumberFormat formatter = new DecimalFormat("0000");

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new RunPersonalizedPageRankBasic(), args);
    }

    public RunPersonalizedPageRankBasic() {
    }

    private static final String BASE = "base";
    private static final String NUM_NODES = "numNodes";
    private static final String START = "start";
    private static final String END = "end";
    private static final String COMBINER = "useCombiner";
    private static final String INMAPPER_COMBINER = "useInMapperCombiner";
    private static final String SOURCES = "sources";

    /**
     * Runs this tool.
     */
    @SuppressWarnings({"static-access"})
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(new Option(COMBINER, "use combiner"));
        options.addOption(new Option(INMAPPER_COMBINER, "user in-mapper combiner"));

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("base path").create(BASE));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("start iteration").create(START));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("end iteration").create(END));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of nodes").create(NUM_NODES));
        options.addOption(OptionBuilder.withArgName("sources").hasArg()
                .withDescription("list of source nodes").create(SOURCES));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(BASE) || !cmdline.hasOption(START) ||
                !cmdline.hasOption(END) || !cmdline.hasOption(NUM_NODES)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String basePath = cmdline.getOptionValue(BASE);
        int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
        int s = Integer.parseInt(cmdline.getOptionValue(START));
        int e = Integer.parseInt(cmdline.getOptionValue(END));
        boolean useCombiner = cmdline.hasOption(COMBINER);
        String[] sources = cmdline.getOptionValue(SOURCES).split(",");
//        int source = Integer.parseInt(sources.trim().split(",")[0]);

        LOG.info("Tool name: RunPageRank");
        LOG.info(" - base path: " + basePath);
        LOG.info(" - num nodes: " + n);
        LOG.info(" - start iteration: " + s);
        LOG.info(" - end iteration: " + e);
        LOG.info(" - use combiner: " + useCombiner);

        // Iterate PageRank.
        for (int i = s; i < e; i++) {
            iteratePageRank(i, i + 1, basePath, n, useCombiner, sources);
        }

        return 0;
    }

    // Run each iteration.
    private void iteratePageRank(int i, int j, String basePath, int numNodes,
                                 boolean useCombiner, String[] sources) throws Exception {
        // Each iteration consists of two phases (two MapReduce jobs).

        // Job 1: distribute PageRank mass along outgoing edges.
        float[] mass = phase1(i, j, basePath, numNodes, useCombiner, sources);

        // Find out how much PageRank mass got lost at the dangling nodes.
        float[] missing = new float[mass.length];
        for (int x = 0; x < mass.length; x++) {
            missing[x] = 1.0f - (float) StrictMath.exp(mass[x]);
        }
//        float missing = 1.0f - (float) StrictMath.exp(mass);

        // Job 2: distribute missing mass, take care of random jump factor.
        phase2(i, j, missing, basePath, numNodes, sources);
    }

    private float[] phase1(int i, int j, String basePath, int numNodes,
                           boolean useCombiner, String[] sources) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("PageRank:Basic:iteration" + j + ":Phase1");
        job.setJarByClass(RunPersonalizedPageRankBasic.class);

        String in = basePath + "/iter" + formatter.format(i);
        String out = basePath + "/iter" + formatter.format(j) + "t";
        String outm = out + "-mass";

        // We need to actually count the number of part files to get the number of partitions (because
        // the directory might contain _log).
        int numPartitions = 0;
        for (FileStatus s : FileSystem.get(getConf()).listStatus(new Path(in))) {
            if (s.getPath().getName().contains("part-"))
                numPartitions++;
        }

        LOG.info("PageRank: iteration " + j + ": Phase1");
        LOG.info(" - input: " + in);
        LOG.info(" - output: " + out);
        LOG.info(" - nodeCnt: " + numNodes);
        LOG.info(" - useCombiner: " + useCombiner);
        LOG.info("computed number of partitions: " + numPartitions);

        int numReduceTasks = numPartitions;

        job.getConfiguration().setInt("NodeCount", numNodes);
        job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
        job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
        //job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048m");
        job.getConfiguration().set("PageRankMassPath", outm);
        job.getConfiguration().setInt("numSources", sources.length);

        job.setNumReduceTasks(numReduceTasks);

        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PageRankNode.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PageRankNode.class);

        job.setMapperClass(MapClass.class);

        job.setReducerClass(ReduceClass.class);

        FileSystem.get(getConf()).delete(new Path(out), true);
        FileSystem.get(getConf()).delete(new Path(outm), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        float[] mass = new float[sources.length];
        for (int x = 0; x < sources.length; x++) {
            mass[x] = Float.NEGATIVE_INFINITY;
        }
        ArrayListOfFloatsWritable flw = new ArrayListOfFloatsWritable();
        FileSystem fs = FileSystem.get(getConf());
        for (FileStatus f : fs.listStatus(new Path(outm))) {
            FSDataInputStream fin = fs.open(f.getPath());
            flw.readFields(fin);
            for (int k = 0; k < flw.size(); k++) {
                mass[k] = sumLogProbs(mass[k], flw.get(k));
            }
            fin.close();
        }
        return mass;
    }

    private void phase2(int i, int j, float[] missing, String basePath, int numNodes, String[] sources) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("PageRank:Basic:iteration" + j + ":Phase2");
        job.setJarByClass(RunPersonalizedPageRankBasic.class);

        LOG.info("missing PageRank mass: " + missing);
        LOG.info("number of nodes: " + numNodes);

        String in = basePath + "/iter" + formatter.format(j) + "t";
        String out = basePath + "/iter" + formatter.format(j);

        LOG.info("PageRank: iteration " + j + ": Phase2");
        LOG.info(" - input: " + in);
        LOG.info(" - output: " + out);

        job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
        job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
        job.getConfiguration().set("MissingMass", Arrays.toString(missing));
        job.getConfiguration().setInt("NodeCount", numNodes);
        job.getConfiguration().setInt("numSources", sources.length);
        job.getConfiguration().set("sources", StringUtils.join(sources, ","));

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PageRankNode.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PageRankNode.class);

        job.setMapperClass(MapPageRankMassDistributionClass.class);

        FileSystem.get(getConf()).delete(new Path(out), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }

    // Adds two log probs.
    private static float sumLogProbs(float a, float b) {
        if (a == Float.NEGATIVE_INFINITY)
            return b;

        if (b == Float.NEGATIVE_INFINITY)
            return a;

        if (a < b) {
            return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
        }

        return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
    }
}

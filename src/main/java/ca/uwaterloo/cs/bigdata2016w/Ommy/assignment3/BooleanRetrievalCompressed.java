package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment3;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.pair.PairOfInts;

public class BooleanRetrievalCompressed extends Configured implements Tool {
    private List<MapFile.Reader> index;
    private FSDataInputStream collection;
    private Stack<Set<Integer>> stack;
    private int numReducers;

    private BooleanRetrievalCompressed() {
    }

    private void initialize(String indexPath, String collectionPath, FileSystem fs) throws IOException {
        index = new ArrayList<>();
        FileStatus[] status = fs.listStatus(new Path(indexPath));
        for (FileStatus fss : status) {
            Path path = fss.getPath();
            if (path.getName().contains("SUCCESS")) continue;
            index.add(new MapFile.Reader(path, fs.getConf()));
        }
        numReducers = index.size();
        collection = fs.open(new Path(collectionPath));
        stack = new Stack<>();
    }

    private void runQuery(String q) throws IOException {
        String[] terms = q.split("\\s+");

        for (String t : terms) {
            if (t.equals("AND")) {
                performAND();
            } else if (t.equals("OR")) {
                performOR();
            } else {
                pushTerm(t);
            }
        }

        Set<Integer> set = stack.pop();
        for (Integer i : set) {
            String line = fetchLine(i);
            System.out.println(i + "\t" + line);
        }
    }

    private void pushTerm(String term) throws IOException {
        stack.push(fetchDocumentSet(term));
    }

    private void performAND() {
        Set<Integer> s1 = stack.pop();
        Set<Integer> s2 = stack.pop();

        Set<Integer> sn = new TreeSet<Integer>();

        for (int n : s1) {
            if (s2.contains(n)) {
                sn.add(n);
            }
        }

        stack.push(sn);
    }

    private void performOR() {
        Set<Integer> s1 = stack.pop();
        Set<Integer> s2 = stack.pop();

        Set<Integer> sn = new TreeSet<Integer>();

        for (int n : s1) {
            sn.add(n);
        }

        for (int n : s2) {
            sn.add(n);
        }

        stack.push(sn);
    }

    private Set<Integer> fetchDocumentSet(String term) throws IOException {
        Set<Integer> set = new TreeSet<Integer>();
        for (PairOfInts pair : fetchPostings(term)) {
            set.add(pair.getLeftElement());
        }

        return set;
    }

    private int getHash(String key) {
        return (key.hashCode() & Integer.MAX_VALUE) % numReducers;
    }

    private List<PairOfInts> fetchPostings(String term) throws IOException {
        Text key = new Text();
        BytesWritable value = new BytesWritable();
        List<PairOfInts> list = new ArrayList<>();

        key.set(term);

        index.get(getHash(term)).get(key, value);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(value.getBytes());
        DataInputStream dis = new DataInputStream(byteArrayInputStream);

        int size = WritableUtils.readVInt(dis);
        if (size == 0) {
            return list;
        }

        int docno = -1;
        for (int i = 0; i< size; i++) {
            if (docno == -1) {
                docno = WritableUtils.readVInt(dis);
            } else {
                docno += WritableUtils.readVInt(dis);
            }
            int freq = WritableUtils.readVInt(dis);
            list.add(new PairOfInts(docno, freq));
        }

        return list;
    }

    private String fetchLine(long offset) throws IOException {
        collection.seek(offset);
        BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

        return reader.readLine();
    }

    public static class Args {
        @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
        public String index;

        @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
        public String collection;

        @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
        public String query;
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

        if (args.collection.endsWith(".gz")) {
            System.out.println("gzipped collection is not seekable: use compressed version!");
            return -1;
        }

        FileSystem fs = FileSystem.get(new Configuration());

        initialize(args.index, args.collection, fs);

        System.out.println("Query: " + args.query);
        long startTime = System.currentTimeMillis();
        runQuery(args.query);
        System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

        return 1;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BooleanRetrievalCompressed(), args);
    }
}

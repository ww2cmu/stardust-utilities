import java.util.*;
import java.util.regex.*;
import java.io.*;
import java.nio.*;
import java.text.*;

public class Compress {
  public static long fileId = 0;
  public static final Pattern nodeRegex = Pattern.compile("([0-9]+)\\.[0-9]+ \\[label=\"(.+)\"\\]");
  public static final Pattern edgeRegex = Pattern.compile("([0-9]+)\\.[0-9]+ -> ([0-9]+)\\.[0-9]+ \\[label=\"R: (-?[0-9\\.]+) us\"\\]");
  public static DecimalFormat df = new DecimalFormat("#.###");
  public static void main(String args[]) throws IOException {
    if (args.length < 2) {
      System.out.println("Usage: java Compress <dot> <output_dir>");
      System.exit(-1);
    }
    File dot = new File(args[0]);
    if (!dot.exists() || !dot.isFile()) {
      System.out.println("Cannot read DOT file");
      System.exit(-2);
    }
    File outputDir = new File(args[1]);
    if (outputDir.isFile()) {
      System.out.println("Invalid directory");
      System.exit(-3);
    }
    if (!outputDir.exists()) {
      if (!outputDir.mkdir()) {
        System.out.println("Could not make directory");
        System.exit(-4);
      }
    }
    df.setMinimumFractionDigits(3);
    parseFile(dot, args[1]);
  }

  public static void parseFile(File dot, String outputDir) throws IOException {
    Scanner sc = new Scanner(new FileInputStream(dot));
    PrintStream out = new PrintStream(new FileOutputStream(outputDir + "/" + dot.getName()), true); 
    while(sc.hasNext()) {
      String line = sc.nextLine();
      out.println(line);
      if (line.matches("\\# [0-9]+  R: [0-9\\.]+ usecs")) {
        line = sc.nextLine();
        out.println(line);
        compress(sc, out, outputDir);
      }
    }
  }

  public static void compress(Scanner sc, PrintStream out, String outputDir) throws IOException {
    String line = sc.nextLine();
    Matcher m;
    if ((line.indexOf("NEW_BLOCK") != -1 || line.indexOf("APPEND_BLOCK") != -1) && (m = nodeRegex.matcher(line)).matches()) {
      Graph g = new Graph(Long.parseLong(m.group(1)), m.group(2));
      FrequencyGraph fg = new FrequencyGraph(m.group(2), g.root);
      line = sc.nextLine();
      while(line.indexOf("}") == -1) {
        if (line.indexOf("->") == -1) {
          m = nodeRegex.matcher(line);
          if (!m.matches()) {
            System.out.println("Doesn't match: " + line);
            System.exit(-5);
          }
          g.addNode(Long.parseLong(m.group(1)), m.group(2));
        } else {
          m = edgeRegex.matcher(line);
          if (!m.matches()) {
            System.out.println("Doesn't match: " + line);
            System.exit(-6);
          }
          g.addEdge(Long.parseLong(m.group(1)), Long.parseLong(m.group(2)), Double.parseDouble(m.group(3)));
        }
        line = sc.nextLine();
      }
      fillFrequencyGraph(g, g.root, fg, new HashSet<Long>());
      printFrequencyGraphNodes(out, fg, fg.root, new HashSet<String>());
      printFrequencyGraphEdges(out, fg, fg.root, new HashSet<String>(), outputDir);
      out.println("}");
    } else {
      out.println(line);
      line = sc.nextLine();
      while(line.indexOf("}") == -1) {
        out.println(line);
        line = sc.nextLine();
      }
    }
  }
  
  public static class Graph {
    long root;
    HashMap<Long, String> name;
    HashMap<Long, ArrayList<Edge>> adjacencyList;
    public Graph(long root, String rootName) {
      this.root = root;
      name = new HashMap<Long, String>();
      name.put(root, rootName);
      adjacencyList = new HashMap<Long, ArrayList<Edge>>();
      adjacencyList.put(root, new ArrayList<Edge>());
    }

    public void addNode(long id, String name) {
      this.name.put(id, name);
      adjacencyList.put(id, new ArrayList<Edge>());
    }
    public void addEdge(long id, long id2, double latency) {
      if (!adjacencyList.containsKey(id))
        adjacencyList.put(id, new ArrayList<Edge>());
      adjacencyList.get(id).add(new Edge(id2, latency));
    }
  }

  public static class FrequencyGraph {
    String root;
    HashMap<String, HashMap<String, ArrayList<Double>>> adjacencyList;
    HashMap<String, Long> ids;
    public FrequencyGraph(String root, long id) {
      this.root = root;
      adjacencyList = new HashMap<String, HashMap<String, ArrayList<Double>>>();
      adjacencyList.put(root, new HashMap<String, ArrayList<Double>>());
      ids = new HashMap<String, Long>();
      ids.put(root, id);
    }
    public void addNode(String node, long id) {
      if (!ids.containsKey(node)) {
        ids.put(node, id);
        adjacencyList.put(node, new HashMap<String, ArrayList<Double>>());
      }
    }
    public void addEdge(String from, String to, double latency) {
      if (!adjacencyList.containsKey(from))
        adjacencyList.put(from, new HashMap<String, ArrayList<Double>>());
      if (!adjacencyList.get(from).containsKey(to))
        adjacencyList.get(from).put(to, new ArrayList<Double>());
      adjacencyList.get(from).get(to).add(latency);
    }
  }

  public static class Edge {
     long id;
     double latency;
     public Edge(long id, double latency) {
       this.id = id;
       this.latency = latency;
     }
  }

  public static void fillFrequencyGraph (Graph g, long node, FrequencyGraph fg, HashSet<Long> visited) {
    if (visited.contains(node))
      return;
    visited.add(node);
    fg.addNode(g.name.get(node), node);
    for (Edge e : g.adjacencyList.get(node)) {
      fg.addEdge(g.name.get(node), g.name.get(e.id), e.latency);
      //System.out.println(g.name.get(node) + " -> " + g.name.get(e.id) + " " + e.latency);
      fillFrequencyGraph(g, e.id, fg, visited);
    }
  }

  public static void printFrequencyGraphNodes(PrintStream out, FrequencyGraph fg, String node, HashSet<String> visited) throws IOException {
    if (visited.contains(node))
      return;
    visited.add(node);
    out.println(fg.ids.get(node) + "." + fg.ids.get(node) + " [label=\"" + node + "\"]");
    for (Map.Entry<String, ArrayList<Double>> entry : fg.adjacencyList.get(node).entrySet()) {
       printFrequencyGraphNodes(out, fg, entry.getKey(), visited);
    }
  }

  public static void printFrequencyGraphEdges(PrintStream out, FrequencyGraph fg, String node, HashSet<String> visited, String outputDir) throws IOException {
    if (visited.contains(node))
      return;
    visited.add(node);
    for (Map.Entry<String, ArrayList<Double>> entry : fg.adjacencyList.get(node).entrySet()) {
      if (entry.getValue().size() == 1) {
        out.println(fg.ids.get(node) + "." + fg.ids.get(node) + " -> " + fg.ids.get(entry.getKey()) + "." + fg.ids.get(entry.getKey()) + " [label=\"R: " + df.format(entry.getValue().get(0)) + " us\"]");
      } else {
        ArrayList<Double> latencies = entry.getValue();
        String fileName = fileId++ + ".dat";
        PrintStream datOut = new PrintStream(new FileOutputStream(outputDir + "/" + fileName), true);
        for(int i = 0; i < latencies.size(); i++)
          datOut.println(df.format(latencies.get(i)) + " us");
        datOut.close();
        out.println(fg.ids.get(node) + "." + fg.ids.get(node) + " -> " + fg.ids.get(entry.getKey()) + "." + fg.ids.get(entry.getKey()) + " [label=\"F: " + fileName + "\"]");
      }
      printFrequencyGraphEdges(out, fg, entry.getKey(), visited, outputDir);
    }
  }
}

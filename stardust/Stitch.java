package stardust;

import java.io.IOException;
import java.util.*;
import java.nio.*;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class Stitch {
    public static class StitchMapper extends TableMapper<Text, Text> {
      @Override
      public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException, InterruptedException {
        String rowkey = Bytes.toString(row.get());
        String[] tokens = rowkey.split("|");
        context.write(new Text(tokens[0] + "|" + tokens[1]), new Text(rowkey + "|" + new String(values.getFamilyMap("inst_pt_name".getBytes()).keySet().iterator().next())));
      }
    }
    public static class StitchReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Reconstruct.Graph g = new Reconstruct.Graph();
        HashMap<String, ArrayList<Reconstruct.Node>> nodes = new HashMap<String, ArrayList<Reconstruct.Node>>();
        HashMap<String, String> adjList = new HashMap<String, String>();
        HashSet<String> idList = new HashSet<String>();
        for (Text t : values) {
          String[] tokens = t.toString().split("|");
          String taskId = tokens[0];
          String requestId = tokens[1];
          String[] traceIds = tokens[2].split(",");
          String currentId = traceIds[traceIds.length - 1];
          String timestamp = tokens[3];
          String label = tokens[4];
          Reconstruct.Node n = new Reconstruct.Node();
          n.label = label;
          n.hostname = label.substring(0, label.indexOf("|"));
          byte[] md = new byte[currentId.length() >> 1];
          for(int j = 0; j < currentId.length(); j+=2)
            md[j >> 1] = (byte)((Character.digit(currentId.charAt(j), 16) << 4)
                               +Character.digit(currentId.charAt(j+1), 16));
          ByteBuffer buf = ByteBuffer.wrap(md);
          n.id = String.valueOf(Math.abs(buf.getLong()));
          idList.add(n.id);
          n.timestamp = Double.parseDouble(timestamp);
          n.strTimestamp = timestamp;
          for (int i = 0; i < traceIds.length - 1; i++) {
            md = new byte[traceIds[i].length() >> 1];
            for(int j = 0; j < traceIds[i].length(); j+=2)
              md[j >> 1] = (byte)((Character.digit(traceIds[i].charAt(j), 16) << 4)
                                 +Character.digit(traceIds[i].charAt(j+1), 16));
            buf = ByteBuffer.wrap(md);
            String pid = String.valueOf(Math.abs(buf.getLong()));
            adjList.put(pid, n.id);
          }
          if (!nodes.containsKey(n.id))
            nodes.put(n.id, new ArrayList<Reconstruct.Node>());
          nodes.get(n.id).add(n);
        }
        for(Map.Entry<String, ArrayList<Reconstruct.Node>> entry : nodes.entrySet()) {
          ArrayList<Reconstruct.Node> a = entry.getValue();
          if (a.size() <= 0)
            continue;
          Collections.sort(a);
          a.get(0).id = a.get(0).id + "." + a.get(0).strTimestamp;
          g.nodes.put(a.get(0).id, a.get(0));
          for(int i = 1; i < a.size(); i++) {
            a.get(i).id = a.get(i).id + "." + a.get(i).strTimestamp;
            a.get(i-1).children.add(new Reconstruct.Edge(a.get(i-1).id, a.get(i).id));
            g.nodes.put(a.get(i).id, a.get(i));
          }
        }
        for(Map.Entry<String, String> entry : adjList.entrySet()) {
          ArrayList<Reconstruct.Node> from = nodes.get(entry.getKey());
          ArrayList<Reconstruct.Node> to = nodes.get(entry.getValue());
          idList.remove(entry.getValue());
          if (from.size() <= 0)
            continue;
          from.get(from.size() - 1).children.add(new Reconstruct.Edge(from.get(from.size()-1).id, to.get(0).id));
        }
        g.root = nodes.get(idList.iterator().next()).get(0);
        if (Reconstruct.validateGraph(g)) {
          Reconstruct.calculateTotalTime(g);
          Reconstruct.calculateLatencies(g.nodes, g.root);
        }
      }
    }
    public static void main(String[] args) throws Exception{
      Configuration conf = HBaseConfiguration.create();
      Job job = new Job(conf, "Stitch");
      job.setJarByClass(Stitch.class);
      Scan scan = new Scan();
      TableMapReduceUtil.initTableMapperJob("inst_pts", scan, StitchMapper.class, Text.class, Text.class, job);
      TableMapReduceUtil.initTableReducerJob("graphs", StitchReducer.class, job);
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

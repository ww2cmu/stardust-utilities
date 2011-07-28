import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseToFile {
  public static void main(String args[]) throws Exception {
    if (args.length < 1) {
      System.out.println("Usage: HBaseToFile filename [jobID]");
      System.exit(-1);
    }

    int count = 0;

      DataOutputStream out = new DataOutputStream(new FileOutputStream(args[0]));
      Configuration conf = HBaseConfiguration.create();
      HTable table = new HTable(conf, "inst_pts");
      Scan s;
      if (args.length >= 2)
        s = new Scan(args[1].getBytes());
      else
        s = new Scan();
      ResultScanner scanner = table.getScanner(s);
      try {
        for(Result r = scanner.next(); r!=null; r = scanner.next()) {
          String rowkey = new String(r.getRow());
          if (args.length >= 2 && rowkey.indexOf(args[1]) == -1)
            break;
          String label = new String(r.getFamilyMap("inst_pt_name".getBytes()).keySet().iterator().next());
          String[] tokens = rowkey.split("\\|");
          String[] reportIds = tokens[2].split(",");
          String[] labeltok = label.split("_");
          String report = "X-Trace Report ver 1.0";
          report       += "\nX-Trace: 19" + tokens[1] + reportIds[reportIds.length-1];
          report       += "\nHost: " + labeltok[0];
          report       += "\nAgent: " + labeltok[1];
          report       += "\nLabel: " + label;
          report       += "\nTaskID: " + tokens[0];
          for(int i = 0; i < reportIds.length - 1; i++) {
          report       += "\nEdge: " + reportIds[i];
          }
          report       += "\nTimestamp: " + tokens[tokens.length-1];
          out.writeInt(report.getBytes("UTF-8").length);
          out.write(report.getBytes("UTF-8"));
          count++;
        }
      } finally {
        scanner.close();
      }
      out.close();
      System.out.println("Count: " + count);
  }
}

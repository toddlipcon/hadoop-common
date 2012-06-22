package org.apache.hadoop.hdfs.qjournal;

import java.util.Arrays;

import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.io.DataOutputBuffer;

abstract class QJMTestUtil {

  static byte[] createTxnData(int startTxn, int numTxns) throws Exception {
    DataOutputBuffer buf = new DataOutputBuffer();
    FSEditLogOp.Writer writer = new FSEditLogOp.Writer(buf);
    
    for (long txid = startTxn; txid < startTxn + numTxns; txid++) {
      FSEditLogOp op = NameNodeAdapter.createMkdirOp("tx " + txid);
      op.setTransactionId(txid);
      writer.writeOp(op);
    }
    
    return Arrays.copyOf(buf.getData(), buf.getLength());
  }
  
}

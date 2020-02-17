package io.transwarp.geo.udf.old;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.List;

public class ToLongListUDF extends UDF {

  public List<Long> evaluate(long val) {
    List<Long> list = new ArrayList<Long>();
    list.add(val);
    list.add(val);
    list.add(val);
    return list;
  }
}

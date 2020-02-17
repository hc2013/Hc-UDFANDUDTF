package io.transwarp.geo.udf.old;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.List;

public class ToStringListUDF extends UDF {

  public List<String> evaluate(String val) {
    List<String> list = new ArrayList();
    list.add(val);
    list.add(val);
    list.add(val);
    return list;
  }

}

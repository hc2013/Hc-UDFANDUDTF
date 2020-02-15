package io.transwarp.geo.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.List;

public class ToFloatListUDF extends UDF {

  public List<Float> evaluate(float val) {
    List<Float> list = new ArrayList();
    list.add(val);
    list.add(val);
    list.add(val);
    return list;
  }

}

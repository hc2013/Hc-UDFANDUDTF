package io.transwarp.geo.udf.old;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;
import java.util.List;

public class TestTypeUDTF extends GenericUDTF {
  private final int argsCount = 3;
  //private ObjectInspector[] inputOIs;
  //private ShowByRegionApi showByRegionApi = new ShowByRegionApi();
  private Object forwardColObj[] = new Object[argsCount];

  public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
    if (argOIs.length != argsCount) {
      throw new UDFArgumentException(new IllegalArgumentException("there must be "
        + argsCount + " parameter for ShowByRegionUDF"));
    }
    //暂时不加type检查
    //inputOIs = argOIs;

    List<String> outFieldNames = new ArrayList<String>();
    List<ObjectInspector> outFieldOIs = new ArrayList<ObjectInspector>();
    outFieldNames.add("long_val");
    outFieldNames.add("float_val");
    outFieldNames.add("str_val");
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
    outFieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaFloatObjectInspector));
    outFieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaStringObjectInspector));
    return ObjectInspectorFactory.getStandardStructObjectInspector(outFieldNames, outFieldOIs);
  }

  public void process(Object[] args) throws HiveException {
    List<Long> longList = (List<Long>) ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaLongObjectInspector).getList(args[0]);
    List<Float> floatList = (List<Float>) ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaFloatObjectInspector).getList(args[1]);
    List<String> strList = (List<String>) ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaStringObjectInspector).getList(args[1]);

    forwardColObj[0] = longList.get(0);
    forwardColObj[1] = floatList;
    forwardColObj[2] = strList;
    forward(forwardColObj);
  }

  public void close() throws HiveException {
    System.out.println("Close ShowByRegionUDF");
  }
}

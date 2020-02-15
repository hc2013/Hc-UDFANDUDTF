package io.transwarp.geo.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

public class ShowByRegionMock2 extends GenericUDTF {

  private final int argsCount = 8;
  private final int retCount = 8;
  private transient Object forwardColObj[] = new Object[retCount];

  public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
    if (argOIs.length != argsCount) {
      throw new UDFArgumentException(new IllegalArgumentException("there must be "
        + argsCount + " parameter for ShowByRegionUDF"));
    }
    //暂时不加type检查

    List<String> outFieldNames = new ArrayList<String>();
    List<ObjectInspector> outFieldOIs = new ArrayList<ObjectInspector>();
    outFieldNames.add("latest_sh_time");
    outFieldNames.add("pass_city");
    outFieldNames.add("pass_time");
    outFieldNames.add("earliest_city_time");
    outFieldNames.add("latest_city_time");
    outFieldNames.add("duration_time");
    outFieldNames.add("speed");
    outFieldNames.add("transportation");
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
    outFieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaStringObjectInspector));
    outFieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaStringObjectInspector));
    outFieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaLongObjectInspector));
    outFieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaLongObjectInspector));
    outFieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaLongObjectInspector));
    outFieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaFloatObjectInspector));
    outFieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaStringObjectInspector));
    return ObjectInspectorFactory.getStandardStructObjectInspector(outFieldNames, outFieldOIs);
  }

  public void process(Object[] args) throws HiveException {

    long l = 111L;
    List<String> strs = new ArrayList<String>();
    strs.add("ok");
    List<Long> longs = new ArrayList<Long>();
    longs.add(333L);
    List<Float> floats = new ArrayList<Float>();
    floats.add(11.11f);
    forwardColObj[0] = l;
    forwardColObj[1] = strs;
    forwardColObj[2] = strs;
    forwardColObj[3] = longs;
    forwardColObj[4] = longs;
    forwardColObj[5] = longs;
    forwardColObj[6] = floats;
    forwardColObj[7] = strs;
    forward(forwardColObj);
  }

  public void close() throws HiveException {
    System.out.println("Close ShowByRegionUDF");
  }

}

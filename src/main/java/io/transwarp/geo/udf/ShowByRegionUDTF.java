package io.transwarp.geo.udf;

import io.transwarp.geo.udf.api.ShowByRegionApiV3;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * created by hanchun on 2020-02-14
 */
public class ShowByRegionUDTF extends GenericUDTF {

  private final int argsCount = 8;
  private final int retCount = 8;
  private ShowByRegionApiV3 showByRegionApi = new ShowByRegionApiV3();
  private Object forwardColObj[] = new Object[retCount];
  private ListObjectInspector[] lois = new ListObjectInspector[argsCount];

  public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
    if (argOIs.length != argsCount) {
      throw new UDFArgumentException(new IllegalArgumentException("there must be "
        + argsCount + " parameter for ShowByRegionUDF"));
    }
    //type检查
    for (int i = 0; i < argsCount; i++) {
      if (!argOIs[i].getCategory().equals(ObjectInspector.Category.LIST)) {
        throw new UDFArgumentException(new IllegalArgumentException("all parameter must be list type!"));
      }
      ListObjectInspector loi = (ListObjectInspector)argOIs[i];
      if (i == 0 && loi.getListElementObjectInspector().getTypeName().equals("bigint")) {
        loi = ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaLongObjectInspector);
      }

      if (i == 1 && loi.getListElementObjectInspector().getTypeName().equals("float")) {
        loi = ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
      }

      if (i == 2 && loi.getListElementObjectInspector().getTypeName().equals("float")) {
        loi = ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
      }

      if (i >= 3 && i <=7 && loi.getListElementObjectInspector().getTypeName().equals("string")) {
        loi = ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      }
      lois[i] = loi;
    }

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

    List timeList = lois[0].getList(args[0]);
    List lonList = lois[1].getList(args[1]);
    List latList = lois[2].getList(args[2]);
    List provIdList = lois[3].getList(args[3]);
    List areaIdList = lois[4].getList(args[4]);
    List districtIdList = lois[5].getList(args[5]);
    List lacList = lois[6].getList(args[6]);
    List ciList = lois[7].getList(args[7]);

    //type check and cast
    ShowByRegionApiV3.ListResult result = showByRegionApi.showByRegion(timeList, lonList, latList,
      provIdList, areaIdList, districtIdList, lacList, ciList);
    forwardColObj[0] = result.getLatest_sh_time();
    forwardColObj[1] = result.getPass_city();
    forwardColObj[2] = result.getPass_time();
    forwardColObj[3] = result.getEarliest_city_time();
    forwardColObj[4] = result.getLatest_city_time();
    forwardColObj[5] = result.getDuration_time();
    forwardColObj[6] = result.getSpeed();
    forwardColObj[7] = result.getTransportation();
    forward(forwardColObj);
  }

  public void close() throws HiveException {
    System.out.println("Close ShowByRegionUDF");
  }

}

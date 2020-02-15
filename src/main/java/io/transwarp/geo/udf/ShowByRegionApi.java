package io.transwarp.geo.udf;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
/**
 * Created by hang.li@transwarp.io on 20-2-14.
 */
public class ShowByRegionApi {

    public ShowByRegionApi() {
    }

    public ListResult showByRegion(List<Long> time_list, List<Float> longitude_list
      , List<Float> latitude_list, List<Text> prov_id_list, List<Text> area_id_list,
                                   List<Text> district_id_list, List<Text> lac_list, List<Text> ci_list) {

        List<ShowByRegionResult> resultList = new ArrayList();

        if (time_list.size() < 1) {
            return null;    //Check If Trajectory Is Empty
        }

        long sh_time = 0L;
        //Initialize start result @ area_id_list
        String regionPreviousStr = area_id_list.get(0).toString();
        Float[] previousLoc = new Float[]{longitude_list.get(0), latitude_list.get(0)};
        if (regionPreviousStr.equals("031")) {
            sh_time = time_list.get(0);
        }
        ShowByRegionResult result = new ShowByRegionResult(regionPreviousStr, time_list.get(0));
        float sectionDistance = 0.0F;
        for (int i = 1; i < time_list.size(); i++) {

            String regionCurrentStr = area_id_list.get(i).toString();
            if (regionPreviousStr.equals("031")) {
                sh_time = time_list.get(i);
            }

            if (regionCurrentStr.equals(regionPreviousStr)) {
                //Accumulate distance
                sectionDistance += calculateDistance(previousLoc, longitude_list.get(i), latitude_list.get(i));
            } else {
                //wrap info in result
                wrapResult(result, time_list.get(i - 1), sectionDistance);
                resultList.add(result);
                result = new ShowByRegionResult(regionCurrentStr, time_list.get(i));
                regionPreviousStr = area_id_list.get(i).toString();
                sectionDistance = 0.0F;
            }

            previousLoc = new Float[]{longitude_list.get(i), latitude_list.get(i)};
        }

        if (result.getComeOut() == 0L) {
            wrapResult(result, time_list.get(time_list.size() - 1), sectionDistance);
            resultList.add(result);
        }

        ListResult answer = new ListResult();
        for (ShowByRegionResult show : resultList) {
            answer.addField(show);
        }
        answer.setLatest_sh_time(sh_time);
        return answer;
    }

    private float calculateDistance(Float[] pre, float x, float y) {
        double dx = pre[0] - x;
        double dy = pre[1] - y;
        return (float) Math.sqrt(dx * dx + dy * dy);
    }

    private void wrapResult(ShowByRegionResult result, long outTime, float distance) {
        result.setComeOut(outTime);
        result.setTotalDistance(distance);

        float sectionTimeInH = ((float) result.getDuration()) / 3600000;
        float distanceInKM = result.getTotalDistance() * 78.71F;
        result.setSpeed(distanceInKM / sectionTimeInH);
        if (result.getDuration() != 0L) {
            if (result.getSpeed() < 10.0F) {
                result.setTransportation("walk");
            } else if (result.getSpeed() < 80.0F) {
                result.setTransportation("car");
            } else if (result.getSpeed() < 320.0F) {
                result.setTransportation("high-speed rail");
            }
        } else {
            result.setTransportation("N/A");
        }

        if (sectionTimeInH < 9) {
            result.setPass_time("pass");
        } else {
            result.setPass_time("stay");
        }
    }

    public static class ShowByRegionResult {
        private String regionId;
        private long comeIn = 0L;
        private long comeOut = 0L;
        private long duration = 0L;
        private long latest_sh_time;
        private String pass_time = "N/A";
        private float totalDistance = 0.0F;
        private float speed = 0.0F;
        private String transportation = "N/A";

        public ShowByRegionResult(String regionId, long comeIn) {
            this.regionId = regionId;
            this.comeIn = comeIn;
        }

        public void setComeOut(long comeOut) {
            //Add transportation & judgement like N/A in duration
            this.comeOut = comeOut;
            this.duration = comeOut - comeIn;
        }

        private void setSpeed(float speed) {
            this.speed = speed;
        }

        public float getSpeed() {
            return speed;
        }

        private void addDistance(double sectionDistance) {
            totalDistance += sectionDistance;
        }

        public String getTransportation() {
            return transportation;
        }

        public void setTransportation(String transportation) {
            this.transportation = transportation;
        }

        public long getDuration() {
            return duration;
        }

        public String getRegionId() {
            return regionId;
        }

        public long getComeIn() {
            return comeIn;
        }

        public long getComeOut() {
            return comeOut;
        }

        public float getTotalDistance() {
            return totalDistance;
        }

        public void setTotalDistance(float totalDistance) {
            this.totalDistance = totalDistance;
        }

        public long getLatest_sh_time() {
            return latest_sh_time;
        }

        public void setLatest_sh_time(long latest_sh_time) {
            this.latest_sh_time = latest_sh_time;
        }

        public String getPass_time() {
            return pass_time;
        }

        public void setPass_time(String pass_time) {
            this.pass_time = pass_time;
        }
    }

    public static class ListResult {
        private long latest_sh_time = 0L;
        private List<String> pass_city = new ArrayList();
        private List<String> pass_time = new ArrayList();
        private List<Long> earliest_city_time = new ArrayList();
        private List<Long> latest_city_time = new ArrayList();
        private List<Long> duration_time = new ArrayList();
        private List<Float> speed = new ArrayList();
        private List<String> transportation = new ArrayList();

        public void addField(ShowByRegionResult show) {
            pass_city.add(show.getRegionId());
            pass_time.add(show.pass_time);
            earliest_city_time.add(show.comeIn);
            latest_city_time.add(show.comeOut);
            duration_time.add(show.duration);
            speed.add(show.speed);
            transportation.add(show.transportation);
        }

        public long getLatest_sh_time() {
            return latest_sh_time;
        }

        public void setLatest_sh_time(long latest_sh_time) {
            this.latest_sh_time = latest_sh_time;
        }

        public List<String> getPass_city() {
            return pass_city;
        }

        public void setPass_city(List<String> pass_city) {
            this.pass_city = pass_city;
        }

        public List<String> getPass_time() {
            return pass_time;
        }

        public void setPass_time(List<String> pass_time) {
            this.pass_time = pass_time;
        }

        public List<Long> getEarliest_city_time() {
            return earliest_city_time;
        }

        public void setEarliest_city_time(List<Long> earliest_city_time) {
            this.earliest_city_time = earliest_city_time;
        }

        public List<Long> getLatest_city_time() {
            return latest_city_time;
        }

        public void setLatest_city_time(List<Long> latest_city_time) {
            this.latest_city_time = latest_city_time;
        }

        public List<Long> getDuration_time() {
            return duration_time;
        }

        public void setDuration_time(List<Long> duration_time) {
            this.duration_time = duration_time;
        }

        public List<Float> getSpeed() {
            return speed;
        }

        public void setSpeed(List<Float> speed) {
            this.speed = speed;
        }

        public List<String> getTransportation() {
            return transportation;
        }

        public void setTransportation(List<String> transportation) {
            this.transportation = transportation;
        }
    }

}
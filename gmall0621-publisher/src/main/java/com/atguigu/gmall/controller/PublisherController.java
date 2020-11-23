package com.atguigu.gmall.controller;

import com.atguigu.gmall.service.ESService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    private ESService esService;

    @GetMapping("realtime-total")
    public Object realtimeTotal(String date) {

        List<Map<String, Object>> rsList = new ArrayList<>();
        Map<String, Object> dauMap = new HashMap();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        Long dauTotal = esService.getDauTotal(date);
        if (dauTotal != null) {
            dauMap.put("value", dauTotal);
        } else {
            dauMap.put("value", 0L);
        }

        rsList.add(dauMap);

        Map<String, Object> newMidMap = new HashMap();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 2333);
        rsList.add(newMidMap);
        return rsList;
    }

    @RequestMapping("realtime-hour")
    public Object realtimeHour(@RequestParam(value = "id", defaultValue = "-1") String id, @RequestParam("date") String date) {
        Map<String, Map> hourMap = new HashMap<>();
        if ("dau".equals(id)) {
            //封装返回的数据
            //获取今天日活分时
            Map dauHourTdMap = esService.getDauHour(date);
            hourMap.put("today", dauHourTdMap);
            //获取昨天日期
            String yd = getYd(date);
            //获取昨天日活分时
            Map dauHourYdMap = esService.getDauHour(yd);
            hourMap.put("yesterday", dauHourYdMap);
        }
        return hourMap;
    }

    private String getYd(String td) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yd = null;
        try {
            Date tdDate = dateFormat.parse(td);
            Date ydDate = DateUtils.addDays(tdDate, -1);
            yd = dateFormat.format(ydDate);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("日期格式转变失败");
        }
        return yd;
    }


}

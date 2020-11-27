package com.atguigu.gmall.controller;

import com.atguigu.gmall.service.ClickHouseService;
import com.atguigu.gmall.service.ESService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    private ESService esService;
    @Autowired
    private ClickHouseService clickHouseService;


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

        //新增交易额
        Map<String,Object> orderAmountMap = new HashMap();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        orderAmountMap.put("value", clickHouseService.getOrderAmount(date));

        rsList.add(orderAmountMap);



        return rsList;
    }

    @RequestMapping("realtime-hour")
    public Object realtimeHour(@RequestParam(value = "id", defaultValue = "-1") String id, @RequestParam("date") String date) {
        Map<String, Map> resultMap = new HashMap<>();
        //获取昨天日期
        String yd = getYd(date);
        if ("dau".equals(id)) {
            //封装返回的数据
            //获取今天日活分时
            Map dauHourTdMap = esService.getDauHour(date);
            resultMap.put("today", dauHourTdMap);
            //获取昨天日活分时
            Map dauHourYdMap = esService.getDauHour(yd);
            resultMap.put("yesterday", dauHourYdMap);
        }else if("order_amount".equals(id)){
            Map orderAmountHourMapTD = clickHouseService.getOrderAmountHour(date);
            Map orderAmountHourMapYD = clickHouseService.getOrderAmountHour(yd);

            resultMap.put("yesterday",orderAmountHourMapYD);
            resultMap.put("today",orderAmountHourMapTD);
        }

        return resultMap;
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

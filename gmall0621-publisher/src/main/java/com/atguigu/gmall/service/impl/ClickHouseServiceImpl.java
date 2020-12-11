package com.atguigu.gmall.service.impl;

import com.atguigu.gmall.mapper.OrderWideMapper;
import com.atguigu.gmall.service.ClickHouseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ClickHouseServiceImpl implements ClickHouseService {

    @Autowired
    private OrderWideMapper orderWideMapper;

    @Override
    public BigDecimal getOrderAmount(String date) {
        return orderWideMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map<String, BigDecimal> getOrderAmountHour(String date) {
        List<Map> mapList = orderWideMapper.selectOrderAmountHourMap(date);
        Map<String, BigDecimal> orderAmountHourMap = new HashMap();
        for (Map map : mapList) {
            orderAmountHourMap.put(String.format("%04d", map.get("hr")), (BigDecimal) map.get("am"));
        }
        return orderAmountHourMap;
    }
}

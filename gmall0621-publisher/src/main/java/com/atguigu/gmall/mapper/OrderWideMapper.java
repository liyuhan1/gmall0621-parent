package com.atguigu.gmall.mapper;

import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface OrderWideMapper {

    //查询当日交易额总数
    public BigDecimal selectOrderAmountTotal(String date);

    //查询当日交易额分时明细
    public List<Map> selectOrderAmountHourMap(String date);

}

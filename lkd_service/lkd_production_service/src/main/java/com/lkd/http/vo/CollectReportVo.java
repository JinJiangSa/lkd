package com.lkd.http.vo;

import lombok.Data;

@Data
public class CollectReportVo {
    //完成数
    private Integer finishCount;
    //进行中总数
    private Integer progressCount;
    //取消的工单数
    private Integer cancelCount;
    //发生日期
    private String collectDate;

}

package com.lkd.http.vo;

import lombok.Data;

import java.io.Serializable;
@Data
public class TaskReportInfoVO  implements Serializable {

    /**
     * 工单总数
     */
    private Integer total;
    /**
     * 完成总数
     */
    private Integer completedTotal;
    /**
     * 拒绝总数
     */
    private Integer cancelTotal;

    /**
     * 进行中总数
     */
    private Integer progressTotal;

    /**
     * 工作人数
     */
    private Integer workerCount;

    /**
     * 是否是运维工单统计
     */
    private boolean repair;

    /**
     * 日期
     */
    private String date;


}

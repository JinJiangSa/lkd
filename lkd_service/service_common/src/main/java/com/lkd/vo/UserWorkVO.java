package com.lkd.vo;

import lombok.Data;

import java.io.Serializable;
@Data
public class UserWorkVO implements Serializable {

    /**
     * 用户Id
     */
    private Integer userId;


    /**
     * 手机号
     */
    private String mobile;

    /**
     * 用户名
     */
    private String userName;

    /**
     * 用户角色名
     */
    private String roleName;

    /**
     * 完成工单数
     */
    private Integer workCount;

    /**
     * 当日进行中的工单
     */
    private Integer progressTotal;

    /**
     * 拒绝工单数
     */
    private Integer cancelCount;

    /**
     * 总工单数
     */
    private Integer total;


}

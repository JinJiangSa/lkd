package com.lkd.service;
import com.baomidou.mybatisplus.extension.service.IService;
import com.lkd.entity.TaskEntity;
import com.lkd.entity.TaskStatusTypeEntity;
import com.lkd.exception.LogicException;
import com.lkd.http.vo.CancelTaskViewModel;
import com.lkd.http.vo.CollectReportVo;
import com.lkd.http.vo.TaskReportInfoVO;
import com.lkd.http.vo.TaskViewModel;
import com.lkd.vo.Pager;
import com.lkd.vo.UserWorkVO;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;

/**
 * 工单业务逻辑
 */
public interface TaskService extends IService<TaskEntity> {


    /**
     * 通过条件搜索工单列表
     * @param pageIndex
     * @param pageSize
     * @param innerCode
     * @param userId
     * @param taskCode
     * @param isRepair 是否是运维工单
     * @return
     */
    Pager<TaskEntity> search(Long pageIndex, Long pageSize, String innerCode, Integer userId, String taskCode, Integer status, Boolean isRepair, String start, String end);





    /**
     * 获取所有状态类型
     * @return
     */
    List<TaskStatusTypeEntity> getAllStatus();


    /**
     * 创建工单
     * @param taskViewModel
     * @return
     */
    boolean createTask(TaskViewModel taskViewModel) throws LogicException;


    /**
     * 接受工单
     * @param id
     * @return
     */
    boolean accept(Long id);

    /**
     * 取消工单
     * @param id
     * @param cancelVM
     * @return
     */
    boolean cancelTask(Long id, CancelTaskViewModel cancelVM);

    /**
     * 完成工单
     * @param id
     * @return
     */
    boolean completeTask(long id);

    /**
     * 工单数统计
     * @param start
     * @param end
     * @return
     */
    List<TaskReportInfoVO> taskStatics(LocalDateTime start, LocalDateTime end);

    /**
     * 人效排名月度统计
     * @param start
     * @param end
     * @param isRepair
     * @param regionId
     * @return
     */
    List<UserWorkVO> userWorkTop10(String start, String end, Boolean isRepair, Integer regionId);

    /**
     * 工单状态统计
     * @param start
     * @param end
     * @return
     */
    List<CollectReportVo> collectReport(String start, String end);

    /**
     * 获取用户工作量
     * @param userId
     * @param start
     * @param end
     * @return
     */
    UserWorkVO userWork(Integer userId, String start, String end);

    /**
     * 更新工单量列表
     * @param taskEntity
     * @param score
     */
    void updateTaskZSet(TaskEntity taskEntity,int score);

    /**
     * 获取同一天内分配的工单最少的人
     * @param regionId
     * @param isRepair
     * @return
     */
    int getLeastUser(Long regionId,Boolean isRepair);
}

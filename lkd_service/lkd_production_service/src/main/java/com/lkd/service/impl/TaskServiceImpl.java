package com.lkd.service.impl;

import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.google.common.base.Strings;
import com.lkd.common.VMSystem;
import com.lkd.config.TopicConfig;
import com.lkd.contract.SupplyChannel;
import com.lkd.contract.SupplyContract;
import com.lkd.contract.TaskCompleteContract;
import com.lkd.dao.TaskCollectDao;
import com.lkd.dao.TaskDao;
import com.lkd.emq.MqttProducer;
import com.lkd.entity.TaskCollectEntity;
import com.lkd.entity.TaskDetailsEntity;
import com.lkd.entity.TaskEntity;
import com.lkd.entity.TaskStatusTypeEntity;
import com.lkd.exception.LogicException;
import com.lkd.feign.UserService;
import com.lkd.feign.VMService;
import com.lkd.http.controller.BaseController;
import com.lkd.http.view.TokenObject;
import com.lkd.http.vo.CancelTaskViewModel;
import com.lkd.http.vo.CollectReportVo;
import com.lkd.http.vo.TaskReportInfoVO;
import com.lkd.http.vo.TaskViewModel;
import com.lkd.service.TaskDetailsService;
import com.lkd.service.TaskService;
import com.lkd.service.TaskStatusTypeService;
import com.lkd.vo.Pager;
import com.lkd.vo.UserVO;
import com.lkd.vo.UserWorkVO;
import com.lkd.vo.VmVO;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.servlet.http.HttpServletRequest;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class TaskServiceImpl extends ServiceImpl<TaskDao, TaskEntity> implements TaskService {
    @Autowired
    private TaskDao taskDao;

    @Autowired
    private TaskCollectDao taskCollectDao;

    @Autowired
    private HttpServletRequest request; //自动注入request

    @Autowired
    private TaskStatusTypeService statusTypeService;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private VMService vmService;

    @Autowired
    private UserService userService;

    @Autowired
    private TaskDetailsService taskDetailsService;


    @Override
    public Pager<TaskEntity> search(Long pageIndex, Long pageSize, String innerCode, Integer userId, String taskCode, Integer status, Boolean isRepair, String start, String end) {
        Page<TaskEntity> page = new Page<>(pageIndex, pageSize);
        LambdaQueryWrapper<TaskEntity> qw = new LambdaQueryWrapper<>();
        if (!Strings.isNullOrEmpty(innerCode)) {
            qw.eq(TaskEntity::getInnerCode, innerCode);
        }
        if (userId != null && userId > 0) {
            qw.eq(TaskEntity::getUserId, userId);
        }
        if (!Strings.isNullOrEmpty(taskCode)) {
            qw.like(TaskEntity::getTaskCode, taskCode);
        }
        if (status != null && status > 0) {
            qw.eq(TaskEntity::getTaskStatus, status);
        }
        if (isRepair != null) {
            if (isRepair) {
                qw.ne(TaskEntity::getProductTypeId, VMSystem.TASK_TYPE_SUPPLY);
            } else {
                qw.eq(TaskEntity::getProductTypeId, VMSystem.TASK_TYPE_SUPPLY);
            }
        }
        if (!Strings.isNullOrEmpty(start) && !Strings.isNullOrEmpty(end)) {
            qw
                    .ge(TaskEntity::getCreateTime, LocalDate.parse(start, DateTimeFormatter.ISO_LOCAL_DATE))
                    .le(TaskEntity::getCreateTime, LocalDate.parse(end, DateTimeFormatter.ISO_LOCAL_DATE));
        }
        //根据最后更新时间倒序排序
        qw.orderByDesc(TaskEntity::getUpdateTime);

        return Pager.build(this.page(page, qw));
    }


    @Override
    public List<TaskStatusTypeEntity> getAllStatus() {
        QueryWrapper<TaskStatusTypeEntity> qw = new QueryWrapper<>();
        qw.lambda()
                .ge(TaskStatusTypeEntity::getStatusId, VMSystem.TASK_STATUS_CREATE);

        return statusTypeService.list(qw);
    }


    @Transactional
    @Override
    public boolean createTask(TaskViewModel taskViewModel) throws LogicException {
        //跨服务查询售货机微服务，得到售货机的地址和区域id
        VmVO vm = vmService.getVMInfo(taskViewModel.getInnerCode());
        if (vm == null) {
            throw new LogicException("该机器不存在");
        }
        //如果是投放工单，状态为运营则抛出异常
        if (VMSystem.TASK_TYPE_DEPLOY.equals(taskViewModel.getProductType())) {
            if (VMSystem.VM_STATUS_RUNNING.equals(vm.getVmStatus())) {
                throw new LogicException("此售货机为运营状态，不能投放");
            }
        }
        // 如果是补货工单，状态不是运营状态则抛出异常
        if (VMSystem.TASK_TYPE_SUPPLY.equals(taskViewModel.getProductType())) {
            if (!VMSystem.VM_STATUS_RUNNING.equals(vm.getVmStatus())) {
                throw new LogicException("此售货机不为运营状态，不能补货");
            }
        }
        // 如果是撤机工单，状态不是运营状态则抛出异常
        if (VMSystem.TASK_TYPE_REVOKE.equals(taskViewModel.getProductType())) {
            if (!VMSystem.VM_STATUS_RUNNING.equals(vm.getVmStatus())) {
                throw new LogicException("此售货机不为运营状态，不能撤机");
            }
        }
        //根据售货机编码获取到所有工单
        LambdaQueryWrapper<TaskEntity> taskLambdaQueryWrapper = new LambdaQueryWrapper<>();
        taskLambdaQueryWrapper.eq(TaskEntity::getInnerCode, vm.getInnerCode());
        List<TaskEntity> taskEntities = taskDao.selectList(taskLambdaQueryWrapper);
        for (TaskEntity taskEntity : taskEntities) {
            //校验这台设备是否有未完成的同类型工单，如果存在则不能创建
            if (taskEntity.getTaskStatus().equals(taskViewModel.getProductType())) {
                throw new LogicException("有未完成的同类型工单，不能创建工单");
            }
        }
        //跨服务查询用户微服务，得到用户名
        UserVO user = userService.getUser(taskViewModel.getUserId());
        if (user == null) {
            throw new LogicException("该用户不存在");
        }

        //新增工单表记录
        TaskEntity taskEntity = new TaskEntity();
        taskEntity.setTaskCode(generateTaskCode());//工单编号
        BeanUtils.copyProperties(taskViewModel, taskEntity);//复制属性
        taskEntity.setTaskStatus(VMSystem.TASK_STATUS_CREATE);//工单状态
        taskEntity.setProductTypeId(taskViewModel.getProductType());//工单类型

        taskEntity.setAddr(vm.getNodeAddr());//地址
        taskEntity.setRegionId(vm.getRegionId());//区域
        taskEntity.setUserName(user.getUserName());//用户名
        this.save(taskEntity);
        //如果是补货工单，向 工单明细表插入记录
        if (taskEntity.getProductTypeId() == VMSystem.TASK_TYPE_SUPPLY) {
            taskViewModel.getDetails().forEach(d -> {
                TaskDetailsEntity detailsEntity = new TaskDetailsEntity();
                BeanUtils.copyProperties(d, detailsEntity);
                detailsEntity.setTaskId(taskEntity.getTaskId());
                taskDetailsService.save(detailsEntity);
            });
        }
        updateTaskZSet(taskEntity, 1);

        return true;
    }


    /**
     * 生成工单编号
     *
     * @return
     */
    private String generateTaskCode() {
        //日期+序号
        String date = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));  //日期字符串
        String key = "lkd.task.code." + date; //redis key
        Object obj = redisTemplate.opsForValue().get(key);
        if (obj == null) {
            redisTemplate.opsForValue().set(key, 1L, Duration.ofDays(1));
            return date + "0001";
        }
        return date + Strings.padStart(redisTemplate.opsForValue().increment(key, 1).toString(), 4, '0');
    }


    @Override
    public boolean accept(Long id) {
        TaskEntity task = this.getById(id);  //查询工单

        //判断工单状态是否为已完成
        if (!task.getTaskStatus().equals(VMSystem.TASK_STATUS_PROGRESS)) {
            throw new LogicException("工单不为待处理状态");
        }
        String userIdStr = request.getHeader("userId");
        Integer userId = Integer.parseInt(userIdStr);
        //判断当前工单的执行人是否为当前登录用户
        if (!userId.equals(task.getUserId())) {
            throw new LogicException("当前工单的执行人不为当前登录用户");
        }

        task.setTaskStatus(VMSystem.TASK_STATUS_PROGRESS);//修改工单状态为进行
        return this.updateById(task);
    }

    @Override
    public boolean cancelTask(Long id, CancelTaskViewModel cancelVM) {
        TaskEntity task = this.getById(id);  //查询工单

        //判断工单是否已完成，如果已经完成则不能取消工单
        if (task.getTaskStatus().equals(VMSystem.TASK_STATUS_FINISH)) {
            throw new LogicException("工单已完成，不能取消工单");
        }
        String userIdStr = request.getHeader("userId");
        Integer userId = Integer.parseInt(userIdStr);
        //判断当前工单的执行人是否为当前登录用户
        if (!userId.equals(task.getUserId())) {
            throw new LogicException("当前工单的执行人不为当前登录用户");
        }

        task.setTaskStatus(VMSystem.TASK_STATUS_CANCEL);
        task.setDesc(cancelVM.getDesc());

        updateTaskZSet(task, -1);
        return this.updateById(task);
    }

    @Autowired
    private MqttProducer mqttProducer;

    @Override
    public boolean completeTask(long id) {
        TaskEntity taskEntity = this.getById(id);
        if (VMSystem.TASK_STATUS_FINISH.equals(taskEntity.getTaskStatus()) || VMSystem.TASK_STATUS_CANCEL.equals(taskEntity.getTaskStatus())) {
            throw new LogicException("当前工单已完成或者已取消");
        }
        String userIdStr = request.getHeader("userId");
        Integer userId = Integer.parseInt(userIdStr);
        //判断当前工单的执行人是否为当前登录用户
        if (!userId.equals(taskEntity.getUserId())) {
            throw new LogicException("当前工单的执行人不为当前登录用户");
        }
        taskEntity.setTaskStatus(VMSystem.TASK_STATUS_FINISH);//修改工单状态
        this.updateById(taskEntity);

        if (taskEntity.getProductTypeId().equals(VMSystem.TASK_TYPE_SUPPLY)) {
            SupplyContract supplyContract = new SupplyContract();
            List<TaskDetailsEntity> taskDetailsEntities = taskDetailsService.getByTaskId(taskEntity.getTaskId());
            List<SupplyChannel> supplyChannelList = new ArrayList<>();
            for (TaskDetailsEntity taskDetailsEntity : taskDetailsEntities) {
                SupplyChannel supplyChannel = new SupplyChannel();
                BeanUtils.copyProperties(taskDetailsEntity, supplyChannel);
                supplyChannel.setChannelId(taskDetailsEntity.getChannelCode());
                supplyChannel.setCapacity(taskDetailsEntity.getExpectCapacity());
                supplyChannelList.add(supplyChannel);
            }
            supplyContract.setSupplyData(supplyChannelList);

            //说明是运营补货工单
            mqttProducer.send(TopicConfig.VMS_SUPPLY_TOPIC, JSON.toJSONString(supplyContract));
        } else {
            //运维工单
            TaskCompleteContract taskCompleteContract = new TaskCompleteContract();
            taskCompleteContract.setInnerCode(taskEntity.getInnerCode());
            taskCompleteContract.setTaskType(VMSystem.TASK_STATUS_FINISH);
            mqttProducer.send(TopicConfig.VMS_COMPLETED_TOPIC, JSON.toJSONString(taskCompleteContract));
        }


        return true;
    }

    @Override
    public List<TaskReportInfoVO> taskStatics(LocalDateTime start, LocalDateTime end) {

        TaskReportInfoVO repairer = getTaskReportInfoVO(start, end, true);
        TaskReportInfoVO operator = getTaskReportInfoVO(start, end, false);

        List<TaskReportInfoVO> list = new ArrayList<>();
        list.add(operator);
        list.add(repairer);
        return list;
    }

    private TaskReportInfoVO getTaskReportInfoVO(LocalDateTime start, LocalDateTime end, boolean repair) {
        LambdaQueryWrapper<TaskEntity> taskLambdaQueryWrapper = new LambdaQueryWrapper<>();
        if (!repair) {
            //运营工单
            taskLambdaQueryWrapper.eq(TaskEntity::getProductTypeId, VMSystem.TASK_TYPE_SUPPLY);
        } else {
            //运维工单
            taskLambdaQueryWrapper.ne(TaskEntity::getProductTypeId, VMSystem.TASK_TYPE_SUPPLY);
        }
        taskLambdaQueryWrapper.between(TaskEntity::getCreateTime, start, end);
        List<TaskEntity> taskEntities = taskDao.selectList(taskLambdaQueryWrapper);
        TaskReportInfoVO taskReportInfoVO = new TaskReportInfoVO();
        taskReportInfoVO.setTotal(taskEntities.size());
        Integer completedTotal = 0;
        Integer cancelTotal = 0;
        Integer progressTotal = 0;

        for (TaskEntity taskEntity : taskEntities) {
            //计算完成数
            if (VMSystem.TASK_STATUS_FINISH.equals(taskEntity.getTaskStatus())) completedTotal++;
            //计算取消数
            if (VMSystem.TASK_STATUS_CANCEL.equals(taskEntity.getTaskStatus())) cancelTotal++;
            //计算进行中总数
            if (VMSystem.TASK_STATUS_PROGRESS.equals(taskEntity.getTaskStatus())) progressTotal++;

        }
        taskReportInfoVO.setCompletedTotal(completedTotal);
        taskReportInfoVO.setCancelTotal(cancelTotal);
        taskReportInfoVO.setProgressTotal(progressTotal);
        if (repair) {
            //运维工单
            Integer repairerCount = userService.getRepairerCount();
            taskReportInfoVO.setWorkerCount(repairerCount);
            taskReportInfoVO.setRepair(repair);
        } else {
            //运营工单
            Integer operatorCount = userService.getOperatorCount();
            taskReportInfoVO.setWorkerCount(operatorCount);
            taskReportInfoVO.setRepair(repair);
        }
        taskReportInfoVO.setDate(new Date().toString());
        return taskReportInfoVO;
    }

    @Override
    public List<UserWorkVO> userWorkTop10(String start, String end, Boolean isRepair, Integer regionId) {
        LambdaQueryWrapper<TaskEntity> taskLambdaQueryWrapper = new LambdaQueryWrapper<>();
        Map<String, Integer> map = new HashMap<>();
        if (!isRepair) {
            //运营工单
            taskLambdaQueryWrapper.eq(TaskEntity::getProductTypeId, VMSystem.TASK_TYPE_SUPPLY);
        } else {
            //运维工单
            taskLambdaQueryWrapper.ne(TaskEntity::getProductTypeId, VMSystem.TASK_TYPE_SUPPLY);
        }
        taskLambdaQueryWrapper.between(TaskEntity::getCreateTime, start + " 00:00:01", end + " 23:59:59");
        taskLambdaQueryWrapper.eq(TaskEntity::getRegionId, regionId);
        List<TaskEntity> taskEntities = taskDao.selectList(taskLambdaQueryWrapper);
        List<UserWorkVO> userWorkVOList = new ArrayList<>();
        for (TaskEntity taskEntity : taskEntities) {
            String userName = taskEntity.getUserName();
            if (VMSystem.TASK_STATUS_FINISH.equals(taskEntity.getTaskStatus())) {
                if (map.containsKey(userName)) {
                    //已经存在
                    Integer workCount = map.get(userName);
                    workCount++;
                    //把已经出现的次数给覆盖掉
                    map.put(userName, workCount);
                } else {
                    //不存在
                    map.put(userName, 1);
                }
            }
        }
        for (String userName : map.keySet()) {
            UserWorkVO userWorkVO = new UserWorkVO();
            userWorkVO.setUserName(userName);
            Integer value = map.get(userName);
            userWorkVO.setWorkCount(value);
            userWorkVOList.add(userWorkVO);
        }
        userWorkVOList = userWorkVOList.stream().sorted(Comparator.comparing(UserWorkVO::getWorkCount).reversed()).collect(Collectors.toList());
        return userWorkVOList;
    }

    @Override
    public List<CollectReportVo> collectReport(String start, String end) {
        List<CollectReportVo> collectReportVoList = new ArrayList<>();
        String startTime = start + " 00:00:01";
        String endTime = end + " 23:59:59";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date startTime1 = format.parse(startTime);
            Date endTime1 = format.parse(endTime);
            Calendar tempStart = Calendar.getInstance();
            tempStart.setTime(endTime1);
            while (startTime1.getTime() <= endTime1.getTime()) {
                // list.add(format.format(endTime1));
                // System.out.println(format.format(endTime1));
                LambdaQueryWrapper<TaskEntity> taskLambdaQueryWrapper = new LambdaQueryWrapper<>();
                Date startTime2 = DateUtils.parseDate(DateFormatUtils.format(endTime1, "yy-MM-dd") + " 00:00:01", new String[]{"yy-MM-dd HH:mm:ss"});

                taskLambdaQueryWrapper.between(TaskEntity::getCreateTime, startTime2, endTime1);
                List<TaskEntity> taskEntities = taskDao.selectList(taskLambdaQueryWrapper);

                Integer finishCount = 0;
                Integer cancelCount = 0;
                Integer progressCount = 0;
                CollectReportVo collectReportVo = new CollectReportVo();
                for (TaskEntity taskEntity : taskEntities) {
                    //计算完成数
                    if (VMSystem.TASK_STATUS_FINISH.equals(taskEntity.getTaskStatus())) finishCount++;
                    //计算取消数
                    if (VMSystem.TASK_STATUS_CANCEL.equals(taskEntity.getTaskStatus())) cancelCount++;
                    //计算进行中总数
                    if (VMSystem.TASK_STATUS_PROGRESS.equals(taskEntity.getTaskStatus())) progressCount++;
                }
                collectReportVo.setFinishCount(finishCount);
                collectReportVo.setCancelCount(cancelCount);
                collectReportVo.setProgressCount(progressCount);
                collectReportVo.setCollectDate(format.format(endTime1));
                collectReportVoList.add(collectReportVo);
                tempStart.add(Calendar.DATE, -1);
                endTime1 = tempStart.getTime();
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return collectReportVoList;
    }

    @Override
    public UserWorkVO userWork(Integer userId, String start, String end) {
        String startTime = start + " 00:00:01";
        String endTime = end + " 23:59:59";
        LambdaQueryWrapper<TaskEntity> taskLambdaQueryWrapper = new LambdaQueryWrapper<>();
        taskLambdaQueryWrapper.eq(userId != null, TaskEntity::getUserId, userId);
        taskLambdaQueryWrapper.between(TaskEntity::getCreateTime, startTime, endTime);
        List<TaskEntity> taskEntities = taskDao.selectList(taskLambdaQueryWrapper);
        UserWorkVO userWorkVO = new UserWorkVO();
        userWorkVO.setTotal(taskEntities.size());
        if (userId != null) {
            userWorkVO.setUserId(userId);
        }
        Integer workCount = 0;
        Integer progressTotal = 0;
        Integer cancelCount = 0;
        for (TaskEntity taskEntity : taskEntities) {
            //计算完成数
            if (VMSystem.TASK_STATUS_FINISH.equals(taskEntity.getTaskStatus())) workCount++;
            //计算取消数
            if (VMSystem.TASK_STATUS_CANCEL.equals(taskEntity.getTaskStatus())) cancelCount++;
            //计算进行中总数
            if (VMSystem.TASK_STATUS_PROGRESS.equals(taskEntity.getTaskStatus())) progressTotal++;
        }
        userWorkVO.setWorkCount(workCount);
        userWorkVO.setCancelCount(cancelCount);
        userWorkVO.setProgressTotal(progressTotal);
        return userWorkVO;
    }

    /**
     * 更新工单量列表
     *
     * @param taskEntity
     * @param score
     */
    @Override
    public void updateTaskZSet(TaskEntity taskEntity, int score) {
        String roleCode = "1003";
        if (taskEntity.getProductTypeId().equals(VMSystem.TASK_TYPE_SUPPLY)) {
            //补货工单
            roleCode = "1002";
        }
        String key = VMSystem.REGION_TASK_KEY_PREF
                + LocalDate.now() + "."
                + taskEntity.getRegionId() + "."
                + roleCode;
        redisTemplate.opsForZSet().incrementScore(key, taskEntity.getUserId(), score);
    }

    @Override
    public int getLeastUser(Long regionId, Boolean isRepair) {
        String roleCode = "1002";
        if (isRepair) {
            //运维工单
            roleCode = "1003";
        }
        String key = VMSystem.REGION_TASK_KEY_PREF
                + LocalDate.now() + "."
                + regionId + "."
                + roleCode;

        //取第一个
        Set<Object> set = redisTemplate.opsForZSet().range(key, 0, -1);
        if(set == null || set.isEmpty()){
            return 0;
        }

        return (Integer) set.stream().collect(Collectors.toList()).get(0);
    }
}

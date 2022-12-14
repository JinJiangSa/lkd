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
    private HttpServletRequest request; //????????????request

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
        //????????????????????????????????????
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
        //?????????????????????????????????????????????????????????????????????id
        VmVO vm = vmService.getVMInfo(taskViewModel.getInnerCode());
        if (vm == null) {
            throw new LogicException("??????????????????");
        }
        //??????????????????????????????????????????????????????
        if (VMSystem.TASK_TYPE_DEPLOY.equals(taskViewModel.getProductType())) {
            if (VMSystem.VM_STATUS_RUNNING.equals(vm.getVmStatus())) {
                throw new LogicException("??????????????????????????????????????????");
            }
        }
        // ???????????????????????????????????????????????????????????????
        if (VMSystem.TASK_TYPE_SUPPLY.equals(taskViewModel.getProductType())) {
            if (!VMSystem.VM_STATUS_RUNNING.equals(vm.getVmStatus())) {
                throw new LogicException("?????????????????????????????????????????????");
            }
        }
        // ???????????????????????????????????????????????????????????????
        if (VMSystem.TASK_TYPE_REVOKE.equals(taskViewModel.getProductType())) {
            if (!VMSystem.VM_STATUS_RUNNING.equals(vm.getVmStatus())) {
                throw new LogicException("?????????????????????????????????????????????");
            }
        }
        //??????????????????????????????????????????
        LambdaQueryWrapper<TaskEntity> taskLambdaQueryWrapper = new LambdaQueryWrapper<>();
        taskLambdaQueryWrapper.eq(TaskEntity::getInnerCode, vm.getInnerCode());
        List<TaskEntity> taskEntities = taskDao.selectList(taskLambdaQueryWrapper);
        for (TaskEntity taskEntity : taskEntities) {
            //????????????????????????????????????????????????????????????????????????????????????
            if (taskEntity.getTaskStatus().equals(taskViewModel.getProductType())) {
                throw new LogicException("???????????????????????????????????????????????????");
            }
        }
        //????????????????????????????????????????????????
        UserVO user = userService.getUser(taskViewModel.getUserId());
        if (user == null) {
            throw new LogicException("??????????????????");
        }

        //?????????????????????
        TaskEntity taskEntity = new TaskEntity();
        taskEntity.setTaskCode(generateTaskCode());//????????????
        BeanUtils.copyProperties(taskViewModel, taskEntity);//????????????
        taskEntity.setTaskStatus(VMSystem.TASK_STATUS_CREATE);//????????????
        taskEntity.setProductTypeId(taskViewModel.getProductType());//????????????

        taskEntity.setAddr(vm.getNodeAddr());//??????
        taskEntity.setRegionId(vm.getRegionId());//??????
        taskEntity.setUserName(user.getUserName());//?????????
        this.save(taskEntity);
        //??????????????????????????? ???????????????????????????
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
     * ??????????????????
     *
     * @return
     */
    private String generateTaskCode() {
        //??????+??????
        String date = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));  //???????????????
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
        TaskEntity task = this.getById(id);  //????????????

        //????????????????????????????????????
        if (!task.getTaskStatus().equals(VMSystem.TASK_STATUS_PROGRESS)) {
            throw new LogicException("???????????????????????????");
        }
        String userIdStr = request.getHeader("userId");
        Integer userId = Integer.parseInt(userIdStr);
        //?????????????????????????????????????????????????????????
        if (!userId.equals(task.getUserId())) {
            throw new LogicException("????????????????????????????????????????????????");
        }

        task.setTaskStatus(VMSystem.TASK_STATUS_PROGRESS);//???????????????????????????
        return this.updateById(task);
    }

    @Override
    public boolean cancelTask(Long id, CancelTaskViewModel cancelVM) {
        TaskEntity task = this.getById(id);  //????????????

        //?????????????????????????????????????????????????????????????????????
        if (task.getTaskStatus().equals(VMSystem.TASK_STATUS_FINISH)) {
            throw new LogicException("????????????????????????????????????");
        }
        String userIdStr = request.getHeader("userId");
        Integer userId = Integer.parseInt(userIdStr);
        //?????????????????????????????????????????????????????????
        if (!userId.equals(task.getUserId())) {
            throw new LogicException("????????????????????????????????????????????????");
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
            throw new LogicException("????????????????????????????????????");
        }
        String userIdStr = request.getHeader("userId");
        Integer userId = Integer.parseInt(userIdStr);
        //?????????????????????????????????????????????????????????
        if (!userId.equals(taskEntity.getUserId())) {
            throw new LogicException("????????????????????????????????????????????????");
        }
        taskEntity.setTaskStatus(VMSystem.TASK_STATUS_FINISH);//??????????????????
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

            //???????????????????????????
            mqttProducer.send(TopicConfig.VMS_SUPPLY_TOPIC, JSON.toJSONString(supplyContract));
        } else {
            //????????????
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
            //????????????
            taskLambdaQueryWrapper.eq(TaskEntity::getProductTypeId, VMSystem.TASK_TYPE_SUPPLY);
        } else {
            //????????????
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
            //???????????????
            if (VMSystem.TASK_STATUS_FINISH.equals(taskEntity.getTaskStatus())) completedTotal++;
            //???????????????
            if (VMSystem.TASK_STATUS_CANCEL.equals(taskEntity.getTaskStatus())) cancelTotal++;
            //?????????????????????
            if (VMSystem.TASK_STATUS_PROGRESS.equals(taskEntity.getTaskStatus())) progressTotal++;

        }
        taskReportInfoVO.setCompletedTotal(completedTotal);
        taskReportInfoVO.setCancelTotal(cancelTotal);
        taskReportInfoVO.setProgressTotal(progressTotal);
        if (repair) {
            //????????????
            Integer repairerCount = userService.getRepairerCount();
            taskReportInfoVO.setWorkerCount(repairerCount);
            taskReportInfoVO.setRepair(repair);
        } else {
            //????????????
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
            //????????????
            taskLambdaQueryWrapper.eq(TaskEntity::getProductTypeId, VMSystem.TASK_TYPE_SUPPLY);
        } else {
            //????????????
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
                    //????????????
                    Integer workCount = map.get(userName);
                    workCount++;
                    //????????????????????????????????????
                    map.put(userName, workCount);
                } else {
                    //?????????
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
                    //???????????????
                    if (VMSystem.TASK_STATUS_FINISH.equals(taskEntity.getTaskStatus())) finishCount++;
                    //???????????????
                    if (VMSystem.TASK_STATUS_CANCEL.equals(taskEntity.getTaskStatus())) cancelCount++;
                    //?????????????????????
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
            //???????????????
            if (VMSystem.TASK_STATUS_FINISH.equals(taskEntity.getTaskStatus())) workCount++;
            //???????????????
            if (VMSystem.TASK_STATUS_CANCEL.equals(taskEntity.getTaskStatus())) cancelCount++;
            //?????????????????????
            if (VMSystem.TASK_STATUS_PROGRESS.equals(taskEntity.getTaskStatus())) progressTotal++;
        }
        userWorkVO.setWorkCount(workCount);
        userWorkVO.setCancelCount(cancelCount);
        userWorkVO.setProgressTotal(progressTotal);
        return userWorkVO;
    }

    /**
     * ?????????????????????
     *
     * @param taskEntity
     * @param score
     */
    @Override
    public void updateTaskZSet(TaskEntity taskEntity, int score) {
        String roleCode = "1003";
        if (taskEntity.getProductTypeId().equals(VMSystem.TASK_TYPE_SUPPLY)) {
            //????????????
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
            //????????????
            roleCode = "1003";
        }
        String key = VMSystem.REGION_TASK_KEY_PREF
                + LocalDate.now() + "."
                + regionId + "."
                + roleCode;

        //????????????
        Set<Object> set = redisTemplate.opsForZSet().range(key, 0, -1);
        if(set == null || set.isEmpty()){
            return 0;
        }

        return (Integer) set.stream().collect(Collectors.toList()).get(0);
    }
}

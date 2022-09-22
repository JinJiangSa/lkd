package com.lkd.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lkd.feign.UserService;
import com.lkd.utils.TaskThreadLocalUtil;
import com.lkd.vo.UserVO;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.StopWatch;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;

/**
 * @Author: yinshijin
 * @Date: 2022/9/21
 */

@Aspect
@Component
@Slf4j
public class OperatorLogAspect {

    @Autowired
    private UserService userService;

    @Around("@annotation(systemControllerLog)")
    public Object aroundLog(ProceedingJoinPoint point, SystemControllerLog systemControllerLog) {

        StringBuilder sb = new StringBuilder();
        StopWatch started = new StopWatch();
        HttpServletRequest request = null;
        try {
            ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            request = attributes.getRequest();

            sb.append("\n<===================================START===================================>\n");
            sb.append("uri:>").append(request.getRequestURI()).append("\n");
            sb.append("url:>").append(request.getRequestURL()).append("\n");
            UserVO userVO = TaskThreadLocalUtil.getUser();
            UserVO user = userService.getUser(userVO.getUserId());
            sb.append("operator:>").append(user).append("\n");

            Date dateNow = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String dateNowStr = sdf.format(dateNow);
            sb.append("operate time:>").append(dateNowStr).append("\n");

            sb.append("request method:>").append(request.getMethod()).append("\n");

            MethodSignature signature = (MethodSignature) point.getSignature();
            Method method = signature.getMethod();
            String methodName = method.getName();
            sb.append("method name:> ").append(methodName).append("\n");

            Parameter[] parameters = method.getParameters();
            ObjectMapper mapper = new ObjectMapper();
            Object[] args = point.getArgs();
            sb.append("request:>\n");
            if (args != null) {
                for (int i = 0; i < args.length; i++) {
                    String paramName = parameters[i].getName();
                    String argStr= null;
                    try {
                        argStr  = mapper.writeValueAsString(args[i]);
                    } catch (Exception e) {
                        argStr = "无法解析";
                    }
                    sb.append(paramName).append(":").append(argStr).append("\n");
                }
            }

            started.start();
            Object proceed = point.proceed();

            sb.append("response:>\n").append(mapper.writeValueAsString(proceed)).append("\n");

            return proceed;
        } catch (RuntimeException e) {
            sb.append("RuntimeException:>").append(e.getMessage()).append("\n");
            throw e;
        } catch (Throwable throwable) {
            sb.append("Throwable:>").append(throwable.getMessage()).append("\n");
            throw new RuntimeException("系统异常!");
        }finally {
            started.stop();
            sb.append("call total time(ms) :> ").append(started.getTime()).append("\n");
            sb.append("<====================================END====================================>\n");
            log.info(sb.toString());
        }
    }
}

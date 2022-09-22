package com.lkd.interceptor;

import com.lkd.feign.UserService;
import com.lkd.utils.TaskThreadLocalUtil;
import com.lkd.vo.UserVO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Optional;

@Slf4j
public class TokenInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //得到header中的信息
        String userIdStr = request.getHeader("userId");
        Optional<String> optional = Optional.ofNullable(userIdStr);
        Integer userId = Integer.parseInt(userIdStr);
        if(optional.isPresent()){
            UserVO user = new UserVO();
            user.setUserId(userId);
            TaskThreadLocalUtil.setUser(user);
            // log.info("wmTokenFilter设置用户信息到threadlocal中...");
        }
        return true;
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView) throws Exception {
        // log.info("清理threadlocal...");
        TaskThreadLocalUtil.clear();
    }
}
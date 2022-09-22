package com.lkd.utils;

import com.lkd.vo.UserVO;

/**
 * @Author: yinshijin
 * @Date: 2022/9/21
 */

public class TaskThreadLocalUtil {
    private final static ThreadLocal<UserVO> USER_THREAD_LOCAL = new ThreadLocal<>();

    /**
     * 添加用户
     * @param user
     */
    public static void setUser(UserVO user){
        USER_THREAD_LOCAL.set(user);
    }

    /**
     * 获取用户
     */
    public static UserVO getUser(){
        return USER_THREAD_LOCAL.get();
    }

    /**
     * 清理用户
     */
    public static void clear(){
        USER_THREAD_LOCAL.remove();
    }
}

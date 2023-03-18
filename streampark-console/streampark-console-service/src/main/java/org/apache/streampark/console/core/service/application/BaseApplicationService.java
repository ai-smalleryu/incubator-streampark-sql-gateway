package org.apache.streampark.console.core.service.application;

import com.baomidou.mybatisplus.extension.service.IService;
import org.apache.streampark.console.core.entity.Application;

/**
 * Base Application service interface.
 */
public interface BaseApplicationService extends IService<Application> {

    /**
     * Start the application
     *
     * @param appParam
     * @param auto
     * @throws Exception
     */
    void start(Application appParam, boolean auto) throws Exception;


    /**
     * Cancel the application
     *
     * @param appParam
     * @throws Exception
     */
    void cancel(Application appParam) throws Exception;




}

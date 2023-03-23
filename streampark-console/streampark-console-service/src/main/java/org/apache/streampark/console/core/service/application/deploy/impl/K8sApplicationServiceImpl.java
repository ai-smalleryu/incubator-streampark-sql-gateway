package org.apache.streampark.console.core.service.application.deploy.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.mapper.ApplicationMapper;
import org.apache.streampark.console.core.service.application.deploy.K8sApplicationService;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
public class K8sApplicationServiceImpl extends ServiceImpl<ApplicationMapper, Application> implements K8sApplicationService {

    @Override
    public void start(Application app, boolean auto) throws Exception {

    }

    @Override
    public void restart(Application application) throws Exception {

    }

    @Override
    public void delete(Application app) throws Exception {

    }

    @Override
    public void cancel(Application app) throws Exception {

    }

    @Override
    public List<String> getRecentK8sNamespace() {
        return null;
    }

    @Override
    public List<String> getRecentK8sClusterId(Integer executionMode) {
        return null;
    }

    @Override
    public List<String> getRecentFlinkBaseImage() {
        return null;
    }

    @Override
    public List<String> getRecentK8sPodTemplate() {
        return null;
    }

    @Override
    public List<String> getRecentK8sJmPodTemplate() {
        return null;
    }

    @Override
    public List<String> getRecentK8sTmPodTemplate() {
        return null;
    }

    @Override
    public String k8sStartLog(Long id, Integer offset, Integer limit) throws Exception {
        return null;
    }
}

package org.apache.streampark.console.core.service.application.deploy.impl;

import org.apache.streampark.common.enums.ExecutionMode;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.util.WebUtils;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.mapper.ApplicationMapper;
import org.apache.streampark.console.core.service.LogClientService;
import org.apache.streampark.console.core.service.application.deploy.K8sApplicationService;
import org.apache.streampark.flink.kubernetes.helper.KubernetesDeploymentHelper;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class K8sApplicationServiceImpl extends ServiceImpl<ApplicationMapper, Application>
    implements K8sApplicationService {

  private static final int DEFAULT_HISTORY_RECORD_LIMIT = 25;

  private static final int DEFAULT_HISTORY_POD_TMPL_RECORD_LIMIT = 5;

  @Autowired private LogClientService logClient;

  @Override
  public void start(Application application, boolean auto) throws Exception {}

  @Override
  public void restart(Application application) throws Exception {}

  @Override
  public void delete(Application application) throws Exception {}

  @Override
  public void cancel(Application application) throws Exception {}

  @Override
  public List<String> getRecentK8sNamespace() {
    return baseMapper.getRecentK8sNamespace(DEFAULT_HISTORY_RECORD_LIMIT);
  }

  @Override
  public List<String> getRecentK8sClusterId(Integer executionMode) {
    return baseMapper.getRecentK8sClusterId(executionMode, DEFAULT_HISTORY_RECORD_LIMIT);
  }

  @Override
  public List<String> getRecentFlinkBaseImage() {
    return baseMapper.getRecentFlinkBaseImage(DEFAULT_HISTORY_RECORD_LIMIT);
  }

  @Override
  public List<String> getRecentK8sPodTemplate() {
    return baseMapper.getRecentK8sPodTemplate(DEFAULT_HISTORY_POD_TMPL_RECORD_LIMIT);
  }

  @Override
  public List<String> getRecentK8sJmPodTemplate() {
    return baseMapper.getRecentK8sJmPodTemplate(DEFAULT_HISTORY_POD_TMPL_RECORD_LIMIT);
  }

  @Override
  public List<String> getRecentK8sTmPodTemplate() {
    return baseMapper.getRecentK8sTmPodTemplate(DEFAULT_HISTORY_POD_TMPL_RECORD_LIMIT);
  }

  @Override
  public String k8sStartLog(Long id, Integer offset, Integer limit) throws Exception {
    Application application = getById(id);
    ApiAlertException.throwIfNull(
        application, String.format("The application id=%s can't be found.", id));
    if (ExecutionMode.isKubernetesMode(application.getExecutionModeEnum())) {
      CompletableFuture<String> future =
          CompletableFuture.supplyAsync(
              () ->
                  KubernetesDeploymentHelper.watchDeploymentLog(
                      application.getK8sNamespace(),
                      application.getJobName(),
                      application.getJobId()));

      return future
          .exceptionally(
              e -> {
                String errorLog =
                    String.format(
                        "%s/%s_err.log",
                        WebUtils.getAppTempDir().getAbsolutePath(), application.getJobId());
                File file = new File(errorLog);
                if (file.exists() && file.isFile()) {
                  return file.getAbsolutePath();
                }
                return null;
              })
          .thenApply(
              path -> {
                if (!future.isDone()) {
                  future.cancel(true);
                }
                if (path != null) {
                  return logClient.rollViewLog(path, offset, limit);
                }
                return null;
              })
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);
    } else {
      throw new ApiAlertException(
          "Job executionMode must be kubernetes-session|kubernetes-application.");
    }
  }
}

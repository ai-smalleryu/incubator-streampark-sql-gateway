package org.apache.streampark.console.core.service.application.impl;

import org.apache.streampark.common.enums.ExecutionMode;
import org.apache.streampark.common.util.Utils;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.exception.ApiDetailException;
import org.apache.streampark.console.base.exception.ApplicationException;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.FlinkCluster;
import org.apache.streampark.console.core.entity.FlinkEnv;
import org.apache.streampark.console.core.entity.FlinkSql;
import org.apache.streampark.console.core.enums.CandidateType;
import org.apache.streampark.console.core.enums.ChangedType;
import org.apache.streampark.console.core.enums.FlinkAppState;
import org.apache.streampark.console.core.enums.OptionState;
import org.apache.streampark.console.core.enums.ReleaseState;
import org.apache.streampark.console.core.mapper.ApplicationMapper;
import org.apache.streampark.console.core.runner.EnvInitializer;
import org.apache.streampark.console.core.service.ApplicationBackUpService;
import org.apache.streampark.console.core.service.ApplicationConfigService;
import org.apache.streampark.console.core.service.FlinkClusterService;
import org.apache.streampark.console.core.service.FlinkEnvService;
import org.apache.streampark.console.core.service.FlinkSqlService;
import org.apache.streampark.console.core.service.application.OpApplicationInfoService;
import org.apache.streampark.console.core.service.application.ValidateApplicationService;
import org.apache.streampark.console.core.task.FlinkRESTAPIWatcher;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Slf4j
@Service
public class ValidateApplicationServiceImpl extends ServiceImpl<ApplicationMapper, Application>
    implements ValidateApplicationService {

  @Autowired private OpApplicationInfoService applicationInfoService;
  @Autowired private ApplicationBackUpService backUpService;
  @Autowired private ApplicationConfigService configService;

  @Autowired private FlinkSqlService flinkSqlService;

  @Autowired private FlinkEnvService flinkEnvService;

  @Autowired private EnvInitializer envInitializer;
  @Autowired private FlinkClusterService flinkClusterService;

  @PostConstruct
  public void resetOptionState() {
    this.baseMapper.resetOptionState();
  }

  @Override
  public boolean existsByTeamId(Long teamId) {
    return baseMapper.existsByTeamId(teamId);
  }

  @Override
  public boolean existsRunningJobByClusterId(Long clusterId) {
    boolean exists = baseMapper.existsRunningJobByClusterId(clusterId);
    if (exists) {
      return true;
    }
    for (Application application : FlinkRESTAPIWatcher.getWatchingApps()) {
      if (clusterId.equals(application.getFlinkClusterId())
          && FlinkAppState.RUNNING.equals(application.getFlinkAppStateEnum())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean existsJobByClusterId(Long clusterId) {
    return baseMapper.existsJobByClusterId(clusterId);
  }

  @Override
  public boolean existsByJobName(String jobName) {
    return this.baseMapper.existsByJobName(jobName);
  }

  /**
   * update FlinkSql type jobs, there are 3 aspects to consider<br>
   * 1. flink sql has changed <br>
   * 2. dependency has changed<br>
   * 3. parameter has changed<br>
   *
   * @param application
   * @param appParam
   */
  private void updateFlinkSqlJob(Application application, Application appParam) {
    FlinkSql effectiveFlinkSql = flinkSqlService.getEffective(application.getId(), true);
    if (effectiveFlinkSql == null) {
      effectiveFlinkSql = flinkSqlService.getCandidate(application.getId(), CandidateType.NEW);
      flinkSqlService.removeById(effectiveFlinkSql.getId());
      FlinkSql sql = new FlinkSql(appParam);
      flinkSqlService.create(sql);
      application.setBuild(true);
    } else {
      // get previous flink sql and decode
      FlinkSql copySourceFlinkSql = flinkSqlService.getById(appParam.getSqlId());
      ApiAlertException.throwIfNull(
          copySourceFlinkSql, "Flink sql is null, update flink sql job failed.");
      copySourceFlinkSql.decode();

      // get submit flink sql
      FlinkSql targetFlinkSql = new FlinkSql(appParam);

      // judge sql and dependency has changed
      ChangedType changedType = copySourceFlinkSql.checkChange(targetFlinkSql);

      log.info("updateFlinkSqlJob changedType: {}", changedType);

      // if has been changed
      if (changedType.hasChanged()) {
        // check if there is a candidate version for the newly added record
        FlinkSql newFlinkSql = flinkSqlService.getCandidate(application.getId(), CandidateType.NEW);
        // If the candidate version of the new record exists, it will be deleted directly,
        // and only one candidate version will be retained. If the new candidate version is not
        // effective,
        // if it is edited again and the next record comes in, the previous candidate version will
        // be deleted.
        if (newFlinkSql != null) {
          // delete all records about candidates
          flinkSqlService.removeById(newFlinkSql.getId());
        }
        FlinkSql historyFlinkSql =
            flinkSqlService.getCandidate(application.getId(), CandidateType.HISTORY);
        // remove candidate flags that already exist but are set as candidates
        if (historyFlinkSql != null) {
          flinkSqlService.cleanCandidate(historyFlinkSql.getId());
        }
        FlinkSql sql = new FlinkSql(appParam);
        flinkSqlService.create(sql);
        if (changedType.isDependencyChanged()) {
          application.setBuild(true);
        }
      } else {
        // judge version has changed
        boolean versionChanged = !effectiveFlinkSql.getId().equals(appParam.getSqlId());
        if (versionChanged) {
          // sql and dependency not changed, but version changed, means that rollback to the version
          CandidateType type = CandidateType.HISTORY;
          flinkSqlService.setCandidate(type, appParam.getId(), appParam.getSqlId());
          application.setRelease(ReleaseState.NEED_ROLLBACK.get());
          application.setBuild(true);
        }
      }
    }
    this.configService.update(appParam, application.isRunning());
  }

  @Override
  public boolean checkEnv(Application appParam) throws ApplicationException {
    Application application = getById(appParam.getId());
    try {
      FlinkEnv flinkEnv;
      if (application.getVersionId() != null) {
        flinkEnv = flinkEnvService.getByIdOrDefault(application.getVersionId());
      } else {
        flinkEnv = flinkEnvService.getDefault();
      }
      if (flinkEnv == null) {
        return false;
      }
      envInitializer.checkFlinkEnv(application.getStorageType(), flinkEnv);
      envInitializer.storageInitialize(application.getStorageType());

      if (ExecutionMode.YARN_SESSION.equals(application.getExecutionModeEnum())
          || ExecutionMode.REMOTE.equals(application.getExecutionModeEnum())) {
        FlinkCluster flinkCluster = flinkClusterService.getById(application.getFlinkClusterId());
        boolean conned = flinkCluster.verifyClusterConnection();
        if (!conned) {
          throw new ApiAlertException("the target cluster is unavailable, please check!");
        }
      }
      return true;
    } catch (Exception e) {
      log.error(Utils.stringifyException(e));
      throw new ApiDetailException(e);
    }
  }

  @Override
  public boolean checkAlter(Application application) {
    Long appId = application.getId();
    FlinkAppState state = FlinkAppState.of(application.getState());
    if (!FlinkAppState.CANCELED.equals(state)) {
      return false;
    }
    long cancelUserId = FlinkRESTAPIWatcher.getCanceledJobUserId(appId);
    long appUserId = application.getUserId();
    return cancelUserId != -1 && cancelUserId != appUserId;
  }

  @Override
  public boolean checkBuildAndUpdate(Application application) {
    boolean build = application.getBuild();
    if (!build) {
      LambdaUpdateWrapper<Application> updateWrapper = Wrappers.lambdaUpdate();
      updateWrapper.eq(Application::getId, application.getId());
      if (application.isRunning()) {
        updateWrapper.set(Application::getRelease, ReleaseState.NEED_RESTART.get());
      } else {
        updateWrapper.set(Application::getRelease, ReleaseState.DONE.get());
        updateWrapper.set(Application::getOptionState, OptionState.NONE.getValue());
      }
      this.update(updateWrapper);

      // backup
      if (application.isFlinkSqlJob()) {
        FlinkSql newFlinkSql = flinkSqlService.getCandidate(application.getId(), CandidateType.NEW);
        if (!application.isNeedRollback() && newFlinkSql != null) {
          backUpService.backup(application, newFlinkSql);
        }
      }

      // If the current task is not running, or the task has just been added,
      // directly set the candidate version to the official version
      FlinkSql flinkSql = flinkSqlService.getEffective(application.getId(), false);
      if (!application.isRunning() || flinkSql == null) {
        applicationInfoService.toEffective(application);
      }
    }
    return build;
  }
}

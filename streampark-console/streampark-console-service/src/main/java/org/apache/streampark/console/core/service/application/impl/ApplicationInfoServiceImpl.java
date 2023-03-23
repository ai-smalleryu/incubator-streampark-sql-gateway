package org.apache.streampark.console.core.service.application.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.core.toolkit.support.SFunction;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import io.fabric8.kubernetes.client.KubernetesClientException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.streampark.common.conf.ConfigConst;
import org.apache.streampark.common.conf.Workspace;
import org.apache.streampark.common.enums.ExecutionMode;
import org.apache.streampark.common.enums.ResolveOrder;
import org.apache.streampark.common.enums.StorageType;
import org.apache.streampark.common.fs.HdfsOperator;
import org.apache.streampark.common.fs.LfsOperator;
import org.apache.streampark.common.util.CompletableFutureUtils;
import org.apache.streampark.common.util.DeflaterUtils;
import org.apache.streampark.common.util.HadoopUtils;
import org.apache.streampark.common.util.PropertiesUtils;
import org.apache.streampark.common.util.ThreadUtils;
import org.apache.streampark.common.util.Utils;
import org.apache.streampark.common.util.YarnUtils;
import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.exception.ApiDetailException;
import org.apache.streampark.console.base.exception.ApplicationException;
import org.apache.streampark.console.base.mybatis.pager.MybatisPager;
import org.apache.streampark.console.base.util.CommonUtils;
import org.apache.streampark.console.base.util.ObjectUtils;
import org.apache.streampark.console.base.util.WebUtils;
import org.apache.streampark.console.core.entity.AppBuildPipeline;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.ApplicationConfig;
import org.apache.streampark.console.core.entity.ApplicationLog;
import org.apache.streampark.console.core.entity.FlinkCluster;
import org.apache.streampark.console.core.entity.FlinkEnv;
import org.apache.streampark.console.core.entity.FlinkSql;
import org.apache.streampark.console.core.entity.Project;
import org.apache.streampark.console.core.entity.SavePoint;
import org.apache.streampark.console.core.enums.AppExistsState;
import org.apache.streampark.console.core.enums.CandidateType;
import org.apache.streampark.console.core.enums.ChangedType;
import org.apache.streampark.console.core.enums.CheckPointType;
import org.apache.streampark.console.core.enums.ConfigFileType;
import org.apache.streampark.console.core.enums.FlinkAppState;
import org.apache.streampark.console.core.enums.OptionState;
import org.apache.streampark.console.core.enums.ReleaseState;
import org.apache.streampark.console.core.mapper.ApplicationMapper;
import org.apache.streampark.console.core.metrics.flink.JobsOverview;
import org.apache.streampark.console.core.runner.EnvInitializer;
import org.apache.streampark.console.core.service.AppBuildPipeService;
import org.apache.streampark.console.core.service.ApplicationBackUpService;
import org.apache.streampark.console.core.service.ApplicationConfigService;
import org.apache.streampark.console.core.service.ApplicationLogService;
import org.apache.streampark.console.core.service.CommonService;
import org.apache.streampark.console.core.service.EffectiveService;
import org.apache.streampark.console.core.service.FlinkClusterService;
import org.apache.streampark.console.core.service.FlinkEnvService;
import org.apache.streampark.console.core.service.FlinkSqlService;
import org.apache.streampark.console.core.service.LogClientService;
import org.apache.streampark.console.core.service.ProjectService;
import org.apache.streampark.console.core.service.SavePointService;
import org.apache.streampark.console.core.service.SettingService;
import org.apache.streampark.console.core.service.VariableService;
import org.apache.streampark.console.core.service.application.ApplicationInfoService;
import org.apache.streampark.console.core.task.FlinkRESTAPIWatcher;
import org.apache.streampark.flink.client.FlinkClient;
import org.apache.streampark.flink.client.bean.CancelRequest;
import org.apache.streampark.flink.client.bean.CancelResponse;
import org.apache.streampark.flink.client.bean.KubernetesSubmitParam;
import org.apache.streampark.flink.client.bean.SubmitRequest;
import org.apache.streampark.flink.client.bean.SubmitResponse;
import org.apache.streampark.flink.core.conf.ParameterCli;
import org.apache.streampark.flink.kubernetes.FlinkK8sWatcher;
import org.apache.streampark.flink.kubernetes.IngressController;
import org.apache.streampark.flink.kubernetes.helper.KubernetesDeploymentHelper;
import org.apache.streampark.flink.kubernetes.model.FlinkMetricCV;
import org.apache.streampark.flink.kubernetes.model.TrackId;
import org.apache.streampark.flink.packer.pipeline.BuildResult;
import org.apache.streampark.flink.packer.pipeline.DockerImageBuildResponse;
import org.apache.streampark.flink.packer.pipeline.ShadedBuildResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.jar.Manifest;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.streampark.common.enums.StorageType.LFS;
import static org.apache.streampark.console.core.task.FlinkK8sWatcherWrapper.Bridge.toTrackId;
import static org.apache.streampark.console.core.task.FlinkK8sWatcherWrapper.isKubernetesApp;
import static org.apache.streampark.console.core.utils.YarnQueueLabelExpression.checkQueueLabelIfNeed;

@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class ApplicationInfoServiceImpl extends ServiceImpl<ApplicationMapper, Application> implements ApplicationInfoService {

    private static final int DEFAULT_HISTORY_RECORD_LIMIT = 25;

    private static final Pattern JOB_NAME_PATTERN =
        Pattern.compile("^[.\\x{4e00}-\\x{9fa5}A-Za-z\\d_\\-\\s]+$");

    private static final Pattern SINGLE_SPACE_PATTERN = Pattern.compile("^\\S+(\\s\\S+)*$");

    @Autowired private ApplicationBackUpService backUpService;

    @Autowired private ApplicationConfigService configService;

    @Autowired private ApplicationLogService applicationLogService;

    @Autowired private FlinkSqlService flinkSqlService;

    @Autowired private SavePointService savePointService;

    @Autowired private EffectiveService effectiveService;

    @Autowired private SettingService settingService;

    @Autowired private CommonService commonService;

    @Autowired private FlinkK8sWatcher k8SFlinkTrackMonitor;

    @Autowired private AppBuildPipeService appBuildPipeService;

    @PostConstruct
    public void resetOptionState() {
        this.baseMapper.resetOptionState();
    }

    private final Map<Long, CompletableFuture<SubmitResponse>> startFutureMap =
        new ConcurrentHashMap<>();

    private final Map<Long, CompletableFuture<CancelResponse>> cancelFutureMap =
        new ConcurrentHashMap<>();

    @Override
    public String upload(MultipartFile file) throws Exception {
        File temp = WebUtils.getAppTempDir();
        String fileName = FilenameUtils.getName(Objects.requireNonNull(file.getOriginalFilename()));
        File saveFile = new File(temp, fileName);
        // delete when exists
        if (saveFile.exists()) {
            saveFile.delete();
        }
        // save file to temp dir
        try {
            file.transferTo(saveFile);
        } catch (Exception e) {
            throw new ApiDetailException(e);
        }
        return saveFile.getAbsolutePath();
    }

    @Override
    public void toEffective(Application application) {
        // set latest to Effective
        ApplicationConfig config = configService.getLatest(application.getId());
        if (config != null) {
            this.configService.toEffective(application.getId(), config.getId());
        }
        if (application.isFlinkSqlJob()) {
            FlinkSql flinkSql = flinkSqlService.getCandidate(application.getId(), null);
            if (flinkSql != null) {
                flinkSqlService.toEffective(application.getId(), flinkSql.getId());
                // clean candidate
                flinkSqlService.cleanCandidate(flinkSql.getId());
            }
        }
    }

    @Override
    public void revoke(Application appParma) throws ApplicationException {
        Application application = getById(appParma.getId());
        ApiAlertException.throwIfNull(
            application,
            String.format("The application id=%s not found, revoke failed.", appParma.getId()));

        // 1) delete files that have been published to workspace
        application.getFsOperator().delete(application.getAppHome());

        // 2) rollback the files to the workspace
        backUpService.revoke(application);

        // 3) restore related status
        LambdaUpdateWrapper<Application> updateWrapper = Wrappers.lambdaUpdate();
        updateWrapper.eq(Application::getId, application.getId());
        if (application.isFlinkSqlJob()) {
            updateWrapper.set(Application::getRelease, ReleaseState.FAILED);
        } else {
            updateWrapper.set(Application::getRelease, ReleaseState.NEED_RELEASE);
        }
        if (!application.isRunning()) {
            updateWrapper.set(Application::getState, FlinkAppState.REVOKED);
        }
        baseMapper.update(null, updateWrapper);
    }

    @Override
    @Transactional(rollbackFor = {Exception.class})
    public Boolean delete(Application paramApp) {

        Application application = getById(paramApp.getId());

        // 1) remove flink sql
        flinkSqlService.removeApp(application.getId());

        // 2) remove log
        applicationLogService.removeApp(application.getId());

        // 3) remove config
        configService.removeApp(application.getId());

        // 4) remove effective
        effectiveService.removeApp(application.getId());

        // remove related hdfs
        // 5) remove backup
        backUpService.removeApp(application);

        // 6) remove savepoint
        savePointService.removeApp(application);

        // 7) remove BuildPipeline
        appBuildPipeService.removeApp(application.getId());

        // 8) remove app
        removeApp(application);

        if (isKubernetesApp(paramApp)) {
            k8SFlinkTrackMonitor.unWatching(toTrackId(application));
        } else {
            FlinkRESTAPIWatcher.unWatching(paramApp.getId());
        }
        return true;
    }

    private void removeApp(Application application) {
        Long appId = application.getId();
        removeById(appId);
        try {
            application
                .getFsOperator()
                .delete(application.getWorkspace().APP_WORKSPACE().concat("/").concat(appId.toString()));
            // try to delete yarn-application, and leave no trouble.
            String path =
                Workspace.of(StorageType.HDFS).APP_WORKSPACE().concat("/").concat(appId.toString());
            if (HdfsOperator.exists(path)) {
                HdfsOperator.delete(path);
            }
        } catch (Exception e) {
            // skip
        }
    }


    @Override
    public List<String> historyUploadJars() {
        return Arrays.stream(LfsOperator.listDir(Workspace.of(LFS).APP_UPLOADS()))
            .filter(File::isFile)
            .sorted(Comparator.comparingLong(File::lastModified).reversed())
            .map(File::getName)
            .filter(fn -> fn.endsWith(".jar"))
            .limit(DEFAULT_HISTORY_RECORD_LIMIT)
            .collect(Collectors.toList());
    }


    @SneakyThrows
    @Override
    @Transactional(rollbackFor = {Exception.class})
    public boolean create(Application appParam) {
        ApiAlertException.throwIfNull(
            appParam.getTeamId(), "The teamId can't be null. Create application failed.");
        appParam.setUserId(commonService.getUserId());
        appParam.setState(FlinkAppState.ADDED);
        appParam.setRelease(ReleaseState.NEED_RELEASE);
        appParam.setOptionState(OptionState.NONE);
        appParam.setCreateTime(new Date());
        appParam.setDefaultModeIngress(settingService.getIngressModeDefault());
        checkQueueLabelIfNeed(appParam.getExecutionMode(), appParam.getYarnQueue());
        appParam.doSetHotParams();
        if (appParam.isUploadJob()) {
            String jarPath =
                WebUtils.getAppTempDir().getAbsolutePath().concat("/").concat(appParam.getJar());
            appParam.setJarCheckSum(FileUtils.checksumCRC32(new File(jarPath)));
        }

        if (save(appParam)) {
            if (appParam.isFlinkSqlJob()) {
                FlinkSql flinkSql = new FlinkSql(appParam);
                flinkSqlService.create(flinkSql);
            }
            if (appParam.getConfig() != null) {
                configService.create(appParam, true);
            }
            return true;
        } else {
            throw new ApiAlertException("create application failed");
        }
    }

    private boolean existsByJobName(String jobName) {
        return this.baseMapper.existsByJobName(jobName);
    }

    @SuppressWarnings("checkstyle:WhitespaceAround")
    @Override
    @SneakyThrows
    @Transactional(rollbackFor = {Exception.class})
    public Long copy(Application appParam) {
        boolean existsByJobName = this.existsByJobName(appParam.getJobName());
        ApiAlertException.throwIfFalse(
            !existsByJobName,
            "[StreamPark] Application names can't be repeated, copy application failed.");

        Application oldApp = getById(appParam.getId());
        Application newApp = new Application();
        String jobName = appParam.getJobName();

        newApp.setJobName(jobName);
        newApp.setClusterId(
            ExecutionMode.isSessionMode(oldApp.getExecutionMode()) ? oldApp.getClusterId() : jobName);
        newApp.setArgs(appParam.getArgs() != null ? appParam.getArgs() : oldApp.getArgs());
        newApp.setVersionId(oldApp.getVersionId());

        newApp.setFlinkClusterId(oldApp.getFlinkClusterId());
        newApp.setRestartSize(oldApp.getRestartSize());
        newApp.setJobType(oldApp.getJobType());
        newApp.setOptions(oldApp.getOptions());
        newApp.setDynamicProperties(oldApp.getDynamicProperties());
        newApp.setResolveOrder(oldApp.getResolveOrder());
        newApp.setExecutionMode(oldApp.getExecutionMode());
        newApp.setFlinkImage(oldApp.getFlinkImage());
        newApp.setK8sNamespace(oldApp.getK8sNamespace());
        newApp.setK8sRestExposedType(oldApp.getK8sRestExposedType());
        newApp.setK8sPodTemplate(oldApp.getK8sPodTemplate());
        newApp.setK8sJmPodTemplate(oldApp.getK8sJmPodTemplate());
        newApp.setK8sTmPodTemplate(oldApp.getK8sTmPodTemplate());
        newApp.setK8sHadoopIntegration(oldApp.getK8sHadoopIntegration());
        newApp.setDescription(oldApp.getDescription());
        newApp.setAlertId(oldApp.getAlertId());
        newApp.setCpFailureAction(oldApp.getCpFailureAction());
        newApp.setCpFailureRateInterval(oldApp.getCpFailureRateInterval());
        newApp.setCpMaxFailureInterval(oldApp.getCpMaxFailureInterval());
        newApp.setMainClass(oldApp.getMainClass());
        newApp.setAppType(oldApp.getAppType());
        newApp.setResourceFrom(oldApp.getResourceFrom());
        newApp.setProjectId(oldApp.getProjectId());
        newApp.setModule(oldApp.getModule());
        newApp.setDefaultModeIngress(oldApp.getDefaultModeIngress());
        newApp.setUserId(commonService.getUserId());
        newApp.setState(FlinkAppState.ADDED);
        newApp.setRelease(ReleaseState.NEED_RELEASE);
        newApp.setOptionState(OptionState.NONE);
        newApp.setCreateTime(new Date());
        newApp.setHotParams(oldApp.getHotParams());

        newApp.setJar(oldApp.getJar());
        newApp.setJarCheckSum(oldApp.getJarCheckSum());
        newApp.setTags(oldApp.getTags());
        newApp.setTeamId(oldApp.getTeamId());

        boolean saved = save(newApp);
        if (saved) {
            if (newApp.isFlinkSqlJob()) {
                FlinkSql copyFlinkSql = flinkSqlService.getLatestFlinkSql(appParam.getId(), true);
                newApp.setFlinkSql(copyFlinkSql.getSql());
                newApp.setDependency(copyFlinkSql.getDependency());
                FlinkSql flinkSql = new FlinkSql(newApp);
                flinkSqlService.create(flinkSql);
            }
            if (newApp.getConfig() != null) {
                ApplicationConfig copyConfig = configService.getEffective(appParam.getId());
                ApplicationConfig config = new ApplicationConfig();
                config.setAppId(newApp.getId());
                config.setFormat(copyConfig.getFormat());
                config.setContent(copyConfig.getContent());
                config.setCreateTime(new Date());
                config.setVersion(1);
                configService.save(config);
                configService.setLatestOrEffective(true, config.getId(), newApp.getId());
            }
            return newApp.getId();
        } else {
            throw new ApiAlertException(
                "create application from copy failed, copy source app: " + oldApp.getJobName());
        }
    }

    @Override
    @Transactional(rollbackFor = {Exception.class})
    public boolean update(Application appParam) {
        checkQueueLabelIfNeed(appParam.getExecutionMode(), appParam.getYarnQueue());
        Application application = getById(appParam.getId());
        application.setRelease(ReleaseState.NEED_RELEASE);
        if (application.isUploadJob()) {
            if (!ObjectUtils.safeEquals(application.getJar(), appParam.getJar())) {
                application.setBuild(true);
            } else {
                File jarFile = new File(WebUtils.getAppTempDir(), appParam.getJar());
                if (jarFile.exists()) {
                    long checkSum = 0;
                    try {
                        checkSum = FileUtils.checksumCRC32(jarFile);
                    } catch (IOException e) {
                        log.error("Error in checksumCRC32 for {}.", jarFile);
                        throw new RuntimeException(e);
                    }
                    if (!ObjectUtils.safeEquals(checkSum, application.getJarCheckSum())) {
                        application.setBuild(true);
                    }
                }
            }
        }

        if (!application.getBuild()) {
            if (!application.getExecutionMode().equals(appParam.getExecutionMode())) {
                if (appParam.getExecutionMode().equals(ExecutionMode.YARN_APPLICATION)
                    || application.getExecutionMode().equals(ExecutionMode.YARN_APPLICATION)) {
                    application.setBuild(true);
                }
            }
        }

        if (ExecutionMode.isKubernetesMode(appParam.getExecutionMode())) {
            if (!ObjectUtils.safeTrimEquals(
                application.getK8sRestExposedType(), appParam.getK8sRestExposedType())
                || !ObjectUtils.safeTrimEquals(
                application.getK8sJmPodTemplate(), appParam.getK8sJmPodTemplate())
                || !ObjectUtils.safeTrimEquals(
                application.getK8sTmPodTemplate(), appParam.getK8sTmPodTemplate())
                || !ObjectUtils.safeTrimEquals(
                application.getK8sPodTemplates(), appParam.getK8sPodTemplates())
                || !ObjectUtils.safeTrimEquals(
                application.getK8sHadoopIntegration(), appParam.getK8sHadoopIntegration())
                || !ObjectUtils.safeTrimEquals(application.getFlinkImage(), appParam.getFlinkImage())) {
                application.setBuild(true);
            }
        }

        appParam.setJobType(application.getJobType());
        // changes to the following parameters need to be re-release to take effect
        application.setJobName(appParam.getJobName());
        application.setVersionId(appParam.getVersionId());
        application.setArgs(appParam.getArgs());
        application.setOptions(appParam.getOptions());
        application.setDynamicProperties(appParam.getDynamicProperties());
        application.setResolveOrder(appParam.getResolveOrder());
        application.setExecutionMode(appParam.getExecutionMode());
        application.setClusterId(appParam.getClusterId());
        application.setFlinkImage(appParam.getFlinkImage());
        application.setK8sNamespace(appParam.getK8sNamespace());
        application.updateHotParams(appParam);
        application.setK8sRestExposedType(appParam.getK8sRestExposedType());
        application.setK8sPodTemplate(appParam.getK8sPodTemplate());
        application.setK8sJmPodTemplate(appParam.getK8sJmPodTemplate());
        application.setK8sTmPodTemplate(appParam.getK8sTmPodTemplate());
        application.setK8sHadoopIntegration(appParam.getK8sHadoopIntegration());

        // changes to the following parameters do not affect running tasks
        application.setModifyTime(new Date());
        application.setDescription(appParam.getDescription());
        application.setAlertId(appParam.getAlertId());
        application.setRestartSize(appParam.getRestartSize());
        application.setCpFailureAction(appParam.getCpFailureAction());
        application.setCpFailureRateInterval(appParam.getCpFailureRateInterval());
        application.setCpMaxFailureInterval(appParam.getCpMaxFailureInterval());
        application.setTags(appParam.getTags());

        switch (appParam.getExecutionMode()) {
            case YARN_APPLICATION:
            case YARN_PER_JOB:
            case KUBERNETES_NATIVE_APPLICATION:
                application.setFlinkClusterId(null);
                break;
            case REMOTE:
            case YARN_SESSION:
            case KUBERNETES_NATIVE_SESSION:
                application.setFlinkClusterId(appParam.getFlinkClusterId());
                break;
            default:
                break;
        }

        // Flink Sql job...
        if (application.isFlinkSqlJob()) {
            updateFlinkSqlJob(application, appParam);
        } else {
            if (application.isStreamParkJob()) {
                configService.update(appParam, application.isRunning());
            } else {
                application.setJar(appParam.getJar());
                application.setMainClass(appParam.getMainClass());
            }
        }
        baseMapper.updateById(application);
        return true;
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
                    application.setRelease(ReleaseState.NEED_ROLLBACK);
                    application.setBuild(true);
                }
            }
        }
        this.configService.update(appParam, application.isRunning());
    }

    @Override
    public void updateRelease(Application application) {
        LambdaUpdateWrapper<Application> updateWrapper = Wrappers.lambdaUpdate();
        updateWrapper.eq(Application::getId, application.getId());
        updateWrapper.set(Application::getRelease, application.getRelease());
        updateWrapper.set(Application::getBuild, application.getBuild());
        if (application.getOptionState() != null) {
            updateWrapper.set(Application::getOptionState, application.getOptionState());
        }
        this.update(updateWrapper);
    }


    @Override
    public void forcedStop(Application app) {
        CompletableFuture<SubmitResponse> startFuture = startFutureMap.remove(app.getId());
        CompletableFuture<CancelResponse> cancelFuture = cancelFutureMap.remove(app.getId());
        Application application = this.baseMapper.getApp(app);
        if (isKubernetesApp(application)) {
            KubernetesDeploymentHelper.watchPodTerminatedLog(
                application.getK8sNamespace(), application.getJobName(), application.getJobId());
            KubernetesDeploymentHelper.deleteTaskDeployment(
                application.getK8sNamespace(), application.getJobName());
            KubernetesDeploymentHelper.deleteTaskConfigMap(
                application.getK8sNamespace(), application.getJobName());
            IngressController.deleteIngress(application.getK8sNamespace(), application.getJobName());
        }
        if (startFuture != null) {
            startFuture.cancel(true);
        }
        if (cancelFuture != null) {
            cancelFuture.cancel(true);
        }
        if (startFuture == null && cancelFuture == null) {
            this.updateToStopped(app);
        }
    }

    @Override
    public void clean(Application appParam) {
        appParam.setRelease(ReleaseState.DONE);
        this.updateRelease(appParam);
    }


    @Override
    public boolean mapping(Application appParam) {
        boolean mapping = this.baseMapper.mapping(appParam);
        Application application = getById(appParam.getId());
        if (isKubernetesApp(application)) {
            k8SFlinkTrackMonitor.doWatching(toTrackId(application));
        } else {
            FlinkRESTAPIWatcher.doWatching(application);
        }
        return mapping;
    }


    @Override
    public void persistMetrics(Application appParam) {
        this.baseMapper.persistMetrics(appParam);
    }

    /**
     * Setup task is starting (for webUI "state" display)
     *
     * @param appParam
     */
    @Override
    public void starting(Application appParam) {
        Application application = getById(appParam.getId());
        Utils.notNull(
            application,
            String.format(
                "The application id=%s not found, start application failed.", appParam.getId()));
        application.setState(FlinkAppState.STARTING);
        application.setOptionTime(new Date());
        updateById(application);
    }

    private void updateToStopped(Application app) {
        Application application = getById(app);
        application.setOptionState(OptionState.NONE);
        application.setState(FlinkAppState.CANCELED);
        application.setOptionTime(new Date());
        updateById(application);
        savePointService.expire(application.getId());
        // re-tracking flink job on kubernetes and logging exception
        if (isKubernetesApp(application)) {
            TrackId id = toTrackId(application);
            k8SFlinkTrackMonitor.unWatching(id);
            k8SFlinkTrackMonitor.doWatching(id);
        } else {
            FlinkRESTAPIWatcher.unWatching(application.getId());
        }
    }

}

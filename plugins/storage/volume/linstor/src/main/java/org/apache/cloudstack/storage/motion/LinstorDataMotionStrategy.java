/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cloudstack.storage.motion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

import com.linbit.linstor.api.ApiException;
import com.linbit.linstor.api.DevelopersApi;
import com.linbit.linstor.api.model.ApiCallRcList;
import com.linbit.linstor.api.model.Resource;
import com.linbit.linstor.api.model.ResourceDefinitionModify;
import com.linbit.linstor.api.model.ResourceConnectionModify;
import com.linbit.linstor.api.model.ResourceGroupSpawn;
import com.linbit.linstor.api.model.ResourceMakeAvailable;
import com.linbit.linstor.api.model.Properties;
import com.linbit.linstor.api.model.ResourceWithVolumes;

import org.apache.cloudstack.engine.subsystem.api.storage.CopyCommandResult;
import org.apache.cloudstack.engine.subsystem.api.storage.DataMotionStrategy;
import org.apache.cloudstack.engine.subsystem.api.storage.DataObject;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStore;
import org.apache.cloudstack.engine.subsystem.api.storage.ObjectInDataStoreStateMachine.Event;
import org.apache.cloudstack.engine.subsystem.api.storage.StrategyPriority;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeDataFactory;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeInfo;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeService;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeService.VolumeApiResult;
import org.apache.cloudstack.framework.async.AsyncCallFuture;
import org.apache.cloudstack.framework.async.AsyncCompletionCallback;
import org.apache.cloudstack.storage.command.CopyCmdAnswer;
import org.apache.cloudstack.storage.datastore.db.PrimaryDataStoreDao;
import org.apache.cloudstack.storage.datastore.db.SnapshotDataStoreDao;
import org.apache.cloudstack.storage.datastore.db.StoragePoolVO;
import org.apache.cloudstack.storage.datastore.util.LinstorUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.cloud.agent.AgentManager;
import com.cloud.agent.api.Answer;
import com.cloud.agent.api.MigrateAnswer;
import com.cloud.agent.api.MigrateCommand;
import com.cloud.agent.api.MigrateCommand.MigrateDiskInfo;
import com.cloud.agent.api.PrepareForMigrationCommand;
import com.cloud.agent.api.to.VirtualMachineTO;
import com.cloud.exception.AgentUnavailableException;
import com.cloud.exception.OperationTimedoutException;
import com.cloud.host.Host;
import com.cloud.hypervisor.Hypervisor.HypervisorType;
import com.cloud.storage.Storage.ImageFormat;
import com.cloud.storage.Storage.StoragePoolType;
import com.cloud.storage.StorageManager;
import com.cloud.storage.Volume;
import com.cloud.storage.VolumeVO;
import com.cloud.storage.dao.GuestOSCategoryDao;
import com.cloud.storage.dao.GuestOSDao;
import com.cloud.storage.dao.SnapshotDao;
import com.cloud.storage.dao.VolumeDao;
import com.cloud.utils.exception.CloudRuntimeException;
import com.cloud.vm.VMInstanceVO;
import com.cloud.vm.dao.VMInstanceDao;

/**
 * DataMotionStrategy for Linstor primary storage.
 *
 * Handles live VM migration and offline volume migration when the destination
 * storage pool is Linstor. Uses DRBD replication and libvirt block copy for
 * cross-storage migrations (e.g. StorPool/SMP -> Linstor).
 */
@Component
public class LinstorDataMotionStrategy implements DataMotionStrategy {
    private static final Logger LOG = Logger.getLogger(LinstorDataMotionStrategy.class);

    @Inject
    private PrimaryDataStoreDao _storagePoolDao;
    @Inject
    private VolumeDao _volumeDao;
    @Inject
    private VolumeDataFactory _volumeDataFactory;
    @Inject
    private VolumeService _volumeService;
    @Inject
    private VMInstanceDao _vmDao;
    @Inject
    private AgentManager _agentManager;
    @Inject
    private GuestOSDao _guestOsDao;
    @Inject
    private GuestOSCategoryDao _guestOsCategoryDao;
    @Inject
    private SnapshotDao _snapshotDao;
    @Inject
    private SnapshotDataStoreDao _snapshotStoreDao;

    // -- canHandle methods --

    @Override
    public StrategyPriority canHandle(DataObject srcData, DataObject destData) {
        // Offline volume copies are handled by the existing AncientDataMotionStrategy
        // which delegates to the storage driver's copyAsync or uses CopyCommand.
        // A native Linstor offline copy (e.g. DRBD clone) can be added later.
        return StrategyPriority.CANT_HANDLE;
    }

    @Override
    public StrategyPriority canHandle(Map<VolumeInfo, DataStore> volumeMap, Host srcHost, Host destHost) {
        if (srcHost.getId() == destHost.getId()) {
            return StrategyPriority.CANT_HANDLE;
        }
        if (MapUtils.isEmpty(volumeMap)) {
            return StrategyPriority.CANT_HANDLE;
        }
        for (DataStore destStore : volumeMap.values()) {
            StoragePoolVO pool = _storagePoolDao.findById(destStore.getId());
            if (pool == null || pool.getPoolType() != StoragePoolType.Linstor) {
                return StrategyPriority.CANT_HANDLE;
            }
        }
        return StrategyPriority.HIGHEST;
    }

    // -- Offline volume copy --

    @Override
    public void copyAsync(DataObject srcData, DataObject destData, Host destHost,
            AsyncCompletionCallback<CopyCommandResult> callback) {
        // This method should never be called because canHandle(DataObject, DataObject)
        // always returns CANT_HANDLE. Implemented only to satisfy the interface contract.
        String errMsg = "LinstorDataMotionStrategy does not handle offline volume copies";
        LOG.error(errMsg);
        CopyCommandResult result = new CopyCommandResult(null, new CopyCmdAnswer(errMsg));
        result.setResult(errMsg);
        callback.complete(result);
    }

    // -- Live migration with storage --

    @Override
    public void copyAsync(Map<VolumeInfo, DataStore> volumeDataStoreMap, VirtualMachineTO vmTO,
            Host srcHost, Host destHost, AsyncCompletionCallback<CopyCommandResult> callback) {
        String errMsg = null;
        Map<String, DevelopersApi> apiCache = new HashMap<>();
        List<ResourceCleanup> cleanupList = new ArrayList<>();
        Map<VolumeInfo, VolumeInfo> srcToDestVolumeInfo = new HashMap<>();

        try {
            if (srcHost.getHypervisorType() != HypervisorType.KVM) {
                throw new CloudRuntimeException(
                        String.format("Invalid hypervisor type [%s]. Only KVM supported",
                                srcHost.getHypervisorType()));
            }

            VMInstanceVO vmInstance = _vmDao.findById(vmTO.getId());
            vmTO.setState(vmInstance.getState());

            List<MigrateDiskInfo> migrateDiskInfoList = new ArrayList<>();
            Map<String, MigrateDiskInfo> migrateStorage = new HashMap<>();

            for (Map.Entry<VolumeInfo, DataStore> entry : volumeDataStoreMap.entrySet()) {
                VolumeInfo srcVolumeInfo = entry.getKey();
                DataStore destDataStore = entry.getValue();

                VolumeVO srcVolume = _volumeDao.findById(srcVolumeInfo.getId());
                StoragePoolVO destStoragePool = _storagePoolDao.findById(destDataStore.getId());
                StoragePoolVO srcStoragePool = _storagePoolDao.findById(srcVolumeInfo.getPoolId());

                DevelopersApi api = getLinstorAPI(destStoragePool, apiCache);
                String rscName = LinstorUtil.RSC_PREFIX + srcVolume.getUuid();
                String destNodeName = destHost.getName();

                // Create destination volume DB record
                VolumeVO destVolume = duplicateVolumeOnAnotherStorage(srcVolume, destStoragePool);

                VolumeInfo destVolumeInfo = _volumeDataFactory.getVolume(destVolume.getId(), destDataStore);
                destVolumeInfo.processEvent(Event.MigrationCopyRequested);
                destVolumeInfo.processEvent(Event.MigrationCopySucceeded);
                destVolumeInfo.processEvent(Event.MigrationRequested);

                boolean sameController = srcStoragePool.getPoolType() == StoragePoolType.Linstor
                        && srcStoragePool.getHostAddress().equals(destStoragePool.getHostAddress());

                if (!sameController) {
                    // Cross-storage migration (StorPool->Linstor, SMP->Linstor, or different Linstor controller)
                    // Create a new Linstor resource on the destination
                    String rscGrp = getRscGrp(destStoragePool);
                    createLinstorResource(api, rscName, srcVolume.getSize(), rscGrp,
                            srcVolume.getName(), vmTO.getName());
                }

                // Ensure resource is available on the destination host
                makeResourceAvailable(api, rscName, destNodeName);
                cleanupList.add(new ResourceCleanup(api, rscName, destNodeName, !sameController));

                // Set allow-two-primaries for live migration
                setAllowTwoPrimaries(api, rscName, destNodeName);

                // Get the device path on destination (DRBD or raw volume)
                String destPath;
                try {
                    destPath = LinstorUtil.getDevicePath(api, rscName);
                } catch (ApiException | CloudRuntimeException e) {
                    destPath = LinstorUtil.formatDrbdByResDevicePath(rscName);
                    LOG.warn(String.format(
                            "Linstor: Could not resolve device path for %s, using default: %s",
                            rscName, destPath));
                }

                // Update destination volume in DB
                destVolume.setPath(srcVolume.getUuid());
                destVolume.setFolder("/dev/");
                _volumeDao.update(destVolume.getId(), destVolume);

                destVolumeInfo = _volumeDataFactory.getVolume(destVolume.getId(), destDataStore);

                // Configure migration disk info
                MigrateDiskInfo migrateDiskInfo = new MigrateDiskInfo(
                        srcVolumeInfo.getPath(),
                        MigrateDiskInfo.DiskType.BLOCK,
                        MigrateDiskInfo.DriverType.RAW,
                        MigrateDiskInfo.Source.DEV,
                        destPath);
                migrateDiskInfoList.add(migrateDiskInfo);

                migrateStorage.put(srcVolumeInfo.getPath(), migrateDiskInfo);
                srcToDestVolumeInfo.put(srcVolumeInfo, destVolumeInfo);
            }

            // Send PrepareForMigrationCommand to destination host
            PrepareForMigrationCommand pfmc = new PrepareForMigrationCommand(vmTO);
            try {
                Answer pfma = _agentManager.send(destHost.getId(), pfmc);
                if (pfma == null || !pfma.getResult()) {
                    String details = pfma != null ? pfma.getDetails() : "null answer returned";
                    errMsg = String.format("Unable to prepare for migration: %s", details);
                    throw new AgentUnavailableException(errMsg, destHost.getId());
                }
            } catch (OperationTimedoutException e) {
                errMsg = String.format("Prepare for migration timed out: %s", e.getMessage());
                throw new AgentUnavailableException(errMsg, destHost.getId());
            }

            // Build and send MigrateCommand to source host
            VMInstanceVO vm = _vmDao.findById(vmTO.getId());
            boolean isWindows = _guestOsCategoryDao
                    .findById(_guestOsDao.findById(vm.getGuestOSId()).getCategoryId())
                    .getName().equalsIgnoreCase("Windows");

            MigrateCommand migrateCommand = new MigrateCommand(
                    vmTO.getName(), destHost.getPrivateIpAddress(), isWindows, vmTO, true);
            migrateCommand.setWait(StorageManager.KvmStorageOnlineMigrationWait.value());
            migrateCommand.setMigrateStorage(migrateStorage);
            migrateCommand.setMigrateStorageManaged(true);
            migrateCommand.setMigrateDiskInfoList(migrateDiskInfoList);
            migrateCommand.setAutoConvergence(StorageManager.KvmAutoConvergence.value());

            MigrateAnswer migrateAnswer = (MigrateAnswer) _agentManager.send(srcHost.getId(), migrateCommand);

            // Validate before post-migration to avoid double handlePostMigration calls
            // (once here, once in the catch block)
            if (migrateAnswer == null) {
                throw new CloudRuntimeException("Unable to get an answer to the migrate command");
            }
            if (!migrateAnswer.getResult()) {
                errMsg = migrateAnswer.getDetails();
                throw new CloudRuntimeException(errMsg);
            }

            handlePostMigration(true, srcToDestVolumeInfo, vmTO, destHost, cleanupList);
        } catch (AgentUnavailableException | OperationTimedoutException | CloudRuntimeException ex) {
            errMsg = String.format(
                    "Live migration of VM [%s] to host [%s] with Linstor storage failed: %s",
                    vmTO.getId(), destHost.getId(), ex.getMessage());
            LOG.error(errMsg, ex);

            // Clean up Linstor resources and DB records on failure
            handlePostMigration(false, srcToDestVolumeInfo, vmTO, destHost, cleanupList);
        } finally {
            CopyCmdAnswer copyCmdAnswer = new CopyCmdAnswer(errMsg);
            CopyCommandResult result = new CopyCommandResult(null, copyCmdAnswer);
            result.setResult(errMsg);
            callback.complete(result);
        }
    }

    // -- Private helper methods --

    private DevelopersApi getLinstorAPI(StoragePoolVO pool, Map<String, DevelopersApi> cache) {
        String url = pool.getHostAddress();
        return cache.computeIfAbsent(url, LinstorUtil::getLinstorAPI);
    }

    private String getRscGrp(StoragePoolVO storagePool) {
        String userInfo = storagePool.getUserInfo();
        return userInfo != null && !userInfo.isEmpty() ? userInfo : "DfltRscGrp";
    }

    private void createLinstorResource(DevelopersApi api, String rscName, long sizeBytes,
            String rscGrp, String volName, String vmName) {
        try {
            // Check if resource already exists
            List<ResourceWithVolumes> existing = api.viewResources(
                    java.util.Collections.emptyList(),
                    java.util.Collections.singletonList(rscName),
                    java.util.Collections.emptyList(),
                    null, null, null);
            if (existing != null && !existing.isEmpty()) {
                LOG.info(String.format("Linstor: Resource %s already exists, skipping creation", rscName));
                return;
            }
        } catch (ApiException e) {
            LOG.warn(String.format(
                    "Linstor: Failed to check if resource %s exists: %s. Attempting creation.",
                    rscName, e.getBestMessage()));
        }

        try {
            ResourceGroupSpawn rscGrpSpawn = new ResourceGroupSpawn();
            rscGrpSpawn.setResourceDefinitionName(rscName);
            rscGrpSpawn.addVolumeSizesItem(sizeBytes / 1024); // Linstor uses KiB

            LOG.info(String.format("Linstor: Spawning resource %s in group %s", rscName, rscGrp));
            ApiCallRcList answers = api.resourceGroupSpawn(rscGrp, rscGrpSpawn);
            if (answers.hasError()) {
                throw new CloudRuntimeException(
                        String.format("Linstor: Failed to spawn resource %s: %s",
                                rscName, LinstorUtil.getBestErrorMessage(answers)));
            }

            ApiCallRcList auxAnswers = LinstorUtil.applyAuxProps(api, rscName, volName, vmName);
            if (auxAnswers != null && auxAnswers.hasError()) {
                LOG.warn(String.format("Linstor: Failed to set aux props on %s: %s (non-fatal)",
                        rscName, LinstorUtil.getBestErrorMessage(auxAnswers)));
            }
        } catch (ApiException apiEx) {
            throw new CloudRuntimeException(
                    String.format("Linstor: Failed to create resource %s: %s",
                            rscName, apiEx.getBestMessage()), apiEx);
        }
    }

    private void makeResourceAvailable(DevelopersApi api, String rscName, String nodeName) {
        try {
            ResourceMakeAvailable rma = new ResourceMakeAvailable();
            ApiCallRcList answers = api.resourceMakeAvailableOnNode(rscName, nodeName, rma);
            if (answers.hasError()) {
                throw new CloudRuntimeException(
                        String.format("Linstor: Unable to make resource %s available on node %s: %s",
                                rscName, nodeName, LinstorUtil.getBestErrorMessage(answers)));
            }
            LOG.info(String.format("Linstor: Resource %s available on node %s", rscName, nodeName));
        } catch (ApiException apiEx) {
            throw new CloudRuntimeException(
                    String.format("Linstor: Failed to make resource %s available on %s: %s",
                            rscName, nodeName, apiEx.getBestMessage()), apiEx);
        }
    }

    private void setAllowTwoPrimaries(DevelopersApi api, String rscName, String destNodeName) {
        try {
            String inUseNode = LinstorUtil.isResourceInUse(api, rscName);
            if (inUseNode != null && !inUseNode.equalsIgnoreCase(destNodeName)) {
                if (LinstorUtil.areResourcesDiskless(api, rscName,
                        Arrays.asList(inUseNode, destNodeName))) {
                    setAllowTwoPrimariesOnRD(api, rscName);
                } else {
                    setAllowTwoPrimariesOnRC(api, rscName, inUseNode, destNodeName);
                }
            }
        } catch (ApiException apiEx) {
            LOG.warn(String.format(
                    "Linstor: Failed to set allow-two-primaries on %s: %s (non-fatal)",
                    rscName, apiEx.getBestMessage()));
        }
    }

    private void setAllowTwoPrimariesOnRD(DevelopersApi api, String rscName) throws ApiException {
        ResourceDefinitionModify rdm = new ResourceDefinitionModify();
        Properties props = new Properties();
        props.put("DrbdOptions/Net/allow-two-primaries", "yes");
        props.put("DrbdOptions/Net/protocol", "C");
        rdm.setOverrideProps(props);
        ApiCallRcList answers = api.resourceDefinitionModify(rscName, rdm);
        if (answers.hasError()) {
            LOG.warn(String.format(
                    "Linstor: Unable to set allow-two-primaries on RD %s: %s",
                    rscName, LinstorUtil.getBestErrorMessage(answers)));
        }
    }

    private void setAllowTwoPrimariesOnRC(DevelopersApi api, String rscName,
            String inUseNode, String destNode) throws ApiException {
        ResourceConnectionModify rcm = new ResourceConnectionModify();
        Properties props = new Properties();
        props.put("DrbdOptions/Net/allow-two-primaries", "yes");
        props.put("DrbdOptions/Net/protocol", "C");
        rcm.setOverrideProps(props);
        ApiCallRcList answers = api.resourceConnectionModify(rscName, inUseNode, destNode, rcm);
        if (answers.hasError()) {
            LOG.warn(String.format(
                    "Linstor: Unable to set allow-two-primaries on RC %s/%s/%s: %s",
                    rscName, inUseNode, destNode, LinstorUtil.getBestErrorMessage(answers)));
        }
    }

    private void removeAllowTwoPrimaries(DevelopersApi api, String rscName) {
        try {
            ResourceDefinitionModify rdm = new ResourceDefinitionModify();
            List<String> deleteProps = new ArrayList<>();
            deleteProps.add("DrbdOptions/Net/allow-two-primaries");
            deleteProps.add("DrbdOptions/Net/protocol");
            rdm.deleteProps(deleteProps);
            api.resourceDefinitionModify(rscName, rdm);
        } catch (ApiException apiEx) {
            LOG.warn(String.format(
                    "Linstor: Failed to remove allow-two-primaries from %s: %s (non-fatal)",
                    rscName, apiEx.getBestMessage()));
        }
    }

    private VolumeVO duplicateVolumeOnAnotherStorage(Volume volume, StoragePoolVO storagePoolVO) {
        Long lastPoolId = volume.getPoolId();
        VolumeVO newVol = new VolumeVO(volume);
        newVol.setInstanceId(null);
        newVol.setChainInfo(null);
        newVol.setPath(null);
        newVol.setFolder(null);
        newVol.setPodId(storagePoolVO.getPodId());
        newVol.setPoolId(storagePoolVO.getId());
        newVol.setLastPoolId(lastPoolId);
        return _volumeDao.persist(newVol);
    }

    private void handlePostMigration(boolean success,
            Map<VolumeInfo, VolumeInfo> srcToDestVolumeInfo,
            VirtualMachineTO vmTO, Host destHost,
            List<ResourceCleanup> cleanupList) {

        if (!success) {
            // Rollback PrepareForMigration
            try {
                PrepareForMigrationCommand pfmc = new PrepareForMigrationCommand(vmTO);
                pfmc.setRollback(true);
                Answer pfma = _agentManager.send(destHost.getId(), pfmc);
                if (pfma == null || !pfma.getResult()) {
                    LOG.debug("Failed to rollback prepare for migration");
                }
            } catch (Exception e) {
                LOG.debug("Failed to rollback prepare for migration", e);
            }
        }

        // Clean up allow-two-primaries on all resources
        for (ResourceCleanup cleanup : cleanupList) {
            removeAllowTwoPrimaries(cleanup.api, cleanup.rscName);
        }

        for (Map.Entry<VolumeInfo, VolumeInfo> entry : srcToDestVolumeInfo.entrySet()) {
            VolumeInfo srcVolumeInfo = entry.getKey();
            VolumeInfo destVolumeInfo = entry.getValue();

            if (success) {
                srcVolumeInfo.processEvent(Event.OperationSuccessed);
                destVolumeInfo.processEvent(Event.OperationSuccessed);

                // Swap volume UUIDs
                _volumeDao.updateUuid(srcVolumeInfo.getId(), destVolumeInfo.getId());

                VolumeVO volumeVO = _volumeDao.findById(destVolumeInfo.getId());
                volumeVO.setFormat(ImageFormat.RAW);
                _volumeDao.update(volumeVO.getId(), volumeVO);

                // Destroy and expunge source volume
                try {
                    _volumeService.destroyVolume(srcVolumeInfo.getId());
                    srcVolumeInfo = _volumeDataFactory.getVolume(srcVolumeInfo.getId());
                    AsyncCallFuture<VolumeApiResult> destroyFuture =
                            _volumeService.expungeVolumeAsync(srcVolumeInfo);
                    if (destroyFuture.get().isFailed()) {
                        LOG.debug("Failed to clean up source volume on storage");
                    }
                } catch (Exception e) {
                    LOG.debug("Failed to clean up source volume on storage", e);
                }

                // Update snapshot references
                if (!_snapshotDao.listByVolumeId(srcVolumeInfo.getId()).isEmpty()) {
                    _snapshotDao.updateVolumeIds(srcVolumeInfo.getId(), destVolumeInfo.getId());
                    _snapshotStoreDao.updateVolumeIds(srcVolumeInfo.getId(), destVolumeInfo.getId());
                }
            } else {
                destVolumeInfo.processEvent(Event.OperationFailed);
                srcVolumeInfo.processEvent(Event.OperationFailed);

                // Revoke access to the destination volume on the destination host
                try {
                    _volumeService.revokeAccess(destVolumeInfo, destHost, destVolumeInfo.getDataStore());
                } catch (Exception e) {
                    LOG.debug("Failed to revoke access for dest volume", e);
                }

                // Clean up failed destination volume
                try {
                    _volumeService.destroyVolume(destVolumeInfo.getId());
                    destVolumeInfo = _volumeDataFactory.getVolume(destVolumeInfo.getId());
                    AsyncCallFuture<VolumeApiResult> destroyFuture =
                            _volumeService.expungeVolumeAsync(destVolumeInfo);
                    if (destroyFuture.get().isFailed()) {
                        LOG.debug("Failed to clean up dest volume on storage");
                    }
                } catch (Exception e) {
                    LOG.debug("Failed to clean up dest volume on storage", e);
                }

                // Clean up Linstor resources created for this migration
                for (ResourceCleanup cleanup : cleanupList) {
                    if (cleanup.deleteOnFail) {
                        deleteLinstorResource(cleanup.api, cleanup.rscName, cleanup.nodeName);
                    }
                }
            }
        }
    }

    private void deleteLinstorResource(DevelopersApi api, String rscName, String nodeName) {
        try {
            // Remove the resource from the destination node (diskless or diskful)
            api.resourceDelete(rscName, nodeName, true);
            LOG.info(String.format("Linstor: Deleted resource %s on node %s", rscName, nodeName));

            // If no resources remain, delete the resource definition entirely
            List<Resource> remaining = api.resourceList(rscName, null, null);
            if (remaining == null || remaining.isEmpty()) {
                api.resourceDefinitionDelete(rscName);
                LOG.info(String.format("Linstor: Deleted resource definition %s", rscName));
            }
        } catch (ApiException apiEx) {
            LOG.warn(String.format(
                    "Linstor: Failed to delete resource %s on node %s: %s (non-fatal)",
                    rscName, nodeName, apiEx.getBestMessage()));
        }
    }

    /**
     * Tracks Linstor resources created during migration for cleanup on failure.
     */
    private static class ResourceCleanup {
        final DevelopersApi api;
        final String rscName;
        final String nodeName;
        final boolean deleteOnFail;

        ResourceCleanup(DevelopersApi api, String rscName, String nodeName, boolean deleteOnFail) {
            this.api = api;
            this.rscName = rscName;
            this.nodeName = nodeName;
            this.deleteOnFail = deleteOnFail;
        }
    }
}

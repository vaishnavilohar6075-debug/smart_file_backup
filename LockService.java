package com.apnileap.scheduler.lock;

import com.apnileap.scheduler.dto.AcquireLockRequest;
import com.apnileap.scheduler.entity.BackupJob;
import com.apnileap.scheduler.entity.JobState;
import com.apnileap.scheduler.entity.Lease;
import com.apnileap.scheduler.entity.LeaseState;
import com.apnileap.scheduler.entity.WaitForEdge;
import com.apnileap.scheduler.exception.BusinessException;
import com.apnileap.scheduler.metrics.MetricsService;
import com.apnileap.scheduler.repository.BackupJobRepository;
import com.apnileap.scheduler.repository.LeaseRepository;
import com.apnileap.scheduler.repository.WaitForEdgeRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class LockService {
    private final LeaseRepository leaseRepo;
    private final WaitForEdgeRepository edgeRepo;
    private final BackupJobRepository jobRepo;
    private final MetricsService metricsService;

    public LockService(LeaseRepository leaseRepo, WaitForEdgeRepository edgeRepo,
                       BackupJobRepository jobRepo, MetricsService metricsService) {
        this.leaseRepo = leaseRepo;
        this.edgeRepo = edgeRepo;
        this.jobRepo = jobRepo;
        this.metricsService = metricsService;
    }

    @Value("${scheduler.lease.default-timeout-ms:30000}")
    private long defaultTimeoutMs;

    @Transactional(noRollbackFor = BusinessException.class)
    public Lease acquireLock(AcquireLockRequest req) {
        Optional<Lease> existing = leaseRepo.findByFileIdAndState(req.getFileId(), LeaseState.ACTIVE);
        if (existing.isPresent()) {
            Lease act = existing.get();
            if (act.getExpiresAt().isBefore(Instant.now())) {
                act.setState(LeaseState.EXPIRED);
                leaseRepo.save(act);
                metricsService.incrementExpiredLeases();
                requeueWaiters(act.getFileId());
            } else {
                metricsService.incrementLockConflicts();
                edgeRepo.save(WaitForEdge.builder()
                        .waitingJobId(req.getHolderId())
                        .blockingJobId(act.getHolderId())
                        .build());
                jobRepo.findById(req.getHolderId()).ifPresent(job -> {
                    job.setState(JobState.WAITING_FOR_LOCK);
                    jobRepo.save(job);
                });
                throw new BusinessException("LOCK_CONFLICT", "File is already leased to " + act.getHolderId());
            }
        }

        long timeout = req.getLeaseTimeoutMs() != null ? req.getLeaseTimeoutMs() : defaultTimeoutMs;
        Lease lease = Lease.builder()
                .leaseId(UUID.randomUUID().toString())
                .fileId(req.getFileId())
                .holderId(req.getHolderId())
                .lockMode(req.getLockMode())
                .state(LeaseState.ACTIVE)
                .acquiredAt(Instant.now())
                .expiresAt(Instant.now().plusMillis(timeout))
                .build();
        return leaseRepo.save(lease);
    }

    @Transactional
    public void releaseLock(String leaseId) {
        leaseRepo.findById(leaseId).ifPresent(lease -> {
            lease.setState(LeaseState.RELEASED);
            leaseRepo.save(lease);
            edgeRepo.deleteByWaitingJobIdOrBlockingJobId(lease.getHolderId(), lease.getHolderId());
            requeueWaiters(lease.getFileId());
        });
    }

    @Scheduled(fixedRate = 5000)
    @Transactional
    public void expireLeases() {
        List<Lease> expired = leaseRepo.findByExpiresAtBeforeAndState(Instant.now(), LeaseState.ACTIVE);
        for (Lease l : expired) {
            l.setState(LeaseState.EXPIRED);
            leaseRepo.save(l);
            metricsService.incrementExpiredLeases();
            edgeRepo.deleteByWaitingJobIdOrBlockingJobId(l.getHolderId(), l.getHolderId());
            requeueWaiters(l.getFileId());
        }
    }

    public void requeueWaiters(String fileId) {
        for (BackupJob waiter : jobRepo.findByFileIdAndState(fileId, JobState.WAITING_FOR_LOCK)) {
            waiter.setState(JobState.QUEUED);
            jobRepo.save(waiter);
        }
    }
}

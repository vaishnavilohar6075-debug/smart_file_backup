package com.apnileap.scheduler.lock;

import com.apnileap.scheduler.entity.BackupJob;
import com.apnileap.scheduler.entity.JobState;
import com.apnileap.scheduler.entity.WaitForEdge;
import com.apnileap.scheduler.entity.WorkerState;
import com.apnileap.scheduler.events.SseService;
import com.apnileap.scheduler.metrics.MetricsService;
import com.apnileap.scheduler.repository.BackupJobRepository;
import com.apnileap.scheduler.repository.WaitForEdgeRepository;
import com.apnileap.scheduler.repository.WorkerRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class DeadlockService {
    private final WaitForEdgeRepository edgeRepo;
    private final BackupJobRepository jobRepo;
    private final WorkerRepository workerRepo;
    private final LockService lockService;
    private final MetricsService metricsService;
    private final SseService sseService;

    public DeadlockService(WaitForEdgeRepository edgeRepo, BackupJobRepository jobRepo,
                           WorkerRepository workerRepo, LockService lockService,
                           MetricsService metricsService, SseService sseService) {
        this.edgeRepo = edgeRepo;
        this.jobRepo = jobRepo;
        this.workerRepo = workerRepo;
        this.lockService = lockService;
        this.metricsService = metricsService;
        this.sseService = sseService;
    }

    @Value("${scheduler.deadlock.enabled:true}")
    private boolean scanEnabled;

    @Scheduled(fixedDelayString = "${scheduler.deadlock.interval-ms:5000}")
    public void autoScan() {
        if (!scanEnabled) {
            return;
        }
        analyseAndRecover();
    }

    @Transactional
    public Map<String, Object> analyseAndRecover() {
        List<WaitForEdge> edges = edgeRepo.findAll();
        Map<String, List<String>> graph = new HashMap<>();
        for (WaitForEdge e : edges) {
            graph.computeIfAbsent(e.getWaitingJobId(), k -> new ArrayList<>()).add(e.getBlockingJobId());
        }

        List<String> cycle = findCycle(graph);
        if (cycle.isEmpty()) {
            return Map.of("cycle_detected", false);
        }

        metricsService.incrementDeadlocksDetected();

        List<BackupJob> cycleJobs = jobRepo.findAllById(cycle);

        BackupJob toCancel = cycleJobs.stream()
                .min(Comparator.comparing(BackupJob::getPriority, Comparator.nullsLast(Integer::compareTo))
                        .thenComparing(BackupJob::getSubmittedAt, Comparator.nullsLast(Comparator.reverseOrder())))
                .orElse(null);

        if (toCancel != null) {
            if (toCancel.getLeaseId() != null) {
                lockService.releaseLock(toCancel.getLeaseId());
                toCancel.setLeaseId(null);
            }
            if (toCancel.getAssignedWorkerId() != null) {
                workerRepo.findById(toCancel.getAssignedWorkerId()).ifPresent(worker -> {
                    worker.setState(WorkerState.AVAILABLE);
                    worker.setCurrentJobId(null);
                    workerRepo.save(worker);
                });
                toCancel.setAssignedWorkerId(null);
            }
            toCancel.setState(JobState.CANCELLED);
            toCancel.setCompletedAt(Instant.now());
            jobRepo.save(toCancel);

            edgeRepo.deleteByWaitingJobIdOrBlockingJobId(toCancel.getJobId(), toCancel.getJobId());
            metricsService.incrementDeadlocksResolved();

            sseService.publishEvent(Map.of(
                    "event_type", "DEADLOCK_RESOLVED",
                    "job_id", toCancel.getJobId(),
                    "state", "CANCELLED",
                    "timestamp", Instant.now().toString(),
                    "correlation_id", toCancel.getCorrelationId() != null ? toCancel.getCorrelationId() : "sys"
            ));
        }

        return Map.of(
                "cycle_detected", true,
                "cycle", cycle,
                "recovered_by_cancelling", toCancel != null ? toCancel.getJobId() : "none"
        );
    }

    private List<String> findCycle(Map<String, List<String>> graph) {
        Set<String> visited = new HashSet<>();
        Set<String> recStack = new HashSet<>();
        Map<String, String> parent = new HashMap<>();

        for (String node : graph.keySet()) {
            if (!visited.contains(node)) {
                List<String> cycle = dfs(node, graph, visited, recStack, parent);
                if (!cycle.isEmpty()) return cycle;
            }
        }
        return Collections.emptyList();
    }

    private List<String> dfs(String node, Map<String, List<String>> graph, Set<String> visited, Set<String> recStack, Map<String, String> parent) {
        visited.add(node);
        recStack.add(node);

        for (String neighbor : graph.getOrDefault(node, Collections.emptyList())) {
            if (!visited.contains(neighbor)) {
                parent.put(neighbor, node);
                List<String> cycle = dfs(neighbor, graph, visited, recStack, parent);
                if (!cycle.isEmpty()) return cycle;
            } else if (recStack.contains(neighbor)) {
                List<String> cycle = new ArrayList<>();
                cycle.add(neighbor);
                String curr = node;
                while (curr != null && !curr.equals(neighbor)) {
                    cycle.add(curr);
                    curr = parent.get(curr);
                }
                cycle.add(neighbor);
                Collections.reverse(cycle);
                return cycle;
            }
        }
        recStack.remove(node);
        return Collections.emptyList();
    }
}

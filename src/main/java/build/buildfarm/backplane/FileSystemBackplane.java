package build.buildfarm.backplane;

import static build.buildfarm.common.io.Utils.getOrIOException;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ToolDetails;
import build.buildfarm.common.CasIndexResults;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.Watcher;
import build.buildfarm.common.function.InterruptingRunnable;
import build.buildfarm.common.grpc.TracingMetadataUtils;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.BackplaneStatus;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.DispatchedOperation;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.GetClientStartTimeRequest;
import build.buildfarm.v1test.GetClientStartTimeResult;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.ShardWorker;
import build.buildfarm.worker.resources.LocalResourceSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;

public class FileSystemBackplane implements Backplane {
  private final Path root;
  private final Instance instance;

  public FileSystemBackplane(Path root, Instance instance) {
    this.root = root;
    this.instance = instance;
  }

  /**
   * Register a runnable for when the backplane cannot guarantee watch deliveries. This runnable may
   * throw InterruptedException
   *
   * <p>onSubscribe is called via the subscription thread, and is not called when operations are not
   * listened to
   */
  @Override
  public void setOnUnsubscribe(InterruptingRunnable onUnsubscribe) {
    // throw new UnsupportedOperationException();
  }

  /** Start the backplane's operation */
  @Override
  public void start(String publicClientName, Consumer<String> onWorkerRemoved) throws IOException {
    // throw new UnsupportedOperationException();
  }

  /** Stop the backplane's operation */
  @Override
  public void stop() throws InterruptedException {
    // throw new UnsupportedOperationException();
  }

  /** Indicates whether the backplane has been stopped */
  @Override
  public boolean isStopped() {
    return false;
  }

  /** Adds a worker to the set of active workers. */
  @Override
  public void addWorker(ShardWorker shardWorker) throws IOException {
    // throw new UnsupportedOperationException();
  }

  /**
   * Removes a worker's name from the set of active workers.
   *
   * <p>Return true if the worker was removed, and false if it was not a member of the set.
   */
  @Override
  public boolean removeWorker(String workerName, String reason) throws IOException {
    return true; // throw new UnsupportedOperationException();
  }

  @Override
  public CasIndexResults reindexCas() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deregisterWorker(String hostName) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** Page all operations */
  @Override
  public ScanResult<Operation> scanExecutions(String cursor, int count) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScanResult<Operation> scanExecutions(String toolInvocationId, String cursor, int count)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  /** Page all toolInvocations */
  @Override
  public ScanResult<String> scanToolInvocations(String cursor, int count) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScanResult<String> scanToolInvocations(String correlatedInvocationsId, String cursor, int count)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  /** Page all correlatedInvocations */
  @Override
  public ScanResult<String> scanCorrelatedInvocations(String cursor, int count) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScanResult<String> scanCorrelatedInvocations(String scope, String value, String cursor, int count)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScanResult<String> scanCorrelatedInvocationIndexKeys(String cursor, int count) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScanResult<String> scanCorrelatedInvocationIndexEntries(String cursor, int count, String keyMatch)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScanResult<Map.Entry<String, QueueEntry>> scanQueuedOperations(String cursor, int count)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScanResult<ExecuteEntry> scanPrequeuedOperations(String cursor, int count) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** Returns a map of the worker name and its start time for given workers. */
  @Override
  public Map<String, Long> getWorkersStartTimeInEpochSecs(Set<String> workerNames) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** Returns the insert time epoch in seconds for the digest. */
  @Override
  public long getDigestInsertTime(Digest blobDigest) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** Returns the entry iterable of all active storage workers. */
  @Override
  public Collection<ShardWorker> getStorageWorkers() throws IOException {
    return ImmutableList.of(ShardWorker.newBuilder().setEndpoint("localhost:8981").build()); // throw new UnsupportedOperationException();
  }

  // TODO this is just a namespace, but kind of jank for just digest function as a string...
  /**
   * The AC stores full ActionResult objects in a hash map where the key is the digest of the action
   * result and the value is the actual ActionResult object.
   *
   * <p>Retrieves and returns an action result from the hash map.
   */
  @Override
  public ActionResult getActionResult(ActionKey actionKey) throws IOException {
    return getOrIOException(instance.getActionResult(actionKey, TracingMetadataUtils.fromCurrentContext()));
  }

  /**
   * The AC stores full ActionResult objects in a hash map where the key is the digest of the action
   * result and the value is the actual ActionResult object.
   *
   * <p>Remove an action result from the hash map.
   */
  @Override
  public void removeActionResult(ActionKey actionKey) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** Bulk remove action results */
  @Override
  public void removeActionResults(Iterable<ActionKey> actionKeys) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Identify an action that should not be executed, and respond to all requests it matches with
   * failover-compatible responses.
   */
  @Override
  public void blocklistAction(String actionId) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * The AC stores full ActionResult objects in a hash map where the key is the digest of the action
   * result and the value is the actual ActionResult object.
   *
   * <p>Stores an action result in the hash map.
   */
  @Override
  public void putActionResult(ActionKey actionKey, ActionResult actionResult) throws IOException {
    try {
      instance.putActionResult(actionKey, actionResult, TracingMetadataUtils.fromCurrentContext());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  /**
   * The CAS is represented as a map where the key is the digest of the blob that is being stored
   * and the value is a set of the names of the workers where that blob is stored.
   *
   * <p>Adds the name of a worker to the set of workers that store a blob.
   */
  @Override
  public void addBlobLocation(Digest blobDigest, String workerName) throws IOException {
    // throw new UnsupportedOperationException();
  }

  /** Remove or add workers to a blob's location set as requested */
  @Override
  public void adjustBlobLocations(Digest blobDigest, Set<String> addWorkers, Set<String> removeWorkers)
      throws IOException {
    // throw new UnsupportedOperationException();
  }

  /**
   * The CAS is represented as a map where the key is the digest of the blob that is being stored
   * and the value is a set of the names of the workers where that blob is stored.
   *
   * <p>Adds the name of a worker to the set of workers that store multiple blobs.
   */
  @Override
  public void addBlobsLocation(Iterable<Digest> blobDigest, String workerName) throws IOException {
    // throw new UnsupportedOperationException();
  }

  /**
   * The CAS is represented as a map where the key is the digest of the blob that is being stored
   * and the value is a set of the names of the workers where that blob is stored.
   *
   * <p>Removes the name of a worker from the set of workers that store a blob.
   */
  @Override
  public void removeBlobLocation(Digest blobDigest, String workerName) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * The CAS is represented as a map where the key is the digest of the blob that is being stored
   * and the value is a set of the names of the workers where that blob is stored.
   *
   * <p>Removes the name of a worker from the set of workers that store multiple blobs.
   */
  @Override
  public void removeBlobsLocation(Iterable<Digest> blobDigests, String workerName) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * The CAS is represented as a map where the key is the digest of the blob that is being stored
   * and the value is a set of the names of the workers where that blob is stored.
   *
   * <p>Returns a random worker from the set of workers that store a blob.
   */
  @Override
  public String getBlobLocation(Digest blobDigest) throws IOException {
    return "localhost:8981"; // throw new UnsupportedOperationException();
  }

  /**
   * The CAS is represented as a map where the key is the digest of the blob that is being stored
   * and the value is a set of the names of the workers where that blob is stored.
   *
   * <p>Returns the set of the names of all workers that store a blob.
   */
  @Override
  public Set<String> getBlobLocationSet(Digest blobDigest) throws IOException {
    return ImmutableSet.of("localhost:8981"); // throw new UnsupportedOperationException();
  }

  @Override
  public Map<Digest, Set<String>> getBlobDigestsWorkers(Iterable<Digest> blobDigests) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Executions are stored in a hash map where the key is the name of the execution and the value is
   * a longrunning Operation object.
   *
   * <p>Retrieves and returns an execution from the hash map.
   */
  @Override
  public Operation getExecution(String executionName) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Operations are stored in a hash map where the key is the name of the operation and the value is
   * the actual Operation object.
   *
   * <p>Stores an operation in the hash map.
   */
  @Override
  public boolean putOperation(Operation operation, ExecutionStage.Value stage) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ExecuteEntry deprequeueOperation() throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  /**
   * The state of operations is tracked in a series of lists representing the order in which the
   * work is to be processed (queued, dispatched, and completed).
   *
   * <p>Moves an operation from the list of queued operations to the list of dispatched operations.
   */
  @Override
  public QueueEntry dispatchOperation(List<Platform.Property> provisions, LocalResourceSet resourceSet)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  /**
   * Pushes an operation onto the head of the list of queued operations after a rejection which does
   * not require revalidation
   */
  @Override
  public void rejectOperation(QueueEntry queueEntry) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Updates the backplane to indicate that the operation is being queued and should not be
   * considered immediately lost
   */
  @Override
  public void queueing(String operationName) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * The state of executions is tracked in a series of lists representing the order in which the
   * work is to be processed (queued, dispatched, and completed).
   *
   * <p>Updates a dispatchedExecution requeue_at and returns whether the execution is still valid.
   */
  @Override
  public boolean pollExecution(QueueEntry queueEntry, ExecutionStage.Value stage, long requeueAt)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  /** Complete an operation */
  @Override
  public void completeOperation(String operationName) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** Delete an operation */
  @Override
  public void deleteOperation(String operationName) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** Register a watcher for an operation */
  @Override
  public ListenableFuture<Void> watchExecution(String executionName, Watcher watcher) {
    return immediateFailedFuture(new UnsupportedOperationException());
  }

  /** Page all dispatched operations */
  @Override
  public ScanResult<DispatchedOperation> scanDispatchedOperations(String cursor, int count)
      throws IOException {
    return new ScanResult<>(SENTINEL_PAGE_TOKEN, ImmutableList.of());
  }

  /** Requeue a dispatched execution */
  @Override
  public void requeueDispatchedExecution(QueueEntry queueEntry) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @brief Acquire an execution to merge for an action
   * @details Prevent any further merges of the actionKey with executions.
   * @param actionKey The source action identifier.
   * @return An execution if the actionKey has an association, null otherwise.
   * @note Suggested return identifier: execution.
   */
  @Override
  public @Nullable Operation mergeExecution(ActionKey actionKey) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @brief Remove actionKey execution merge association.
   * @details Prevent any further merges of the actionKey with executions.
   * @param actionKey The source action identifier.
   */
  @Override
  public void unmergeExecution(ActionKey actionKey) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @brief Submit an execution into the arrival queue
   * @details Interacts with executions and actions map to present an execution for processing or
   *     signal mergability.
   * @param executeEntry The submission into the arrival queue.
   * @param execution The execution object created with name key if not mergable.
   * @param ignoreMerge Whether to consider mergability.
   * @return false if ignoreMerge is false and the execution could be merged, true otherwise.
   * @note Suggested return identifier: prequeued.
   */
  @Override
  public boolean prequeue(ExecuteEntry executeEntry, Operation execution, boolean ignoreMerge)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void queue(QueueEntry queueEntry, Operation operation) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** Test for whether a request is blocklisted */
  @Override
  public boolean isBlocklisted(RequestMetadata requestMetadata) throws IOException {
    return false; // throw new UnsupportedOperationException();
  }

  /** Test for whether an operation may be queued */
  @Override
  public boolean canQueue() throws IOException {
    return false;
  }

  /** Test for whether an operation may be prequeued */
  @Override
  public boolean canPrequeue() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public BackplaneStatus backplaneStatus() throws IOException {
    return BackplaneStatus.getDefaultInstance();
  }

  @Override
  public Boolean propertiesEligibleForQueue(List<Platform.Property> provisions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetClientStartTimeResult getClientStartTime(GetClientStartTimeRequest request) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** Set expiry time for digests */
  @Override
  public void updateDigestsExpiry(Iterable<Digest> digests) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void indexCorrelatedInvocationsId(
      String correlatedInvocationsId, Map<String, List<String>> indexScopeValues)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addToolInvocationId(
      String toolInvocationId, String correlatedInvocationsId, ToolDetails toolDetails)
      throws IOException {
    // throw new UnsupportedOperationException();
  }

  @Override
  public void incrementRequestCounters(
      String actionId, String toolInvocationId, String actionMnemonic, String targetId)
      throws IOException {
    // throw new UnsupportedOperationException();
  }
}

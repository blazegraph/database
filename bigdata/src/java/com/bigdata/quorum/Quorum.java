package com.bigdata.quorum;

import java.rmi.Remote;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * A quorum is a collection of services instances comprising the same logical
 * service. The {@link Quorum} interface provides a view of the quorum from the
 * perspective of one of those member services. A quorum has a replication
 * factor <em>k</em>. A member service may "join" with a quorum when it shares
 * an agreement with at least (k+1)/2 other quorum members concerning its state.
 * When there are at least (k+1)/2 member services joined with the quorum, the
 * quorum is "meets" and elects a leader. Each time a new leader is elected, it
 * assigns a unique token to the quorum. Client reads and writes will block
 * unless the quorum is met. If a quorum breaks, then any outstanding writes
 * will be discarded and the client must wait until the quorum meets again
 * before reading or writing on the quorum.
 * <p>
 * The first service in the chain is the quorum <i>leader</i>. Client writes are
 * directed to the leader (also known as the master) and replicated to the
 * member services joined with the quorum. Clients may read from any service in
 * the quorum, but only for historical commit points. The current uncommitted
 * state is only available from the quorum leader as the followers are only
 * guaranteed to be consistent as of each commit point.
 * <p>
 * The services in the quorum are organized into a <em>write pipeline</em> for
 * the purposes of replication. The leader is always the first service in the
 * write pipeline. Services which are not yet joined with the quorum are allowed
 * into the write pipeline while they are synchronizing with the leader. When a
 * service is synchronized, it can then join the quorum.
 * <p>
 * This interface reflects state changes in the quorum for some logical service.
 * Logical services and their physical instances are managed outside of the
 * {@link Quorum} interface. When a physical service is starts or stops, it uses
 * a {@link Quorum} for the corresponding logical service to observe and manage
 * state changes in the emergent quorum of physical services for the same
 * logical service.
 * 
 * @todo Consider hot spare allocation. When a hot spare is assigned to a
 *       quorum, the hot spare will be a quorum member and will join the write
 *       pipeline, but it will not yet be joined with the quorum. If the failed
 *       service comes back online and the quorum is once more at capacity, then
 *       the hot spare should be taken down by the same administrative
 *       monitoring which caused it to be allocated. That administrative
 *       monitoring needs to distinguish between the number of created instances
 *       of a service (something which we track in zookeeper for the SMS) and
 *       the #of join members of the quorum (also tracked in zookeeper, but this
 *       is a different value). Normally, the SMS will not allocate a new
 *       physical service instance once the replication count of the service has
 *       been satisfied, which is why this needs to happen in slightly different
 *       logic.
 *       <p>
 *       If we allow temporary overcapacity during the recruitment of a hot
 *       spare then we may have to closely review the definition of a member
 *       service which can vote and be counted in a quorum in order to avoid
 *       degrading the quorum by, essentially, having <i>k</i> as an even
 *       number. Perhaps a hot spare must <em>replace</em> a quorum member, in
 *       which case the other service must not be allowed back into the quorum
 *       once the hot spare has been assigned to the quorum. This would mean
 *       that the other service was disabled and/or destroyed before the hot
 *       spare was allocated.
 *       <p>
 *       Also related to hot spare allocation is support for planned downtime.
 *       Who has responsibility for ensuring that planned downtime for a node
 *       does not cause a quorum to break? Presumably we should not recruit a
 *       hot spare during planned downtime.
 */
public interface Quorum<S extends Remote, C extends QuorumClient<S>> {

    /**
     * The constant used to indicate that there is no quorum (@value
     * {@value #NO_QUORUM}).
     * <p>
     * Note: The quorum token is formed by adding one to the
     * {@link #lastValidToken()}. The initial value of the
     * {@link #lastValidToken()} is {@value #NO_QUORUM}. Therefore, the the
     * first valid quorum token is ZERO (0).
     */
    long NO_QUORUM = -1;

    /**
     * Return <em>k</em>, the target replication factor. The replication factor
     * must be a non-negative odd integer (1, 3, 5, 7, etc). A quorum exists
     * only when <code>(k + 1)/2</code> physical services for the same logical
     * service have an agreement on state. A single service with
     * <code>k := 1</code> is the degenerate case and has a minimum quorum size
     * of ONE (1). High availability is only possible when <code>k</code> is GT
     * ONE (1). Thus <code>k := 3</code> is the minimum value for which services
     * can be highly available and has a minimum quorum size of <code>2</code>.
     */
    int replicationFactor();

    /**
     * The current token for the quorum. The initial value before the quorum has
     * met is {@link #NO_QUORUM}. When a leader is elected, it sets the current
     * token as <code>token := lastValidToken() + 1</code>. The current token is
     * cleared to {@link #NO_QUORUM} if the leader leaves the met quorum. It is
     * cleared {@link #NO_QUORUM} if the quorum breaks. While a leader may be
     * elected many times for the same <em>lastCommitTime</em>, a new quorum
     * token is assigned each time a leader is elected.
     */
    long token();
    
    /**
     * The quorum token which was assigned the last time a leader was elected.
     */
    long lastValidToken();

    /**
     * Return <code>true</code> if {@link #replicationFactor()} is GT ONE (1).
     * High availability exists (in principle) when the
     * {@link Quorum#replicationFactor()} <em>k</em> is greater than one. High
     * availability exists (in practice) when the {@link Quorum}
     * {@link Quorum#isQuorumMet() is met} for a {@link Quorum} that is
     * configured for high availability.
     * 
     * @return <code>true</code> if this {@link Quorum} is highly available
     *         <em>in principle</code>
     */
    boolean isHighlyAvailable();

    /**
     * Return true iff the #of services joined with the quorum is GTE (k + 1). A
     * service with a met quorum is highly available <em>in practice</em>.
     */
    boolean isQuorumMet();

    /**
     * Add a listener
     * 
     * @param listener
     *            The listener.
     * 
     * @throws IllegalArgumentException
     *             if the listener is null.
     * @throws IllegalArgumentException
     *             if the listener is the quorum's client (the quorum's client
     *             is always a listener).
     */
    void addListener(QuorumListener listener);

    /**
     * Remove a listener (the quorum's client is always a listener).
     * 
     * @param listener
     *            The listener.
     * 
     * @throws IllegalArgumentException
     *             if the listener is null.
     * @throws IllegalArgumentException
     *             if the listener is the quorum's client (the quorum's client
     *             is always a listener).
     */
    void removeListener(QuorumListener listener);
    
    /**
     * Return the identifiers for the member services (all known physical
     * services for the logical service).
     * 
     * @return The {@link UUID}s of the member services.
     */
    UUID[] getMembers();

    /**
     * Return an immutable snapshot of the votes cast by the quorum members.
     */
    Map<Long,UUID[]> getVotes();
    
    /**
     * Return the vote cast by the service.
     * 
     * @param serviceId
     *            The service.
     * @return The vote cast by that service -or- <code>null</code> if the
     *         service has not cast a vote.
     * 
     * @throws IllegalArgumentException
     *             if the serviceId is <code>null</code>.
     */
    Long getCastVote(final UUID serviceId);
    
    /**
     * Return the identifiers for the member services joined with this quorum.
     * If the quorum was met at the moment the request was processed, then the
     * first element of the array was the quorum leader as of that moment and
     * the remaining elements are followers (non-blocking).
     * 
     * @return The {@link UUID}s of the member services joined with this quorum.
     */
    UUID[] getJoinedMembers();

    /**
     * Return the service identifiers for the services in the write pipeline in
     * the order in which they will accept and relay writes.
     * 
     * @return The {@link UUID}s of the ordered services in the write pipeline.
     */
    UUID[] getPipeline();

    /**
     * Return the {@link UUID} of the service which is the last service in the
     * write pipeline.
     * 
     * @return The {@link UUID} of the last service in the write pipeline or
     *         <code>null</code> if there are no services in the write pipeline.
     */
    UUID getLastInPipeline();

    /**
     * Return the {@link UUID}of the service in the pipeline which is
     * immediately upstream from (prior to) and downstream from (next to) the
     * specified service. These are, respectively, the service from which it
     * receives data (upstream) and to which it sends data (downstream).
     * 
     * @param serviceId
     *            The service id.
     * 
     * @return Either <code>null</code> if the <i>serviceId</i> does not appear
     *         in the write pipeline -or- an array of two elements whose values
     *         are: [0] The upstream serviceId in the write pipeline, which will
     *         be <code>null</code> iff <i>serviceId</i> is the first service in
     *         the write pipeline; and [1] The downstream service in the write
     *         pipeline, which will be <code>null</code> iff <i>serviceId</i> is
     *         the last service in the write pipeline.
     */
    UUID[] getPipelinePriorAndNext(final UUID serviceId);
    
    /**
     * Await a met quorum (blocking). If the {@link Quorum}is not met, then this
     * will block until the {@link Quorum} meets.
     * 
     * @return The current quorum token.
     * 
     * @throws AsynchronousQuorumCloseException
     *             if {@link #terminate()} is invoked while awaiting a quorum
     *             meet.
     */
    long awaitQuorum() throws InterruptedException,
            AsynchronousQuorumCloseException;

    /**
     * Await a met quorum (blocking). If the {@link Quorum}is not met, then this
     * will block until the {@link Quorum} meets.
     * 
     * @param timeout
     *            The timeout.
     * @param units
     *            The timeout units.
     * 
     * @return The current quorum token.
     * 
     * @throws AsynchronousQuorumCloseException
     *             if {@link #terminate()} is invoked while awaiting a quorum
     *             meet.
     * @throws TimeoutException
     *             if the timeout expired before the quorum met.
     */
    long awaitQuorum(long timeout, TimeUnit units) throws InterruptedException,
            AsynchronousQuorumCloseException, TimeoutException;

    /**
     * Await a met break (blocking). If the {@link Quorum} is met, then this
     * will block until the {@link Quorum} breaks.
     * 
     * @throws AsynchronousQuorumCloseException
     *             if {@link #terminate()} is invoked while awaiting a quorum
     *             break.
     */
    void awaitBreak() throws InterruptedException,
            AsynchronousQuorumCloseException;

    /**
     * Await a met break (blocking). If the {@link Quorum} is met, then this
     * will block until the {@link Quorum} breaks.
     * 
     * @param timeout
     *            The timeout.
     * @param units
     *            The timeout units.
     * 
     * @throws AsynchronousQuorumCloseException
     *             if {@link #terminate()} is invoked while awaiting a quorum
     *             break.
     * @throws TimeoutException
     *             if the timeout expired before the quorum breaks.
     */
    void awaitBreak(long timeout, TimeUnit units) throws InterruptedException,
            AsynchronousQuorumCloseException, TimeoutException;

    /**
     * Assert that the quorum associated with the token is still valid. The
     * pattern for using this method is to save the {@link #token()} somewhere.
     * This method may then be invoked to verify that the saved token is still
     * valid and, hence, that the quorum is still met.
     * 
     * @param token
     *            The token for the quorum.
     * 
     * @throws QuorumException
     *             if the quorum is invalid.
     * @throws QuorumException
     *             if the <i>token</i> is {@link #NO_QUORUM}.
     */
    void assertQuorum(long token);

    /**
     * Assert that the token is still valid and that the {@link #getClient()} is
     * the quorum leader.
     * 
     * @param token
     *            A quorum token.
     * @throws QuorumException
     *             if the quorum is invalid.
     * @throws QuorumException
     *             if the <i>token</i> is {@link #NO_QUORUM}.
     * @throws QuorumException
     *             if our client is not the quorum leader.
     */
    void assertLeader(long token);
    
    /**
     * The {@link UUID} of the leader {@link Quorum} leader (non-blocking).
     * 
     * @return The {@link UUID} of the leader {@link Quorum} leader -or-
     *         <code>null</code> if the quorum is not met.
     */
    UUID getLeaderId();
    
    /**
     * Start any asynchronous processing associated with maintaining the
     * {@link Quorum} state.
     */
    void start(C client);

    /**
     * Terminate any asynchronous processing associated with maintaining the
     * {@link Quorum} state.
     */
    void terminate();
    
    /**
     * Return the {@link QuorumClient} iff the quorum is running.
     * 
     * @return The {@link QuorumClient}.
     * 
     * @throws IllegalStateException
     *             if the quorum is not running.
     */
    C getClient();

    /**
     * Return the {@link QuorumMember} iff the quorum is running.
     * 
     * @return The {@link QuorumMember}.
     * 
     * @throws IllegalStateException
     *             if the quorum is not running.
     * @throws UnsupportedOperationException
     *             if the client does not implement {@link QuorumMember}.
     */
    QuorumMember<S> getMember();

    /**
     * The object used to effect changes in distributed quorum state on the
     * behalf of the {@link QuorumMember}.
     * 
     * @return The {@link QuorumActor} which will effect changes in the
     *         distributed state of the quorum.
     * 
     * @throws IllegalStateException
     *             if the quorum is not running.
     * @throws IllegalStateException
     *             if the client is not a {@link QuorumMember}.
     */
    QuorumActor<S, C> getActor();

}

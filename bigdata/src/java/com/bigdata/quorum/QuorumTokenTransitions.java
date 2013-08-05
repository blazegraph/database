package com.bigdata.quorum;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;

/**
 * Wraps the token/join transitions in a testable manner.
 * 
 * Both enables JUnit testing and also provides context to represent
 * better abstractions on the quorum/service states.
 */
public class QuorumTokenTransitions {
    
    final long currentQuorumToken;
    final long newQuorumToken;
    final long currentHaReady;

    public final boolean didBreak;
    public final boolean didMeet;
    public final boolean didJoinMetQuorum;
    public final boolean didLeaveMetQuorum;
    
    final boolean wasMet;
    final boolean isMet;
    final boolean isJoined;
    final boolean wasJoined;
    
    public QuorumTokenTransitions(final long currentQuorumToken,
            final long newQuorumToken, final QuorumService<HAGlue> service,
            final long haReady) {

        this(currentQuorumToken, newQuorumToken, service != null
                && service.isJoinedMember(newQuorumToken), haReady);
        
    }
    
    public QuorumTokenTransitions(final long currentQuorumToken,
            final long newQuorumToken, final boolean joined,
            final long haReady) {
    	
        this.currentHaReady = haReady;
        this.currentQuorumToken = currentQuorumToken;
        this.newQuorumToken = newQuorumToken;
                    
        isJoined = joined;
        wasJoined = haReady != Quorum.NO_QUORUM;
        wasMet = currentQuorumToken != Quorum.NO_QUORUM;
        isMet = newQuorumToken != Quorum.NO_QUORUM;
        
        // Both quorum token and haReadyToken agree with newValue.
		final boolean noTokenChange = currentQuorumToken == newQuorumToken
				&& currentQuorumToken == currentHaReady;

		/*
         * TODO: more understanding required as to the effect of this clause
         */
        if (noTokenChange && isJoined) {
            didBreak = false;
            didMeet = false;
            didJoinMetQuorum = didJoin();
            didLeaveMetQuorum = false;
        } else if (isBreak()) {
            didBreak = true; // quorum break.
            didMeet = false;
            didJoinMetQuorum = false;
            didLeaveMetQuorum = wasJoined; // if service was joined with met quorum, then it just left the met quorum.           	
        } else if (isMeet()) {

            /*
             * Quorum meet.
             * 
             * We must wait for the lock to update the token.
             */
            
            didBreak = false;
            didMeet = true; // quorum meet.
            didJoinMetQuorum = false;
            didLeaveMetQuorum = false;

        } else if (didJoin()) {

            /*
             * This service is joining a quorum that is already met.
             */
            
            didBreak = false;
            didMeet = false;
            didJoinMetQuorum = true; // service joined with met quorum.
            didLeaveMetQuorum = false;
            
		} else if (didLeaveMet()) {

			/*
			 * This service is leaving a quorum that is already met (but
			 * this is not a quorum break since the new token is not
			 * NO_QUORUM).
			 */

			didBreak = false;
			didMeet = false;
			didJoinMetQuorum = false;
			didLeaveMetQuorum = true; // service left met quorum. quorum
										// still met.

        } else {
            didBreak = false;
            didMeet = false;
            didJoinMetQuorum = false;
            didLeaveMetQuorum = false;
            
            // throw new AssertionError("Bad state from: oldToken=" + currentQuorumToken + ", newToken=" + newQuorumToken + ", haReady=" + haReady + ", isJoined=" + isJoined);
        }
        
        checkStates();
    }
    
    // TODO Document rationale for each assertion.
    private void checkStates() {
    	if (wasJoined && wasMet && currentHaReady > currentQuorumToken)
    		throw new AssertionError("haReady greater than current token");
    	
    	if (wasMet && isMet && newQuorumToken < currentQuorumToken) {
    		throw new AssertionError("next token less than current token");
    	}
    	
    	if (wasMet && isMet && newQuorumToken != currentQuorumToken) {
    		throw new AssertionError("New quorum token without quorum break first, current: " + currentQuorumToken + ", new: " + newQuorumToken);
    	}
    	
    if (didMeet && didJoinMetQuorum) {
            /*
             * It is not possible to both join with an existing quorum and
             * to be one of the services that caused a quorum to meet. These
             * conditions are exclusive.
             */
        throw new AssertionError("didMeet && didJoinMetQuorum");
    }
        
    	/**
    	 * This is a bit odd, it is okay, but probably didLeaveMetQuorum will
    	 * only be true iff isJoined
    	 */
//    	if (didBreak && didLeaveMetQuorum) {
//    		throw new AssertionError("didBreak && didLeaveMetQuorum");
//    	}
//    	if (didLeaveMetQuorum && !isJoined) { // TODO Why not valid?
//    		throw new AssertionError("didLeaveMetQuorum && !isJoined");
//    	}
    }
    
    /*
     * Methods for making decisions in the ctor. They are NOT for public
     * use. There are simple public fields that report out the decisions
     * make using these methods.
     */
    
//    public final boolean noChange() {
//    	return currentQuorumToken == newQuorumToken;
//    }
    
    private final boolean isBreak() {
    	return wasMet && !isMet;
    }
    
    private final boolean isMeet() {
    	return isMet & !wasMet;
    }
    
    private final boolean didJoin() {
    	return isMet && isJoined && !wasJoined;
    }
    
//    private final boolean didLeave() {
//    	return isBreak() && wasJoined;
//    }
    
    private final boolean didLeaveMet() {
    	return isMet && wasJoined && !isJoined;
    }
    
    public final String showState() {
        return ""; // TODO showState().
    }
}


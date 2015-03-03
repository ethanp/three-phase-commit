package system;

import messages.vote_req.VoteRequest;

/**
 * Ethan Petuchowski 3/2/15
 */
public abstract class DistributedSystem {
    abstract boolean processRequestToCompletion(VoteRequest voteRequest);
}

package system;

import messages.Message;
import messages.vote_req.VoteRequest;

/**
 * Ethan Petuchowski 3/2/15
 */
public abstract class DistributedSystem {
    abstract Message processRequestToCompletion(VoteRequest voteRequest);
}

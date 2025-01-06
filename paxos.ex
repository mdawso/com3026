defmodule Paxos do

  def start(name, participants) do
    pid = spawn(Paxos, :init, [name, participants])
    :global.re_register_name(name, pid)
    pid
  end

  def init(name, participants) do
    state = %{
      name: name,
      participants: participants,
      
      inst: nil,
      prepared_procs: nil,
    }
    run(state)
  end

  def run(state) do
    state = receive do
      {:propose, inst, value, t} -> 
       
        # This means we need a new consensus inst. First run prepare phase
        state = {state | prepared_procs: [], inst: inst} # Reset the prepared procs set new instance id
        
        for participant <- state.participants do
          send(participant, {:prepare, self()}) # Send a prepare request to each process
        end
        
        receive do
          {:prepared, from} -> state = %{state | prepared_procs: [from | state.prepared_procs]} # Append each process that responds to the prepared procs for this instance
        after t -> state
        end

        # We have now collected all processes that will participate in this instance of consensus
        # Begin round 1
        # Next, we attempt to get all procs to accept the value that is being proposed

        
      
      {:get_decision, from} -> send(from, {:decision, state.decision})
      {:prepare, from} -> send(from, {:prepared, self()})
    end
    run(state)
  end

  def propose(pid, inst, value, t) do
    send(pid, {:propose, inst, value, t})
  end

  def get_decision(pid, inst, t) do
    send(pid, {:get_decision, self()})
    receive do
      {:decision, decision} -> decision
    end
  end

end
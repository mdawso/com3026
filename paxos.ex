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
      proposal: nil,        # current proposal
      recv_proposal: nil,   # received proposal
      proposal_number: 0,   # current proposal number
      decision: nil,        # final decision
      timeouts: 5000        # timeout duration for proposals
    }
    run(state)
  end

  def run(state) do
    state = receive do
      {:propose, inst, value} ->
        new_state = propose(inst, value, state)
        {:noreply, new_state}

      {:get_decision, inst} ->
        send(inst, {:decision, state.decision})
        state

      {:prepare, inst, proposal_number} ->
        # Prepare phase: respond with promise or previously accepted proposal
        state = prepare(inst, proposal_number, state)
        {:noreply, state}

      {:accept, inst, proposal_number, value} ->
        # Accept phase: accept the proposal
        state = accept(inst, proposal_number, value, state)
        {:noreply, state}

      {:abort} ->
        # Abort: reset state
        IO.puts("Aborting proposal...")
        state = %{state | proposal: nil, recv_proposal: nil, proposal_number: 0}
        {:noreply, state}
    end
    run(state)
  end

  # --------------------------------------------------------------
  # Proposer Logic

  def propose(inst, value, state) do
    new_proposal_number = state.proposal_number + 1
    state = %{state | proposal: value, proposal_number: new_proposal_number}
    
    # Send prepare phase to acceptors
    U.beb_broadcast(state.participants, {:prepare, inst, new_proposal_number})

    # Wait for promises or abort
    response = wait_for_promises_or_abort(state.timeouts)

    case response do
      {:promises, promises} ->
        # After receiving majority of promises, send proposal
        U.beb_broadcast(state.participants, {:accept, inst, new_proposal_number, value})
        response = wait_for_decision_or_abort(state.timeouts)
        case response do
          {:decision, v} -> {:decision, v}
          {:abort} -> {:abort}
        end
      {:abort} -> {:abort}
    end
  end

  def wait_for_promises_or_abort(timeout) do
    receive do
      {:promise, _} -> 
        # collect promises (you should collect a majority here)
        {:promises, []}  # Simplified for illustration
    after
      timeout -> {:abort}
    end
  end

  def wait_for_decision_or_abort(timeout) do
    receive do
      {:decide, value} -> {:decision, value}
    after
      timeout -> {:abort}
    end
  end

  # --------------------------------------------------------------
  # Acceptors Logic

  def prepare(inst, proposal_number, state) do
    # Check if proposal number is higher than the current one
    if proposal_number > state.proposal_number do
      # Respond with promise
      send(inst, {:promise, proposal_number, state.proposal})
      # Update state with new proposal number
      %{state | proposal_number: proposal_number}
    else
      # Reject the proposal
      send(inst, {:reject, proposal_number})
      state
    end
  end

  def accept(inst, proposal_number, value, state) do
    if proposal_number >= state.proposal_number do
      # Accept the proposal
      send(inst, {:decide, value})
      %{state | proposal_number: proposal_number, decision: value}
    else
      state
    end
  end

  # --------------------------------------------------------------
  # Required API functions

  def propose(pid, inst, value, t) do
    send(pid, {:propose, inst, value})
    receive do
      {:decision, v} -> {:decision, v}
      {:abort} -> {:abort}
    after
      t -> {:timeout}
    end
  end

  def get_decision(pid, inst, t) do
    send(pid, {:get_decision, inst})
    receive do
      {:decision, v} -> v
    after
      t -> nil
    end
  end
  # -------------------------------------------------------
end

# Utilities
defmodule U do
  def unicast(p, m) when is_pid(p), do: send(p, m)
  def unicast(p, m) do
    case :global.whereis_name(p) do
      pid when is_pid(pid) -> send(pid, m)
      :undefined -> :ok
    end
  end
  def beb_broadcast(dest, m) do for p <- dest do unicast(p, m) end end
end


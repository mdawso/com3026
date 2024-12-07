defmodule Paxos do

  # start function
  def start(name, participants) do

    pid = spawn(Paxos, :init, [name, participants])
    :global.register_name(name, pid)
    pid

  end

  # init function
  def init(name, participants) do

    state = %{
      name: name,
      participants: participants,
      instances: %{}
    }
    loop(state)

  end

  # loop function
  defp loop(state) do

    receive do
      {:propose, inst, value, from} ->
        state = handle_propose(state, inst, value, from)
        loop(state)

      {:get_decision, inst, from} ->
        state = handle_get_decision(state, inst, from)
        loop(state)
    end

  end

  # handle_propose function
  defp handle_propose(state, inst, value, from) do

    # Implement paxos logic here
    send(from, {:decision, value})
    state

  end

  # handle_get_decision function
  defp handle_get_decision(state, inst, from) do

    decision = Map.get(state.instances, inst, nil)
    send(from, decision)
    state

  end

  # propose function
  def propose(pid, inst, value, t) do

    send(pid, {:propose, inst, value, self()})

    receive do

      {:decision, v} -> {:decision, v}
      {:abort} -> {:abort}
      after t -> {:timeout}

    end

  end

  # get_decision function
  def get_decision(pid, inst, t) do

    send(pid, {:get_decision, inst, self()})

    receive do
      decision -> decision
      after t -> nil

    end

  end

end

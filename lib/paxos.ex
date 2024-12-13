defmodule Paxos do

  # implement required api functions
  # start(atom:name, atom[]:participants) -> pid: pid
  # propose(pid: pid, inst: inst, float: value, float: t)

  @spec start(atom, [atom]) :: pid
  def start(name, participants) do
    pid = spawn(fn -> paxos_process(name, participants) end)
    :global.register_name(name, pid)
    pid
  end

  defp paxos_process(name, participants) do
    # Implementation of the Paxos process goes here
    receive do
      # Handle messages
    end
  end

  @spec propose(pid, float, float, float) :: {:decision, float} | {:abort} | {:timeout}
  def propose(pid, inst, value, t) do
    send(pid, {:propose, self(), inst, value})
    receive do
      {:decision, v} -> {:decision, v}
      {:abort} -> {:abort}
    after
      t -> {:timeout}
    end
  end

end

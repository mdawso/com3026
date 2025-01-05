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
    }
    run(state)
  end

  def run(state) do
    IO.puts("YOUR MUM LMAO")
    run(state)
  end

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

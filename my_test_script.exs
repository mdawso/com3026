IEx.Helpers.c("paxos.ex", ".")
pid = Paxos.start(:paxos, [:paxos])
Paxos.propose(pid, 1, 69, 5)
Paxos.get_decision(pid, 1, 5)
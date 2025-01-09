IEx.Helpers.c("paxos.ex", ".")
IEx.Helpers.c("account_server.ex", ".")

p1pid = Paxos.start(:p1, [:p1,:p2,:p3])
p2pid = Paxos.start(:p2, [:p1,:p2,:p3])
p3pid = Paxos.start(:p3, [:p1,:p2,:p3])

accs = AccountServer.start(:accs, :p1)

AccountServer.deposit(accs, 69)
bal = AccountServer.balance(accs)
IO.puts(bal)

AccountServer.withdraw(accs, 42)
bal = AccountServer.balance(accs)
IO.puts(bal)
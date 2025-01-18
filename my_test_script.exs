# Tests the paxos impl with my server

IEx.Helpers.c("paxos.ex", ".")
IEx.Helpers.c("server.ex", ".")

p1pid = Paxos.start(:p1, [:p1,:p2,:p3])
p2pid = Paxos.start(:p2, [:p1,:p2,:p3])
p3pid = Paxos.start(:p3, [:p1,:p2,:p3])

serverpid = Server.start(:server, :p1, 100)

out = Server.book_seat(:server, 10) |> IO.inspect(label: "Booking seat 10")
out = Server.book_seat(:server, 10) |> IO.inspect(label: "Booking seat 10 again")
out = Server.book_seat(:server, 0) |> IO.inspect(label: "Booking seat 0")
out = Server.book_seat(:server, 101) |> IO.inspect(label: "Booking seat 101")

For part 2 of the coursework I have written a server for booking seats in a theatre.
To use the server, it is very similar to the provided account server.
When creating the server you use start/3 providing a name, paxos process and number of seats.
To book a seat you use book_seat/1, providing an integer for the seat number.
The server will then return:
    -> {:ok, :seat_booked} if the seat is free.
    -> {:error, :seat_already_booked} if the seat has already been booked by someone else
    -> {:error, :invalid_seat_number} if the seat is outside the bounds (0, num_of_seats]
    -> {:error, :abort} if abort is called at any point during consensus
    -> {:error, :timeout} if the paxos times out
    -> {:error, :server_not_found} if the server is crashed ect




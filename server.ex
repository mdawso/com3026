defmodule Server do

	defp log(x) do if false do IO.puts(x) end end
	
	def start(name, paxos_proc, num_of_seats) do
		pid = spawn(Server, :init, [name, paxos_proc, num_of_seats])
		pid = case :global.re_register_name(name, pid) do
			:yes -> pid  
			:no  -> nil
		end
		log("Server #{name} started with PID #{inspect(pid)}")
		pid
	end

	def init(name, paxos_proc, num_of_seats) do
		state = %{
			name: name,
			pax_pid: get_paxos_pid(paxos_proc),
			last_instance: 0,
			num_of_seats: num_of_seats,
			booked_seats: %{},
			pending: {0, nil}
		}
		run(state)
	end

	# Get pid of a Paxos instance to connect to
	defp get_paxos_pid(paxos_proc) do
		case :global.whereis_name(paxos_proc) do
			pid when is_pid(pid) -> pid
			:undefined -> raise(Atom.to_string(paxos_proc))
		end
	end

	defp wait_for_reply(_, 0), do: nil
	defp wait_for_reply(r, attempt) do
		msg = receive do
			msg -> msg
			after 1000 -> 
				send(r, {:poll_for_decisions})
				nil
		end
		if msg, do: msg, else: wait_for_reply(r, attempt-1)
	end

	def book_seat(server_name, seat_number) do
		case :global.whereis_name(server_name) do
			pid when is_pid(pid) ->
				send(pid, {:book_seat, seat_number, self()})
				case wait_for_reply(pid, 5) do
					{:ok, :seat_booked} -> {:ok, :seat_booked}
					{:error, :seat_already_booked} -> {:error, :seat_already_booked}
					{:error, :invalid_seat_number} -> {:error, :invalid_seat_number}
					{:abort} -> {:error, :abort}
					_ -> {:error, :timeout}
				end
			:undefined -> {:error, :server_not_found}
		end
	end

	def run(state) do
		state = receive do
			{:book_seat, seat_number, client} ->
				state = poll_for_decisions(state)
				if seat_number < 1 or seat_number > state.num_of_seats do
					send(client, {:error, :invalid_seat_number})
					state
				else
					if Paxos.propose(state.pax_pid, state.last_instance + 1, {:book_seat, seat_number, client}, 1000) == {:abort} do
						send(client, {:abort})
						state
					else
						%{state | pending: {state.last_instance + 1, client}}
					end
				end

			{:poll_for_decisions} ->
				poll_for_decisions(state)

			_ -> state
		end
		run(state)
	end

	defp poll_for_decisions(state) do
		case Paxos.get_decision(state.pax_pid, i = state.last_instance + 1, 1000) do
			{:book_seat, seat_number, client} ->
				state = if Map.has_key?(state.booked_seats, seat_number) do
					send(client, {:error, :seat_already_booked})
					%{state | pending: {0, nil}}
				else
					send(client, {:ok, :seat_booked})
					%{state | booked_seats: Map.put(state.booked_seats, seat_number, true), pending: {0, nil}}
				end
				poll_for_decisions(%{state | last_instance: i})

			nil -> state
		end
	end

end
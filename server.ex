defmodule Server do
    
    def start(name, paxos_proc, num_of_seats) do
        pid = spawn(Server, :init, [name, paxos_proc, num_of_seats])
        pid = case :global.re_register_name(name, pid) do
            :yes -> pid  
            :no  -> nil
        end
        IO.puts(if pid, do: "registered #{name}", else: "failed to register #{name}")
        pid
    end

    def init(name, paxos_proc, num_of_seats) do
        state = %{
            name: name,
            pax_pid: get_paxos_pid(paxos_proc),
            last_instance: 0,
            pending: {0, nil},
            balance: 0,
            num_of_seats: num_of_seats,
            booked_seats: %{},
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

    def run(state) do
        receive do
            {:book_seat, seat_number, from} ->
                if seat_number < 1 or seat_number > state.num_of_seats do
                    send(from, {:error, :invalid_seat_number})
                    run(state)
                else
                    if Map.has_key?(state.booked_seats, seat_number) do
                        send(from, {:error, :seat_already_booked})
                        run(state)
                    else
                        new_state = %{state | booked_seats: Map.put(state.booked_seats, seat_number, true)}
                        send(from, {:ok, :seat_booked})
                        run(new_state)
                    end
                end
            _ ->
                run(state)
        end
    end
    
    def book_seat(server_name, seat_number) do
        case :global.whereis_name(server_name) do
            pid when is_pid(pid) ->
                send(pid, {:book_seat, seat_number, self()})
                receive do
                    {:ok, :seat_booked} -> {:ok, :seat_booked}
                    {:error, :seat_already_booked} -> {:error, :seat_already_booked}
                    {:error, :invalid_seat_number} -> {:error, :invalid_seat_number}
                after
                    5000 -> {:error, :timeout}
                end
            :undefined -> {:error, :server_not_found}
        end
    end
end
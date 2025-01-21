defmodule Paxos do

	defp log(x) do if false do IO.puts(x) end end

	def start(name, participants) do
		pid = spawn(Paxos, :init, [name, participants])
		:global.re_register_name(name, pid)
		log("Started Paxos process with name #{name} and participants #{inspect(participants)}")
		pid
	end

	def init(name, participants) do
		state = %{
			name: name,
			participants: participants,
			
			parent_name: nil, 
			leader: nil,
			
			bal: {0, name},
			a_bal: nil, 
			a_val: nil, 
			a_bal_max: nil, 
			a_val_max: nil,
			
			value: nil,
			inst: nil,

			prepared_quorum: 0,
			accepted_quorum: 0,

			proposal: nil,
			proposal_h: nil,
			
			decision: nil,
			decisions_map: %{},

			prepare_called: false,

			instances: %{},
			parent_pid: nil,
		}
		LeaderElector.start(name, participants)
		run(state)
	end

	defp handle_check_instance(state, pid, inst, value) do
		log("Handle Check Instance")
		state = %{state | parent_name: pid, parent_pid: pid}
		case Map.get(state.decisions_map, inst) do
			nil ->
				log("No decision found for instance #{inst}, broadcasting proposal")
				beb(state.participants, {:update_instances_map, inst, pid, [value]})
				if state.inst == nil do
					send(self(), {:broadcast, pid, inst, value})
					%{state | inst: inst}
				else
					state
				end
			decision ->
				log("Decision found for instance #{inst}, sending decision")
				send(state.parent_name, {:decision, decision})
				state
		end
	end

	defp handle_update_instance_map(state, inst_id, parent_pid, proposals) do
		log("Handle Update Instance Map")
		case Map.has_key?(state.instances, inst_id) do
			false ->
				log("Instance #{inst_id} not found, updating instances map")
				new_instances = Map.put(state.instances, inst_id, {parent_pid, proposals})
				%{state | instances: new_instances}
			true ->
				log("Instance #{inst_id} already exists")
				state
		end
	end

	defp handle_return_decision(state, pid, inst) do
		log("Handle Return Decision")
		inst_decision = Map.get(state.decisions_map, inst)
		send(pid, {:return_decision, inst_decision})
		state
	end

	defp handle_broadcast_proposal(state, value, inst) do
		log("Handle Broadcast Proposal")
		state = %{state | proposal_h: value, inst: inst}
		if state.name == state.leader && !state.prepare_called do
			log("Leader broadcasting prepare message")
			beb(state.participants, {:prepare, increment_ballot_number(state.bal, state.name), state.name, state.inst})
			%{state | prepare_called: true}
		else
			state
		end
	end

	defp handle_broadcast(state, pid, inst, value) do
		log("Handle Broadcast")
		state = %{state | proposal: value, inst: inst}
		beb(state.participants, {:broadcast_proposal, value, inst})
		if state.name == state.leader && !state.prepare_called do
			log("Leader broadcasting prepare message")
			beb(state.participants, {:prepare, increment_ballot_number(state.bal, state.name), state.name, state.inst})
			state = %{state | prepare_called: true}
			state
		else
			state
		end
	end

	defp handle_leader_elect(state, p) do
		log("Handle Leader Elect")
		if state.name == p do
			log("#{state.name} is the new leader")
			if state.proposal != nil || state.proposal_h != nil do
				beb(state.participants, {:prepare, increment_ballot_number(state.bal, p), p, state.inst})
				state
			end
		end
		%{state | leader: p, bal: state.bal}
	end

	defp handle_prepare(state, b, leader, inst) do
		log("Handle Prepare")
		if state.inst == inst do
			if compare_ballot(b, &>/2, state.bal) do
				log("Ballot is higher, sending prepared message")
				unicast(leader, {:prepared, b, state.a_bal, state.a_val, state.inst})
				%{state | bal: b, leader: leader, a_bal_max: nil, a_val_max: nil}
			else
				log("Ballot is lower, sending nack message")
				unicast(leader, {:nack, b, inst})
				state
			end
			state
		else
			state
		end
	end

	defp handle_prepared(state, b, a_bal, a_val, inst) do
		log("Handle Prepared")
		if inst == state.inst do
			state = %{state | prepared_quorum: state.prepared_quorum + 1}

			state = 
				if a_bal != nil and a_val != nil do
					if state.a_bal_max == nil or a_bal > state.a_bal_max do
						%{state | a_bal_max: a_bal, a_val_max: a_val}
					else
						state
					end
				else
					state
				end

			if state.leader == state.name and state.prepared_quorum >= (floor(length(state.participants) / 2) + 1) do
				log("Quorum reached, sending accept message")
				state = %{state | prepared_quorum: 0}

				v = 
					if state.a_val_max == nil do
						state.proposal || state.proposal_h
					else
						state.a_val_max
					end

				state = %{state | value: v}
				beb(state.participants, {:accept, b, state.value, state.name, inst})
			end

			state
		else
			state
		end
	end

	defp handle_accept(state, b, v, sender, inst) do
		log("Handle Accept")
		if inst == state.inst do
			if compare_ballot(b, &>=/2, state.bal) do
				log("Ballot is higher or equal, sending accepted message")
				unicast(sender, {:accepted, b, inst})
				%{state | bal: b, a_val: v, a_bal: b}
			else
				log("Ballot is lower, sending nack message")
				unicast(sender, {:nack, b, inst})
				state
			end
		else
			state
		end
	end

	defp handle_accepted(state, b, inst) do
		log("Handle Accepted")
		if inst == state.inst do
			state = %{state | accepted_quorum: state.accepted_quorum + 1}
			state = if state.leader == state.name do
				if state.accepted_quorum >= (floor(length(state.participants) / 2) + 1) do
					log("Quorum reached, broadcasting decision")
					state = %{state | accepted_quorum: 0}
					if state.parent_name != nil do
						send(state.parent_name, {:decision, state.a_val})
					end
					beb(state.participants, {:received_decision, state.a_val, inst})
					state
				else
					state
				end
			else
				state
			end
			state
		else
			state
		end
	end

	defp handle_nack(state, b, inst) do
		log("Handle Nack")
		if state.inst == inst do
			if state.leader == state.name do
				log("Leader received nack, aborting")
				if state.parent_pid != nil do
					send(state.parent_pid, {:abort})
					beb(state.participants, {:abort, b, inst})
				end
				beb(state.participants, {:abort, b, inst})
			end
		end
		state
	end

	defp handle_abort(state, b, inst) do
		log("Handle Abort")
		if state.inst == inst do
			if state.parent_pid != nil do
				send(state.parent_pid, {:abort})
				beb(state.participants, {:abort, b, inst})
			end
		end
		state
	end

	defp handle_recieved_decision(state, v, inst) do
		log("Handle Received Decision")
		state = %{state | decision: v, decisions_map: Map.put(state.decisions_map, inst, v), instances: Map.delete(state.instances, inst), inst: nil}
		state = %{state |
			bal: {0, state.name},
			a_bal: nil,
			a_val: nil,
			value: nil,
			inst: nil,
			prepared_quorum: 0,
			accepted_quorum: 0,
			proposal: nil,
			proposal_h: nil,
			decision: nil,
			parent_pid: nil,
			prepare_called: false,
		}
		foundInstance = Enum.at(Map.keys(state.instances), 0)
		foundInstanceProposal = Map.get(state.instances, foundInstance)
		if foundInstance != nil do
			{pid, proposals} = foundInstanceProposal
			if state.name == state.leader do
				send(self(), {:broadcast, pid, foundInstance, Enum.at(proposals, 0)})
			end
		end
		state
	end

	def run(state) do
		state = receive do
			{:prepare, b, leader, inst} -> handle_prepare(state, b, leader, inst)
			{:prepared, b, a_bal, a_val, inst} -> handle_prepared(state, b, a_bal, a_val, inst)
			{:leader_elect, p} -> handle_leader_elect(state, p)
			{:nack, b, inst} -> handle_nack(state, b, inst)
			{:broadcast_proposal, value, inst} -> handle_broadcast_proposal(state, value, inst)
			{:broadcast, pid, inst, value} -> handle_broadcast(state, pid, inst, value)
			{:check_instance, pid, inst, value} -> handle_check_instance(state, pid, inst, value)
			{:update_instances_map, inst_id, parent_pid, proposals} -> handle_update_instance_map(state, inst_id, parent_pid, proposals)
			{:received_decision, v, inst} -> handle_recieved_decision(state, v, inst)
			{:return_decision, pid, inst} -> handle_return_decision(state, pid, inst)
			{:accept, b, v, sender, inst} -> handle_accept(state, b, v, sender, inst)
			{:accepted, b, inst} -> handle_accepted(state, b, inst)
			{:abort, b, inst} -> handle_abort(state, b, inst)
		end
		run(state)
	end

	def propose(pid, inst, value, t) do
		Process.send_after(self(), {:timeout}, t)
		send(pid, {:check_instance, self(), inst, value})
		result = receive do
			{:decision, v} -> {:decision,v}
			{:abort} -> {:abort}
		after
			t -> {:timeout}
		end
	end

	def get_decision(pid, inst, t) do
		send(pid, {:return_decision, self(), inst})
		result = receive do
			{:return_decision, v } -> v
		after
			t -> nil
		end
	end

	defp unicast(p, m) when is_pid(p), do: send(p, m)
	defp unicast(p, m) do
		case :global.whereis_name(p) do
			pid when is_pid(pid) -> send(pid, m)
			:undefined -> :ok
		end
	end

	defp beb(dest, m), do: for p <- dest, do: unicast(p, m)

	defp compare_ballot({a1, a2}, operator, {b1, b2}) do
		log("Comparing ballots: #{inspect({a1, a2})} with #{inspect({b1, b2})}")
		diff = a1 - b1
		compare_result = if diff == 0 do
			if a2 == b2 do 0 else if a2 > b2 do 1 else -1 end end
		else
			diff
		end
		operator.(compare_result, 0)
	end

	defp increment_ballot_number(ballot_tuple, leader) do
  		{elem(ballot_tuple,0)+1, leader}
  	end
end

defmodule LeaderElector do

	def start(name, participants) do
		cname = name_concat(name, "_le_");
		participants = Enum.map(participants, fn x -> name_concat(x, "_le_") end);
		pid = spawn(LeaderElector, :init, [cname, participants, name]);
		register_name(cname, pid);
	end

	def init(name, participants, parent_name) do
		state = %{
			name: name,
			participants: participants,
			parent_name: parent_name,
			leader: nil,
			timeout: 1000,
			alive: %MapSet{},
		}
		Process.send_after(self(), {:timeout}, state.timeout)
		run(state)
	end

	defp handle_heartbeat_request(state, pid) do
		send(pid, {:heartbeat_reply, state.parent_name})
		state
	end

	defp handle_heartbeat_reply(state, name) do
		%{state | alive: MapSet.put(state.alive, name)}
	end

	defp handle_timeout(state) do
		beb(state.participants, {:heartbeat_request, self()})
		Process.send_after(self(), {:timeout}, state.timeout)
		state = elect_leader(state)
		%{state | alive: %MapSet{}}
	end

	def run(state) do
		state = receive do
			{:heartbeat_request, pid} ->
				handle_heartbeat_request(state, pid)
			{:heartbeat_reply, name} ->
				handle_heartbeat_reply(state, name)
			{:timeout} ->
				handle_timeout(state)
		end
		run(state)
	end

	def elect_leader(state) do
		case Enum.sort(state.alive) do
			[] -> state
			[leader | _] when leader != state.leader ->
				unicast(state.parent_name, {:leader_elect, leader})
				%{state | leader: leader}
			_ -> state
		end
	end

	defp unicast(p, m) when is_pid(p), do: send(p, m)
	defp unicast(p, m) do
		case :global.whereis_name(p) do
			pid when is_pid(pid) -> send(pid, m)
			:undefined -> :ok
		end
	end

	defp beb(dest, m), do: for p <- dest, do: unicast(p, m)

	defp name_concat(name, to_add), do: String.to_atom(Atom.to_string(name) <> to_add)

	defp register_name(name, pid, link \\ true) do
		case :global.re_register_name(name, pid) do
			:yes ->
				if link do
					Process.link(pid)
				end
				pid
			:no ->
				Process.exit(pid, :kill)
				:error
		end
	end
end

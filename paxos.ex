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
		_ld = LeaderDetector.start(name, participants)
		run(state)
	end

	defp handle_check_instance(state, pid, inst, value) do
		state = %{state | parent_name: pid, parent_pid: pid}
		case Map.get(state.decisions_map, inst) do
			nil ->
				beb(state.participants, {:update_instances_map, inst, pid, [value]})
				if state.inst == nil do
					send(self(), {:broadcast, pid, inst, value})
					%{state | inst: inst}
				else
					state
				end
			decision ->
				send(state.parent_name, {:decision, decision})
				state
		end
	end

	defp handle_update_instance_map(state, inst_id, parent_pid, proposals) do
		case Map.has_key?(state.instances, inst_id) do
			false ->
				new_instances = Map.put(state.instances, inst_id, {parent_pid, proposals})
				%{state | instances: new_instances}
			true ->
				state
		end
	end

	defp handle_return_decision(state, pid, inst) do
		inst_decision = Map.get(state.decisions_map, inst)
		send(pid, {:return_decision, inst_decision})
		state
	end

	defp handle_broadcast_proposal(state, value, inst) do
		state = %{state | proposal_h: value, inst: inst}
		if state.name == state.leader && !state.prepare_called do
			beb(state.participants, {:prepare, increment_ballot_number(state.bal, state.name), state.name, state.inst})
			%{state | prepare_called: true}
		else
			state
		end
	end

	defp handle_broadcast(state, pid, inst, value) do
		state = %{state | proposal: value, inst: inst}
		beb(state.participants, {:broadcast_proposal, value, inst})
		if(state.name == state.leader && state.prepare_called != true) do
			beb(state.participants, {:prepare, increment_ballot_number(state.bal, state.name),state.name, state.inst })
			state = %{state | prepare_called: true}
			state
		else
			state
		end
	end

	defp handle_leader_elect(state, p) do
		if(state.name == p) do
			if(state.proposal != nil || state.proposal_h != nil) do
				beb(state.participants, {:prepare, increment_ballot_number(state.bal, p),p, state.inst })
				state
			end
		end
		%{state | leader: p, bal: state.bal}
	end

	defp handle_prepare(state, b, leader, inst) do
		if state.inst == inst do
			if compare_ballot(b, &>/2, state.bal) do
				unicast(leader, {:prepared, b, state.a_bal, state.a_val, state.inst})
				%{state | bal: b, leader: leader, a_bal_max: nil, a_val_max: nil}
			else
				unicast(leader, {:nack, b, inst})
				state
			end
			state
		else
			state
		end
	end

	defp handle_prepared(state, b, a_bal, a_val, inst) do
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
		if inst == state.inst do
			if compare_ballot(b, &>=/2, state.bal) do
				unicast(sender, {:accepted, b, inst})
				%{state | bal: b, a_val: v, a_bal: b}
			else
				unicast(sender, {:nack, b, inst})
				state
			end
		else
			state
		end
	end

	defp handle_accepted(state, b, inst) do
		if inst == state.inst do
			state = %{state | accepted_quorum: state.accepted_quorum+1}
			state = if(state.leader == state.name) do
				if(state.accepted_quorum >= (floor(length(state.participants)/2) +1)) do # check if there is a quorum of accepted
					state=%{state | accepted_quorum: 0}
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
		if state.inst == inst  do
			if state.leader == state.name do
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
		if state.inst == inst do
			if state.parent_pid != nil do
				send(state.parent_pid, {:abort})
				beb(state.participants, {:abort, b, inst})
			end
		end
		state
	end

	defp handle_recieved_decision(state, v, inst) do
		state= %{state | decision: v, decisions_map: Map.put(state.decisions_map, inst, v), instances: Map.delete(state.instances, inst), inst: nil}
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
		if(foundInstance != nil) do
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
			{:decision, v} ->
				{:decision,v}
			{:abort} ->
				{:abort}
		after
			t -> {:timeout}
		end
	end

	def get_decision(pid, inst, t) do
		send(pid, {:return_decision, self(), inst})
		result = receive do
			{:return_decision, v } ->
					v
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

	defp compare_ballot({bal1, pid1}, operator, {bal2, pid2}) do
		case bal1 - bal2 do
			0 -> operator.(String.compare(Atom.to_string(pid1), Atom.to_string(pid2)), 0)
			diff -> operator.(diff, 0)
		end
	end

	defp increment_ballot_number(ballot_tuple, leader) do
		{elem(ballot_tuple,0)+1, leader}

	end
end

defmodule LeaderDetector do

	def start(name, participants) do
		cname = name_concat(name, "leader_election");
		participants = Enum.map(participants, fn x -> name_concat(x, "leader_election") end);
		pid = spawn(LeaderDetector, :init, [cname, participants, name]);
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

	def run(state) do
		state = receive do
			{:timeout} ->
				beb(state.participants, {:heartbeat_request, self()})
				Process.send_after(self(), {:timeout}, state.timeout)
				state = elect_leader(state)
				%{state | alive: %MapSet{}}
			{:heartbeat_request, pid} ->
				send(pid, {:heartbeat_reply, state.parent_name})
				state
			{:heartbeat_reply, name} ->
				%{state | alive: MapSet.put(state.alive, name)}

		end
		run(state)
	end

	def elect_leader(state) do
		sorted_processes = Enum.sort(state.alive)
		if(MapSet.size(state.alive) > 0) do
			first_process = Enum.at(sorted_processes, 0)
			if(first_process != state.leader) do
				unicast(state.parent_name, {:leader_elect, first_process})
				%{state | leader: first_process}
			else
				state
			end
		else
			state
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

	defp compare_ballot({bal1, pid1}, operator, {bal2, pid2}) do
		case bal1 - bal2 do
			0 -> operator.(String.compare(Atom.to_string(pid1), Atom.to_string(pid2)), 0)
			diff -> operator.(diff, 0)
		end
	end

	defp increment_ballot_number(ballot_tuple, leader) do
		{elem(ballot_tuple,0)+1, leader}
	end
end

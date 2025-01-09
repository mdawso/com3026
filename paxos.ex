# ----------------------------
# Paxos
# ----------------------------
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
			
			bal: {0, name}, #ballot number, process id
			a_bal: nil, # {bal_num, proc_id}
			a_val: nil, # could be any type
			a_bal_max: nil, # {bal_num, proc_id}
			a_val_max: nil,
			leader: nil, # id of the leader process
			value: nil, # decided value
			inst: nil, # instance
			parent_name: nil, # name of process that started the Paxos
			prepared_quorum: 0, # number of processes that have sent prepared
			accepted_quorum: 0, # number of processes that have sent accepted

			proposal: nil, # value that process proposed
			proposal_h: nil, # value that another process has proposed
			decision: nil, # value that was decided
			decisions_map: %{}, # map of instance -> decision

			prepare_called: false,

			instances: %{}, #instance -> {pid, [proposal 1, proposal 2]}
			parent_pid: nil,
		}
		_eld = EventualLeaderDetector.start(name, participants)
		run(state)
	end

	def run(state) do
		state = receive do
			{:check_instance, pid, inst, value} ->
				state = %{state | parent_name: pid, parent_pid: pid,}
				potentialDecision = Map.get(state.decisions_map, inst)
				if potentialDecision != nil do
					send(state.parent_name, {:decision, potentialDecision})
					state
				else
					Utils.beb(state.participants, {:update_instances_map, inst, pid, [value]})
					if(state.inst == nil) do
						send(self(), {:broadcast, pid, inst, value})
						%{state | inst: inst}
					else
						state
					end
				end
			{:update_instances_map, inst, pid, [proposals]} ->
				if(Map.has_key?(state.instances, inst)) do
					state
				else
					state = %{state | instances: Map.put(state.instances, inst, {pid, [proposals]})}
					state
				end
			{:return_decision, pid, inst} ->
				inst_decision = Map.get(state.decisions_map, inst)
				send(pid, {:return_decision, inst_decision })
				state
			{:broadcast_proposal, value, inst} ->
				state = %{state | proposal_h: value, inst: inst}
				if state.name == state.leader && state.prepare_called != true do
					Utils.beb(state.participants, {:prepare, Utils.increment_ballot_number(state.bal, state.name),state.name, state.inst })
					state = %{state | prepare_called: true}
					state
				end
				state
			{:broadcast, pid, inst, value} ->
				state = %{state | proposal: value, inst: inst}
				Utils.beb(state.participants, {:broadcast_proposal, value, inst})
				if(state.name == state.leader && state.prepare_called != true) do
					Utils.beb(state.participants, {:prepare, Utils.increment_ballot_number(state.bal, state.name),state.name, state.inst })
					state = %{state | prepare_called: true}
					state
				else
					state
				end
			{:leader_elect, p} ->
				if(state.name == p) do
					if(state.proposal != nil || state.proposal_h != nil) do
						Utils.beb(state.participants, {:prepare, Utils.increment_ballot_number(state.bal, p),p, state.inst })
						state
					end
				end
				%{state | leader: p, bal: state.bal}
			{:prepare, b, leader, inst} ->
				if state.inst == inst do
					if Utils.compare_ballot(b, &>/2, state.bal) do
						Utils.unicast(leader, {:prepared, b, state.a_bal, state.a_val, state.inst})
						%{state | bal: b, leader: leader, a_bal_max: nil, a_val_max: nil}
					else
						Utils.unicast(leader, {:nack, b, inst})
						state
					end
					state
				else
					state
				end
			{:prepared, b, a_bal, a_val, inst} ->
				if inst == state.inst do
					state = %{state | prepared_quorum: state.prepared_quorum+1}
					state= if a_bal == nil and a_val == nil do
						state
					else
						if state.a_bal_max == nil do
								%{state | a_bal_max: a_bal, a_val_max: a_val}
						else
							if a_bal > state.a_bal_max do
								%{state | a_bal_max: a_bal, a_val_max: a_val}
							else
								state
							end

						end
					end
					state = if(state.leader == state.name) do
						if(state.prepared_quorum >= (floor(length(state.participants)/2) +1)) do
							state = %{state | prepared_quorum: 0}
							state = if state.a_val_max == nil do
								if state.proposal != nil do
									v = state.proposal
									%{state | value: v}
								else
									v = state.proposal_h
									%{state | value: v}
								end
							else
								v = state.a_val_max
								%{state | value: v}
							end
							Utils.beb(state.participants, {:accept, b, state.value, state.name, inst})
							state
						else
							state
						end
					end
					state
				else
					state
				end

			{:accept, b, v, sender, inst} ->
				if inst == state.inst do
					if Utils.compare_ballot(b, &>=/2, state.bal) do
						Utils.unicast(sender, {:accepted, b, inst})
						%{state | bal: b, a_val: v, a_bal: b}
					else
						Utils.unicast(sender, {:nack, b, inst})
						state
					end
				else
					state
				end

			{:accepted, b, inst} ->
				if inst == state.inst do
					state = %{state | accepted_quorum: state.accepted_quorum+1}
					state = if(state.leader == state.name) do
						if(state.accepted_quorum >= (floor(length(state.participants)/2) +1)) do # check if there is a quorum of accepted
							state=%{state | accepted_quorum: 0}
							if state.parent_name != nil do
								send(state.parent_name, {:decision, state.a_val})
							end
							Utils.beb(state.participants, {:received_decision, state.a_val, inst})
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

			{:nack, b, inst} ->
				if state.inst == inst  do
					if state.leader == state.name do
						if state.parent_pid != nil do
							send(state.parent_pid, {:abort})
							Utils.beb(state.participants, {:abort, b, inst})
						end
							Utils.beb(state.participants, {:abort, b, inst})
					end
				end
				state
			{:abort, b, inst} ->
				if state.inst == inst do
					if state.parent_pid != nil do
						send(state.parent_pid, {:abort})
						Utils.beb(state.participants, {:abort, b, inst})
					end
				end
				state

			{:received_decision, v, inst} ->
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
end

defmodule Utils do
	def unicast(p, m) when is_pid(p), do: send(p, m)
	def unicast(p, m) do
		case :global.whereis_name(p) do
			pid when is_pid(pid) -> send(pid, m)
			:undefined -> :ok
		end
	end

	def beb(dest, m), do: for p <- dest, do: unicast(p, m)

	def add_to_name(name, to_add), do: String.to_atom(Atom.to_string(name) <> to_add)

	def register_name(name, pid, link \\ true) do # \\ is default value
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

	def compare_ballot({bal1, pid1}, operator, {bal2, pid2}) do
		case bal1 - bal2 do
			0 -> operator.(String.compare(Atom.to_string(pid1), Atom.to_string(pid2)), 0)
			diff -> operator.(diff, 0)
		end
	end

	def increment_ballot_number(ballot_tuple, leader) do
		{elem(ballot_tuple,0)+1, leader}

	end
end

defmodule EventualLeaderDetector do

	def start(name, participants) do
		# edit name to include leader_election - means it won't get mixed up with other processes
		new_name = Utils.add_to_name(name, "leader_election");

		# in participants, change all the names and then save to participants
		participants = Enum.map(participants, fn x -> Utils.add_to_name(x, "leader_election") end);

		# spawn leader election
		pid = spawn(EventualLeaderDetector, :init, [new_name, participants, name]); # atom name = pid

		# register name
		Utils.register_name(new_name, pid);

	end

	def init(name, participants, parent_name) do
		state = %{
			name: name,
			participants: participants,
			parent_name: parent_name, # process that owns leader elector
			leader: nil,
			timeout: 1000,
			alive: %MapSet{},
		}

		Process.send_after(self(), {:timeout}, state.timeout) # start timeout

		run(state)
	end

	def run(state) do

		state = receive do
			{:timeout} ->
				# send heartbeat request to all processes
				Utils.beb(state.participants, {:heartbeat_request, self()})

				# start restart timeout if timeout is reached
				Process.send_after(self(), {:timeout}, state.timeout)

				# elect leader from alive
				state = elect_leader(state)

				# clear alive - this is so that a process that has crashed is not stuck in alive
				%{state | alive: %MapSet{}}

			{:heartbeat_request, pid} ->
				# send heartbeat
				send(pid, {:heartbeat_reply, state.parent_name})

				state

			{:heartbeat_reply, name} ->
				#receive heartbeat reply and add process to alive
				%{state | alive: MapSet.put(state.alive, name)}

		end
		run(state)
	end

	def elect_leader(state) do
		# order alive from smallest to largest pid
		sorted_processes = Enum.sort(state.alive) # returns a list of the sorted processes

		# check if there are any processes are alive
		if(MapSet.size(state.alive) > 0) do
			first_process = Enum.at(sorted_processes, 0)

			# if process is not already leader, then make it leader
			if(first_process != state.leader) do
				Utils.unicast(state.parent_name, {:leader_elect, first_process})
				%{state | leader: first_process}
				# if process is already leader, then don't change the state
			else
				state
			end
		else
			state
		end
	end
end

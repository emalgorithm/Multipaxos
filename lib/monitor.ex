# Tencho Tenev (tt1215) and Emanuele Rossi (er1115)

# coursework 2, paxos made moderately complex

defmodule Monitor do

def start config do
  Process.send_after self(), :print, config.print_after
  next config, 0, Map.new, Map.new, Map.new, Map.new, Map.new, Map.new
end # start

defp next config, clock, requests, updates, transactions, decisions, replica_decisions, replica_proposals do
  receive do
  { :db_update, db, seqnum, transaction } ->
    { :move, amount, from, to } = transaction

    done = Map.get updates, db, 0

    if seqnum != done + 1  do
      IO.puts "  ** error db #{db}: seq #{seqnum} expecting #{done+1}"
      System.halt
    end

    transactions =
      case Map.get transactions, seqnum do
      nil ->
        # IO.puts "db #{db} seq #{seqnum} #{done}"
        Map.put transactions, seqnum, %{ amount: amount, from: from, to: to }

      t -> # already logged - check transaction
        if amount != t.amount or from != t.from or to != t.to do
	  IO.puts " ** error db #{db}.#{done} [#{amount},#{from},#{to}] " <>
            "= log #{done}/#{Map.size transactions} [#{t.amount},#{t.from},#{t.to}]"
          System.halt
        end
        transactions
      end # case

    updates = Map.put updates, db, seqnum
    next config, clock, requests, updates, transactions, decisions, replica_decisions, replica_proposals

  { :client_request, server_num } ->  # requests by replica
    seen = Map.get requests, server_num, 0
    requests = Map.put requests, server_num, seen + 1
    next config, clock, requests, updates, transactions, decisions, replica_decisions, replica_proposals

  { :commander_decision, {ballot_num, slot_num, req} } -> # decision by commander
    if Map.has_key?(decisions, req) and decisions[req] != slot_num do
        IO.puts " ** error duplicate decision of #{inspect req} " <>
                "for slots #{slot_num} and #{decisions[req]}"
        System.halt
    end
    decisions = Map.put decisions, req, slot_num
    next config, clock, requests, updates, transactions, decisions, replica_decisions, replica_proposals

  { :replica_decision, server_num, cmd, proposals } -> # replica sees a decision for its own proposal
    if Map.has_key?(replica_decisions, server_num) and Enum.member?(replica_decisions[server_num], cmd) do
      IO.puts " ** error duplicate decision of #{inspect cmd} " <>
              " in replica #{server_num}"
    end
    replica_decisions = Map.update replica_decisions, server_num, [cmd], &([cmd | &1])
    replica_proposals = Map.put replica_proposals, server_num, proposals
    next config, clock, requests, updates, transactions, decisions, replica_decisions, replica_proposals

  :print ->
    clock = clock + config.print_after
    sorted = updates |> Map.to_list |> List.keysort(0)
    IO.puts "time = #{clock}  updates done = #{inspect sorted}"
    sorted = requests |> Map.to_list |> List.keysort(0)
    IO.puts "time = #{clock} requests seen = #{inspect sorted}"
    sorted = replica_decisions |> Map.to_list |> List.keysort(0) |> Enum.map(fn({r,dcs}) -> {r,length(dcs)} end)
    IO.puts "time = #{clock} decided proposals p. replica = #{inspect sorted}"
    # sorted = replica_proposals |> Map.to_list |> List.keysort(0)
    # IO.puts "time = #{clock} replica proposals = #{inspect sorted}"
    IO.puts ""
    Process.send_after self(), :print, config.print_after
    next config, clock, requests, updates, transactions, decisions, replica_decisions, replica_proposals

  # ** ADD ADDITIONAL MESSAGES HERE

  _ ->
    IO.puts "monitor: unexpected message"
    System.halt
  end # receive
end # next

end # Monitor

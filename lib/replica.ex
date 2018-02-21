# Tencho Tenev (tt1215) and Emanuele Rossi (er1115)

defmodule Replica do
  def start config, database, monitor do
    receive do
      {:bind, leaders} ->
        eager_next {Map.new, 0}, 0, [], Map.new, leaders, database, monitor, config
    end
  end

  defp propose {proposals, slot_in}, cmd, leaders do
    # This check is just for debugging / testing purposes
    if Enum.member?(Map.values(proposals), cmd) do
      IO.puts " ** error duplicate proposal for #{inspect cmd} at slot #{slot_in}"
    end
    for l <- leaders, do: send l, {:propose, slot_in, cmd}
    {(Map.put proposals, slot_in, cmd), slot_in + 1}
  end

  defp process_decisions decisions, slot_out, database do
    if decisions[slot_out] do
      {_client, _cmdid, transaction} = decisions[slot_out]
      send database, {:execute, transaction}
      decisions = Map.delete decisions, slot_out
      process_decisions decisions, slot_out + 1, database
    else
      {decisions, slot_out}
    end
  end

  defp process_requests({proposals, slot_in}, [cmd | rs], slot_out, window, leaders)
    when slot_in < slot_out + window do
      {proposals, slot_in}
      |> propose(cmd, leaders)
      |> process_requests(rs, slot_out, window, leaders)
  end

  defp process_requests({proposals, slot_in}, requests, slot_out, window, leaders) do
    {proposals, slot_in, requests}
  end

  defp eager_next {proposals, slot_in}, slot_out, requests, decisions, leaders, database, monitor, config do
    receive do
      {:client_request, cmd} ->
        send monitor, {:client_request, config[:server_num]}

        if slot_in < slot_out + config[:window] do
          {proposals, slot_in}
          |> propose(cmd, leaders)
          |> eager_next(slot_out, requests, decisions, leaders, database, monitor, config)
        else
          eager_next({proposals, slot_in}, slot_out, [cmd | requests], decisions, leaders, database, monitor, config)
        end
      {:decision, slot_num, decided_cmd} ->
        decisions = Map.put decisions, slot_num, decided_cmd

        if proposals[slot_num] == decided_cmd and slot_num >= slot_out do
          send monitor, {:replica_decision, config[:server_num], decided_cmd, proposals}
        end

        # Try to commit decisions on top of application state
        # If a slot becomes available also try to place proposals for pending request
        {proposals, slot_in, slot_out, requests, decisions} = if slot_num == slot_out do
          {decisions, slot_out} = process_decisions decisions, slot_out, database
          slot_in = max(slot_in, slot_out)
          {proposals, slot_in, requests} = process_requests {proposals, slot_in}, requests, slot_out, config[:window], leaders
          {proposals, slot_in, slot_out, requests, decisions}
        else
          {proposals, max(slot_in, slot_num), slot_out, requests, decisions}
        end

        # In case of a failed proposal:
        #   - retry if a slot is available
        #   - otherwise add failed proposal to requests
        # Handles a decision that:
        #  - there is no proposal for this slot
        #  - matches a proposal by this replica
        #  - causes a proposal to be retried
        if !(Map.has_key? proposals, slot_num) or proposals[slot_num] == decided_cmd do
          proposals = Map.delete(proposals, slot_num)
          eager_next({proposals, slot_in}, slot_out, requests, decisions, leaders, database, monitor, config)
        else
          if slot_in < slot_out + config[:window] do
            cmd = proposals[slot_num]
            {Map.delete(proposals, slot_num), slot_in}
            |> propose(cmd, leaders)
            |> eager_next(slot_out, requests, decisions, leaders, database, monitor, config)
          else
            cmd = proposals[slot_num]
            eager_next({Map.delete(proposals, slot_num), slot_in}, slot_out, [cmd | requests], decisions, leaders, database, monitor, config)
          end
        end
    end
  end
end

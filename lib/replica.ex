defmodule Replica do
  def start config, database, monitor do
    receive do
      {:bind, leaders} ->
        eager_next {Map.new, 0}, 0, Map.new, leaders, database, monitor, config
    end
  end

  # This is an unconditional propose used for client requests
  # Note slot_in = slot_num
  defp client_propose {proposals, slot_in}, cmd, leaders do
    for l <- leaders, do: send l, {:propose, slot_in, cmd}
    {(Map.put proposals, slot_in, cmd), slot_in + 1}
  end

  # Handles a decision that:
  #  - there is no proposal for this slot
  #  - matches a proposal by this replica
  #  - causes a proposal to be retried
  defp retry_propose {proposals, slot_in}, slot_num, cmd, leaders do
    if !(Map.has_key? proposals, slot_num) or proposals[slot_num] == cmd do
      {proposals, slot_in}
    else
      for l <- leaders, do: send l, {:propose, slot_in, proposals[slot_num]}
      proposals
      |> Map.put(slot_in, proposals[slot_num])
      |> Map.put(slot_num, cmd)
      |> (fn(p) -> {p, slot_in + 1} end).()
    end
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

  # Decisions are also stored in proposals but in the form {:decided, cmd}
  # There is no need to keep a set of pending requests as they get eagerly
  # placed in proposals. This wouldn't work if a limit on the pending proposals
  # is imposed such as when replicas need to handle reconfiguration messages
  defp eager_next {proposals, slot_in}, slot_out, decisions, leaders, database, monitor, config do
    receive do
      {:client_request, cmd} ->
        send monitor, {:client_request, config[:server_num]}
        {proposals, slot_in}
        |> client_propose(cmd, leaders)
        |> eager_next(slot_out, decisions, leaders, database, monitor, config)
      {:decision, slot_num, decided_cmd} ->
        decisions = Map.put decisions, slot_num, decided_cmd

        {decisions, slot_out} = if slot_num == slot_out do
          process_decisions decisions, slot_out, database
        else
          {decisions, slot_out}
        end

        {proposals, slot_in}
        |> retry_propose(slot_num, decided_cmd, leaders)
        |> eager_next(slot_out, decisions, leaders, database, monitor, config)
    end
  end

  # Lazy implementation (incomplete)
  defp propose leaders, requests, decisions, proposals, slot_in do
    if Map.has_key? decisions, slot_in do
      propose leaders, requests, decisions, proposals, slot_in + 1
    else
      case Enum.fetch requests, 0 do
        {:error} -> next leaders, requests, decisions
        {:ok, cmd} ->
          for l <- leaders, do: send l, {:propose, slot_in, cmd}
          propose leaders, (MapSet.delete requests, cmd), decisions,  (Map.put proposals, slot_in, cmd), slot_in + 1
      end
    end
  end

  defp next leaders, requests, decisions, slot_in do
    receive do
      {:client_request, cmd} ->
        next leaders, (MapSet.put requests, cmd), decisions
      {:decision, slot_num, cmd} ->
        decisions = Map.put decisions, slot_num, cmd
    end
  end
end

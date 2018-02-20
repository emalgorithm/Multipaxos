defmodule Acceptor do
  def start _config do
    # The ballot number { integer, leader } must follow total ordering
    next {0, 0}, Map.new, 0
  end

  # The use of Map.put maintains the latest pvalue for each slot
  # which reduces the acceptor state
  defp put_if_true(condition, set, key, elem) do
    if condition, do: (Map.put set, key, elem), else: set
  end

  defp next ballot_num, accepted, highest_safe_slot do
    receive do
      {:prepare, leader, leader_ballot_num} ->
        ballot_num = max ballot_num, leader_ballot_num
        accepted_set = MapSet.new(Map.values(accepted))
        send leader, {:promise, ballot_num, {accepted_set, highest_safe_slot}}
        next ballot_num, accepted, highest_safe_slot
      {:accept, leader, {leader_ballot_num, slot_num, _cmd}=pvalue} ->
        accepted = put_if_true ballot_num == leader_ballot_num, accepted, slot_num, pvalue
        send leader, {:accepted, ballot_num, {}}
        next ballot_num, accepted, highest_safe_slot
      # {:decision, pvalue} -> next(ballot_num, MapSet.delete(accepted, pvalue))
      {:update_slot_out, new_highest_safe_slot} -> 
        accepted = (for {slot_num, pvalue} <- accepted, slot_num >= highest_safe_slot, do: {slot_num, pvalue}) |> Map.new 
        next ballot_num, accepted, max(highest_safe_slot, new_highest_safe_slot)
    end
  end
end

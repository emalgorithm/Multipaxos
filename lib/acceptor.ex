defmodule Acceptor do
  def start _config do
    # The ballot number { integer, leader } must follow total ordering
    next {0, 0}, Map.new
  end

  # The use of Map.put maintains the latest pvalue for each slot
  # which reduces the acceptor state
  defp put_if_true(condition, set, key, elem) do
    if condition, do: (Map.put set, key, elem), else: set
  end

  defp next ballot_num, accepted do
    receive do
      {:prepare, leader, leader_ballot_num} ->
        ballot_num = max ballot_num, leader_ballot_num
        accepted_set = MapSet.new(Map.values(accepted))
        send leader, {:promise, ballot_num, accepted_set}
        next ballot_num, accepted
      {:accept, leader, {leader_ballot_num, slot_num, _cmd}=pvalue} ->
        accepted = put_if_true ballot_num == leader_ballot_num, accepted, slot_num, pvalue
        send leader, {:accepted, ballot_num, {}}
        next ballot_num, accepted
      # {:decision, pvalue} -> next(ballot_num, MapSet.delete(accepted, pvalue))
    end
  end
end

# Tencho Tenev (tt1215) and Emanuele Rossi (er1115)
defmodule Acceptor do
  def start _config do
    # The ballot number { integer, leader } must follow total ordering
    next {0, 0}, Map.new
  end

  defp put_if_true(condition, map, key, elem) do
    if condition, do: Map.put(map, key, elem), else: map
  end

  defp next ballot_num, accepted do
    receive do
      {:prepare, leader, leader_ballot_num} ->
        ballot_num = max ballot_num, leader_ballot_num
	accepted_set = accepted |> Map.values |> MapSet.new
        send leader, {:promise, ballot_num, accepted_set}
        next ballot_num, accepted
      {:accept, leader, {leader_ballot_num, slot_num, _cmd}=pvalue} ->
        accepted = put_if_true ballot_num == leader_ballot_num, accepted, slot_num, pvalue
        send leader, {:accepted, ballot_num, {}}
        next ballot_num, accepted
    end
  end
end

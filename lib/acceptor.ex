defmodule Acceptor do
  def start _config do
    # The ballot number { integer, leader } must follow total ordering
    next {0, 0}, MapSet.new
  end

  defp put_if_true(condition, set, elem) do
    if condition, do: (MapSet.put set, elem), else: set
  end

  defp next ballot_num, accepted do
    receive do
      {:prepare, leader, leader_ballot_num} ->
        ballot_num = max ballot_num, leader_ballot_num
        send leader, {:promise, ballot_num, accepted}
        next ballot_num, accepted
      {:accept, leader, {leader_ballot_num, _slot_num, _cmd}=pvalue} ->
        accepted = put_if_true ballot_num == leader_ballot_num, accepted, pvalue
        send leader, {:accepted, ballot_num, {}}
        next ballot_num, accepted
    end
  end
end

defmodule Commander do
  def start leader, acceptors, replicas, {ballot_num, slot_num, cmd}=pvalue, monitor do
    for a <- acceptors, do: send a, {:accept, self(), pvalue}

    # Ensure accepted with the right ballot number for a majority of acceptors
    Leader.collect_majority (length acceptors), :accepted, leader, ballot_num

    # At this point majority of acceptors have accepted the ballot
    for r <- replicas, do: send r, {:decision, slot_num, cmd}

    send monitor, {:commander_decision, pvalue}
  end
end

defmodule Scout do
  def start leader, acceptors, ballot_num do
    for a <- acceptors, do: send a, {:prepare, self(), ballot_num}

    # Ensure promise with the right ballot number for a majority of acceptors
    all_accepted = Leader.collect_majority (length acceptors), :promise, leader, ballot_num

    accepted = Enum.reduce all_accepted, fn(a, acc) -> MapSet.union a, acc end
    send leader, {:adopted, ballot_num, accepted}
  end
end

defmodule Leader do
  def start _config, monitor do
    receive do
      {:bind, acceptors, replicas} ->
        init_ballot_num = {0, self()}
        spawn Scout, :start, [self(), acceptors, init_ballot_num]
        next acceptors, replicas, init_ballot_num, false, Map.new, monitor
    end
  end

  def next acceptors, replicas, ballot_num, active, proposals, monitor do
    receive do
      {:propose, slot_num, cmd} ->
        if active and !(Map.has_key? proposals, slot_num) do
          pvalue = {ballot_num, slot_num, cmd}
          spawn Commander, :start, [self(), acceptors, replicas, pvalue, monitor]
        end

        proposals = Map.put_new proposals, slot_num, cmd
        next acceptors, replicas, ballot_num, active, proposals, monitor
      {:adopted, ^ballot_num, pvalues} ->
        proposals = Map.merge proposals, (pmax pvalues)
        for {slot_num, cmd} <- proposals do
          pvalue = {ballot_num, slot_num, cmd}
          spawn Commander, :start, [self(), acceptors, replicas, pvalue, monitor]
        end
        next acceptors, replicas, ballot_num, true, proposals, monitor
      {:preempted, {round, _leader}=preempt} when preempt > ballot_num ->
        ballot_num = {round + 1, self()}
        spawn Scout, :start, [self(), acceptors, ballot_num]
        next acceptors, replicas, ballot_num, false, proposals, monitor
    end
  end

  # For each slot find the largest ballot
  # Returns map of slot, cmd pairs suitable for merging into proposals
  defp pmax(pvalues) when pvalues == %MapSet{}, do: Map.new

  defp pmax pvalues do
    slot_to_pvalues = pvalues |> MapSet.to_list |> Enum.group_by(fn {_, s, _} -> s end)

    for {slot, pvalues} <- slot_to_pvalues do
      {_, _, cmd} = Enum.sort(pvalues, fn (a, b) -> a >= b end) |> hd
      {slot, cmd}
    end
    |> Map.new
  end

  def collect_majority count, msg_receive, leader, leader_ballot_num do
    for _ <- 0..(div count, 2) do
      receive do
        {^msg_receive, ballot_num, data} when leader_ballot_num == ballot_num ->
          data
        {^msg_receive, ballot_num, _data} ->
          send leader, {:preempted, ballot_num}
          Process.exit self(), :kill
      end
    end
  end
end

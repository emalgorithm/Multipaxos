defmodule Commander do
  def start leader, acceptors, replicas, {ballot_num, slot_num, cmd}=pvalue, monitor do
    for a <- acceptors, do: send a, {:accept, self(), pvalue}

    # Ensure accepted with the right ballot number for a majority of acceptors
    Leader.collect_majority (length acceptors), :accepted, leader, ballot_num

    IO.puts("#{inspect cmd}           Decision: #{inspect pvalue}")
    # At this point majority of acceptors have accepted the ballot
    for r <- replicas, do: send r, {:decision, slot_num, cmd}

    IO.puts "Decision: #{inspect pvalue}"
    send monitor, {:commander_decision, pvalue}
  end
end

defmodule Scout do
  def start leader, acceptors, ballot_num do
    # IO.puts "Scout spawned"
    for a <- acceptors, do: send a, {:prepare, self(), ballot_num}

    # Ensure promise with the right ballot number for a majority of acceptors
    {all_accepted, highest_safe_slots} = Leader.collect_majority((length acceptors), :promise, leader, ballot_num) |> Enum.unzip
    highest_safe_slot = Enum.max highest_safe_slots

    accepted = Enum.reduce all_accepted, fn(a, acc) -> MapSet.union a, acc end
    # IO.puts "Leader #{inspect leader} Accepted pvalues are #{inspect accepted} for ballot #{inspect ballot_num} highest_safe_slot is #{inspect highest_safe_slot}"
    # if (elem ballot_num, 0) < 100, do: IO.puts "Scout for #{inspect leader} adopted ballot #{inspect ballot_num}"
    send leader, {:adopted, ballot_num, accepted, highest_safe_slot}
  end
end

defmodule Leader do
  def start config, monitor do
    receive do
      {:bind, acceptors, replicas} ->
        init_ballot_num = {0, self()}
        spawn Scout, :start, [self(), acceptors, init_ballot_num]
        replicas_to_slots = Map.new(replicas, fn replica -> {replica, 0} end) 
        next acceptors, replicas, init_ballot_num, false, Map.new, replicas_to_slots, config[:n_servers], monitor
    end
  end

  def next acceptors, replicas, ballot_num, active, proposals, replicas_to_slots, n_server, monitor do
    receive do
      {:propose, slot_num, cmd} ->
        if active and !(Map.has_key? proposals, slot_num) do
          pvalue = {ballot_num, slot_num, cmd}
          spawn Commander, :start, [self(), acceptors, replicas, pvalue, monitor]
        end

        proposals = Map.put_new proposals, slot_num, cmd
        next acceptors, replicas, ballot_num, active, proposals, replicas_to_slots, n_server, monitor
      {:adopted, ^ballot_num, pvalues, highest_safe_slot} ->
        proposals = (for {slot_num, cmd} <- proposals, slot_num >= highest_safe_slot, do: {slot_num, cmd}) |> Map.new |> Map.merge((pmax pvalues))
        # IO.puts("Proposals size: #{Kernel.map_size(proposals)}")
        for {slot_num, cmd} <- proposals do
          # IO.puts("different Spawing commander for slot num #{slot_num}")
          pvalue = {ballot_num, slot_num, cmd}
          spawn Commander, :start, [self(), acceptors, replicas, pvalue, monitor]
        end
        next acceptors, replicas, ballot_num, true, proposals, replicas_to_slots, n_server, monitor
      {:preempted, {round, _leader}=preempt} when preempt > ballot_num ->
        ballot_num = {round + 1, self()}
        spawn Scout, :start, [self(), acceptors, ballot_num]
        next acceptors, replicas, ballot_num, false, proposals, replicas_to_slots, n_server, monitor
      {:update_slot_out, replica, slot_out} ->
        replicas_to_slots = replicas_to_slots |> Map.put(replica, slot_out)
        # IO.puts("#{inspect replicas_to_slots}")
        # IO.puts("n_server: #{n_server}")
        highest_safe_slot = replicas_to_slots |> Map.values |> Enum.sort(&(&1 >= &2)) |> Enum.at(div(n_server, 2))
        # IO.puts("leader #{inspect self()} is at highest_safe_slot update is #{inspect highest_safe_slot}")
        proposals = (for {slot_num, cmd} <- proposals, slot_num >= highest_safe_slot, do: {slot_num, cmd}) |> Map.new
        for acceptor <- acceptors do
          send acceptor, {:update_slot_out, highest_safe_slot} 
        end
        next acceptors, replicas, ballot_num, active, proposals, replicas_to_slots, n_server, monitor
    end
  end

  # For each slot find the largest ballot
  # Returns map of slot, cmd pairs suitable for merging into proposals
  defp pmax(pvalues) when pvalues == %MapSet{}, do: Map.new

  defp pmax pvalues do
    sorted_pvalues = pvalues |> MapSet.to_list |> Enum.sort
    {max_ballot_num, _, _} = hd sorted_pvalues

    Enum.split_with(sorted_pvalues, (gt_ballot max_ballot_num))
    |> elem(0)
    |> Map.new(fn({_, slot_num, cmd}) -> {slot_num, cmd} end)
  end

  defp gt_ballot max_ballot_num do
    fn {ballot_num, _, _} -> ballot_num >= max_ballot_num
    end
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

defmodule CicadaBus.BrokerTest do
  use ExUnit.Case

  alias CicadaBus.Event
  alias CicadaBus
  alias __MODULE__.Broker


  defmodule Broker do
    use CicadaBus.Handler

    # this makes a queue but for each
    deftopic "match/short/topic"
    deftopic "match/wildcard/suffix/**"
    deftopic "match/wildcard/*/path"
    deftopic "match/wildcard/**/tail"


    defhandle "inline/**", ev, opts do
      case ev.value do
        {pid, ref} ->
          send pid, {ref, :ok, 1}

        _ ->
          nil
      end
    end


    defhandle "inline/**", %{value: {pid, ref}} = ev, _opts do
      {pid, ref} = ev.value
      send pid, {ref, :ok, 2}
    end


    defhandle "inline/**", %{meta: %{guarantee: :drop}} = ev, _opts do
      {pid, ref} = ev.value
      send pid, {ref, :should_fail, 3}
    end
  end


  # Check matching. The explicit call to Broker.handle/1 means this is
  # handeled outside of normal queue handling
  test "topic matching", opts do
    {:ok, broker} = Broker.start_link("match/**")

    # Explicit
    assert :ok = Broker.input(Event.new("match/short/topic", opts[:test]), broker, sync: true)

    # wildcard suffix
    assert :ok = Broker.input(Event.new("match/wildcard/suffix/should/match", opts[:test]), broker, sync: true)

    # single wildcard path
    assert :ok = Broker.input(Event.new("match/wildcard/single/path", opts[:test]), broker, sync: true)

    # wildcard tail match
    assert :ok = Broker.input(Event.new("match/wildcard/path/with/tail", opts[:test]), broker, sync: true)

    # Matches even though there's no subscribers
    assert :ok = Broker.input(Event.new("match/no-subscribers", opts[:test]), broker, sync: true)

    # Dropped since the event is not accepted
    assert :drop = Broker.input(Event.new("unmatched/ev", opts[:test]), broker, sync: true)
  end


  test "get topic" do
    topic = "**"
    {:ok, pid} = Broker.start_link(topic)

    assert {:ok, "**"} == CicadaBus.topic(pid)
  end


  test "subscribe -> unsubscribe" do
    topic = "**"
    {:ok, pid} = Broker.start_link(topic)

    {:ok, state} = Broker.subscribe(pid)
    :ok = Broker.unsubscribe(state, pid)
    assert {:error, :no_subscription} == Broker.unsubscribe(state, pid)
  end


  test "auto-unsubscribe on failure" do
    topic = "**"
    {:ok, pid} = Broker.start_link(topic)

    {parent, ref} = {self(), make_ref()}

    {child, monref} = spawn_monitor(fn ->
      {:ok, state} = Broker.subscribe(pid)
      send parent, {ref, :init, state}
      Process.hibernate(:timer, :sleep, [10])
    end)

    assert_receive {^ref, :init, state}
    send child, :continue
    assert_receive {:DOWN, ^monref, :process, ^child, :normal}
    assert {:error, :no_subscription} == Broker.unsubscribe(state, pid)
  end


  test "root level subscription" do
    {:ok, broker} = Broker.start_link("**")
    {:ok, ref} = Broker.subscribe(broker)

    :ok = Broker.input(ev = Event.new("enqueue", :value), broker)

    assert_receive {^ref, {:event, _ackref, ^ev}}
  end


  test "inline handler" do
    {:ok, broker} = Broker.start_link("**")
    :ok = Broker.input(ev = Event.new("inline/event", {self(), ref = make_ref()}), broker)

    assert_receive {^ref, :ok, 1}
    assert_receive {^ref, :ok, 2}
    refute_receive {^ref, :should_fail, 3}
  end
end

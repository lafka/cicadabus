defmodule CicadaBus.HandlerTest do
  use ExUnit.Case

  alias CicadaBus.Handler
  alias CicadaBus.Event


  test "minimal handler" do
    # A basic handler with subscribe/unsubscribe.
    # This accept
    {:ok, pid} = Handler.start_link("**")
    assert {:ok, state} = Handler.subscribe(pid)

    :ok = Handler.input(ev = Event.new("test/topic", :value), pid)

    assert_receive {^state, {:event, _ackref, ^ev}}

    assert :ok = Handler.unsubscribe state, pid

    :ok = Handler.input(Event.new("test/topic", :value), pid)

    refute_receive _
  end


  test "reject unmatched input" do
    {:ok, pid} = Handler.start_link("only/**")
    assert {:ok, _state} = Handler.subscribe(pid)
    assert :drop = Handler.input(Event.new("unmatched/topic", :value), pid, sync: true)
    refute_receive _
  end


  test "chain handlers" do
    # This setup can be used along with registered name and Supervisor
    # to create a processing pipeline based on events
    #
    {:ok, root} = Handler.start_link("root/**")
    {:ok, branch} = Handler.start_link("**/branch/**")
    {:ok, leaf} = Handler.start_link("**/branch/leaf")

    {:ok, _} = Handler.attach(root, branch)
    {:ok, _} = Handler.attach(branch, leaf)

    assert {:ok, state} = Handler.subscribe(leaf)

    assert :ok = Handler.input(ev = Event.new("root/branch/leaf", :value), root)

    assert_receive {^state, {:event, _ackref, ^ev}}
  end


  test "deliver upon subscription" do
    # In case there's a event with some delivery guarantee it will be
    # delivered once the first subscriber connects
    {:ok, pid} = Handler.start_link("**")
    :ok = Handler.input(ev = Event.new("enqueue", :value, meta: %{guarantee: :any}), pid)

    refute_receive _

    {:ok, ref} = Handler.subscribe(pid)
    assert_receive {^ref, {:event, _ackref, ^ev}}
  end


  test "acknowledgement" do
    # Test that a given handler does not skip events with acknowledgement flag set.

    # In this case any subscriber may send the acknowledgement.
    # Since guarantee is set to any in handler this overwrites any
    # nil values for all events
    {:ok, pid} = Handler.start_link("**", guarantee: :any, acknowledge: true)


    :ok = Handler.input(Event.new("ack/queue", v1 = 1), pid)
    :ok = Handler.input(Event.new("ack/queue", v2 = 2), pid)
    :ok = Handler.input(Event.new("ack/queue", v3 = 3), pid)
    :ok = Handler.input(Event.new("ack/queue", v4 = 4), pid)

    {:ok, s} = Handler.subscribe(pid)

    assert_receive {^s, {:event, ackref,  %{value: ^v1}}}, 0
    refute_receive {^s, {:event, _ackref, _ev}}, 0

    :ok = Handler.ack(s, ackref, pid)
    assert_receive {^s, {:event, ackref,  %{value: ^v2}}}, 10
    refute_receive {^s, {:event, _ackref, _ev}}, 0

    :ok = Handler.ack(s, ackref, pid)
    assert_receive {^s, {:event, ackref,  %{value: ^v3}}}, 10
    refute_receive {^s, {:event, _ackref, _ev}}, 0

    :ok = Handler.ack(s, ackref, pid)
    assert_receive {^s, {:event, _ackref,  %{value: ^v4}}}, 10
    refute_receive {^s, {:event, _ackref, _ev}}, 0

    refute_receive _
  end

  defmodule ExtraHandler do
    use CicadaBus.Handler

    defhandle "**", %{value: {pid, ref}}, opts do
      send pid, {ref, opts}
    end
  end

  test "pass options to handler" do
    {:ok, pid} = ExtraHandler.start_link("**", additional: true)
    :ok = ExtraHandler.input(Event.new("test", {self(), ref = make_ref()}), pid)

    assert_receive {^ref, [additional: true]}
  end
end

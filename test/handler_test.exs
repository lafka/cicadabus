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
end

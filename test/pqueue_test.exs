defmodule Tnest.Edge.Bus.PqueueTest do
  use ExUnit.Case

  # Simple queue functionality, all on same priority
  test "member" do
    queue = Enum.reduce(1..10, :pqueue.new(), fn v, acc -> :pqueue.in(v, acc) end)

    assert :pqueue.member(1, queue)
    assert :pqueue.member(2, queue)
    refute :pqueue.member(100, queue)
    refute :pqueue.member(1, :pqueue.new())

    Enum.reduce(1..10, queue, fn(v, acc) ->
      {{:value, n}, q} = :pqueue.out(acc)
      assert v == n
      q
    end)
  end

  test "member with priority" do
    queue = Enum.reduce(0..99, :pqueue.new(), fn v, acc -> :pqueue.in(v, rem(v, 10), acc) end)
    assert :pqueue.member(1, queue)
    assert :pqueue.member(50, queue)
    refute :pqueue.member(200, queue)

    q =
    Enum.reduce(0..9, queue, fn(v, q) ->
      x = v
      assert {{:value, ^x, ^v}, q} = :pqueue.out_p(q)
      x = x + 10

      assert {{:value, ^x, ^v}, q} = :pqueue.out_p(q)
      x = x + 10

      assert {{:value, ^x, ^v}, q} = :pqueue.out_p(q)
      x = x + 10

      assert {{:value, ^x, ^v}, q} = :pqueue.out_p(q)
      x = x + 10

      assert {{:value, ^x, ^v}, q} = :pqueue.out_p(q)
      x = x + 10

      assert {{:value, ^x, ^v}, q} = :pqueue.out_p(q)
      x = x + 10

      assert {{:value, ^x, ^v}, q} = :pqueue.out_p(q)
      x = x + 10

      assert {{:value, ^x, ^v}, q} = :pqueue.out_p(q)
      x = x + 10

      assert {{:value, ^x, ^v}, q} = :pqueue.out_p(q)
      x = x + 10

      assert {{:value, ^x, ^v}, q} = :pqueue.out_p(q)
      q
    end)

    assert :pqueue.is_empty(q)
  end

  test "peek" do
    queue = Enum.reduce(1..10, :pqueue.new(), fn v, acc -> :pqueue.in(v, v, acc) end)

    empty =
      Enum.reduce(1..10, queue, fn(v, acc) ->
        assert {:value, ^v} = :pqueue.peek(acc)
        {{:value, ^v}, q} = :pqueue.out(acc)
        q
      end)

    assert :empty == :pqueue.peek(empty)
  end
end

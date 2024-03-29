defmodule CicadaBus.NestedTest do
  use ExUnit.Case

  require Logger

  alias __MODULE__.Root
  alias CicadaBus.Event

  defmodule Root do
    use CicadaBus.Handler
    deftopic "fst/**", to: CicadaBus.NestedTest.First
  end

  defmodule First do
    use CicadaBus.Handler
    deftopic "*/snd/**", to: CicadaBus.NestedTest.Second
  end

  defmodule Second do
    use CicadaBus.Handler
    deftopic "*/*/third/**", to: CicadaBus.NestedTest.Third
  end

  defmodule Third do
    use CicadaBus.Handler

    defhandle "**/final", %{value: {pid, ref}} do
       send pid, {ref, :done}
    end
  end

  test "nested" do
    ref = make_ref()

    {:ok, root} = Root.start_link("**")

    :ok = Root.input(Event.new("fst/snd/third/final", {self(), ref}), root)

    assert_receive {^ref, :done}
  end

  defmodule ImportableHandler do
    use CicadaBus.Handler, partial: true

    defhandle "**", _ev, _opts do
      :ok
    end
  end

  test "application level include" do
    # This ensures we don't start partial handlers by accident
    assert_raise ArgumentError, ~r/.*can not be used directly$/, fn -> ImportableHandler.start_link("**") end
  end
end

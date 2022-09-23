defmodule CicadaBus.TopicSupervisor do
  @moduledoc """
  Supervises all the topics
  """

  require Logger

  use DynamicSupervisor

  def start_link(opts \\ []) do
    DynamicSupervisor.start_link(__MODULE__, [], opts)
  end


  @impl true
  def init(_) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end


  @doc """
  Start a child in `module` using `args.

  See {module}.child_spec/1
  """
  def start_child(module, args, pid) do
    %{start: {m, f, _a}} = module.child_spec(args)
    argstr = args |> inspect |> String.trim("[") |> String.trim("]")
    Logger.info "starting supervised child #{m}.#{f}(#{argstr})"
    DynamicSupervisor.start_child(pid, module.child_spec(args))
  end
end

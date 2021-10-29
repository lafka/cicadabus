defmodule CicadaBus.App do
  @moduledoc false
  use Application

  def start(_, _) do
    children = [
      {CicadaBus.TopicSupervisor, name: CicadaBus.TopicSupervisor}
    ]

    Supervisor.start_link(children, strategy: :one_for_all, name: __MODULE__)
  end
end

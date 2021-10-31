# CicadaBus

A minimal, local topic exchange.

Provides a GenServer based topic handler. Allow sieving events through topics and
subscribers to create several levels of one-to-many event dispatch.
    
It's intended to make a simple, local, topic exchange without any external dependencies.


## Installation

If [available on GitHub](https://github.com/lafka/cicadabus), the package can be installed
by adding `cicadabus` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:cicadabus, github: "lafka/cicadabus"}
  ]
end
```


## Example


```elixir
defmodule LocalExchange do
  use CicadaBus.Handler

  require Logger

  defmodule SpecializedExchange do
    use CicadaBus

    defhandle "**/special/topic", ev, _opts do
      Logger.info "matched **/special/topic!"
    end
  end

  # Spawn a sub exchange processing events starting with `special/`
  deftopic "special/**", to: __MODULE__.SpecializedExchange


  # This is spawn a queue which requires acknowledgement from
  # all subscribers 
  deftopic "outgoing/**", acknowledge: true, guarantee: :all


  # Handlers process the event inside the worker GenServer.
  # Any number of handlers may be used, they will be called
  # exactly once. In case of a crash this will take down the
  # entire GenServer.
  defhandle "**", ev = %{topic: topic}, _opts do
     Logger.info "match #{Enum.join(topic, "/")}: #{inspect ev}
  end
end
```

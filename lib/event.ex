defmodule CicadaBus.Event do
  @moduledoc """
  A specific event given as input to the stream
  """

  use TypedStruct

  alias CicadaBus.Handler

  typedstruct enforce: true do
    field(:correlation_id, Handler.correlation_id())
    field(:client_id, Handler.client_id())
    field(:topic, Handler.topic())
    field(:priority, non_neg_integer(), default: 1000)
    field(:value, term())
    field(:acknowledge, bool() | non_neg_integer(), default: false)
    field(:meta, Handler.meta(), default: %{guarantee: nil})
  end

  def new(topic, value, opts \\ [])

  def new("" <> topic, value, opts) do
    new(String.split(topic, "/"), value, opts)
  end

  def new(topic, value, opts) do
    opts = Keyword.put_new(opts, :correlation_id, make_ref())
    struct(__MODULE__, [{:topic, topic}, {:value, value} | opts])
  end
end

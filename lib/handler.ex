defmodule CicadaBus.Handler do
  @moduledoc """
  The topic handler used to process incoming data

  A event may match several topics subscribers. It is always delivered
  to all of them.

  A handler is a process consuming incoming events and the delegating to
  it's subscribers (which is any process or a topic - implemented as a
  handler).

  ## Example

  ```elixir
  def Broker do
    # `sink` ensures a GenServer sink is enabled which may be called by a
    # producer like :ok = `Broker.cast(%Event{} = ev)`
    use CicadaBus.Handler, sink: true

    import AMQPClient,only: [channnel: 0]


    deftopic "netmgmt/nbr", to: NetMgmt.State
    deftopic "netmgmt/state", to: NetMgmt.State
    deftopic "netmgmt/route", to: NetMgmt.Routing

    deftopic "metering/row", to: Metering.Table
    deftopic "metering/object", to: Metering.Object
    deftopic "metering/push", to: Metering.Event
    deftopic "metering/notification", to: Metering.Event

    # Match on topic starting with hes or nms. Then forward the item over AMQP
    # for remote delivery. The difference between `defhandle` and `topic` is that
    # `defhandle` should be a stateless function which can should not yield a response.
    # `topic` on the other hand may be calls a  module which may, or may not,
    # be a stateful agent.
    #

    defhandle "**"(%{topic: [t|_]} = ev) when t in ["nms", "hes"] do
      stripped = Map.take(ev, [:correlation_id, :value, :meta])

      AMQP.Basic.publish(channel(), exchange(), Enum.join(ev.topic, "/"), stripped)

      :ok
    end
  end
  ```
  """

  use GenServer

  require Logger

  alias CicadaBus.TopicSupervisor
  alias CicadaBus.Event

  @type correlation_id() :: String.t()
  @type client_id() :: String.t()
  @type topic :: [String.t() | number()]
  @type version :: {non_neg_integer(), non_neg_integer(), non_neg_integer(), String.t()}
  # Delivery guarantees are the following:
  # :nil := no guarantees, if no subscribers event is dropped. If ACK enabled
  #         it has no other effect than logging a error
  # :any := deliver to alteast one, the event is delayed until atleast one
  #         subscriber is ready
  # :all := deliver to all subscribers. If coupled with ACK then all the
  #         subscribers must have either responded, crashed or timeout expired (if any).
  @type guarantee :: :all | :any | nil
  @type meta :: %{
          optional(:ttl) => DateTime.t(),
          optional(:api) => atom(),
          optional(:version) => version(),
          optional(:guarantee) => guarantee(),
          # The reason for delivery failure
          optional(:error) => term(),
          # the source of the queue which should be used for upstream acknowledgement
          optional(:authority) => pid() | atom()
        }



  @doc """
  Add a subscriber to topic
  """

  defmacro deftopic(t, opts \\ []) do
    module = __CALLER__.module

    regex = PathGlob.compile(t)

    # Code.eval_quoted is definitly wrong but how do we actually do this?
    {term, _} = Code.eval_quoted(opts)
    # Default to a bare handler if nothing is set
    term = Keyword.put_new(term, :to, __MODULE__)
    Module.put_attribute(module, :topics, {t, regex, term})
  end

  @doc """
  Handle one single event
  """
  defmacro defhandle(t, event, opts \\ quote(do: _), do: block) do
    module = __CALLER__.module
    regex = PathGlob.compile(t)
    topics = Module.get_attribute(module, :handlers, [])

    # fnname = :"$#{topic}_#{length(topics)}"
    fnname = :"handlers_#{t}_#{length(topics)}"

    Module.put_attribute(module, :handlers, {t, regex, to: {module, fnname, []}})

    quote do
      def unquote(fnname)(unquote(event), unquote(opts)) do
          unquote(block)
      end
      # in case of pattern matching we add a fallback so we don't crash on
      # function clause error
      def unquote(fnname)(_, _)  do
        nil
      end
    end
  end

  @doc """
  Register a module a stream handler
  """
  defmacro __using__(_) do
    caller = __CALLER__.module
    Module.register_attribute(caller, :topics, persist: true, accumulate: true)
    Module.register_attribute(caller, :handlers, persist: true, accumulate: true)

    quote do
      use GenServer
      require Logger
      import CicadaBus.Handler, except: [topics: 0]

      # @doc """
      # Start a new bus handler.

      # The bus handler allows to build a tree of processing for events. There
      # may be adhoc subscribers for each topic that may attach during runtime.
      # """
      # def start_link(topic, opts \\ []) do
      #   genargs = ~w(name)a
      #   {genopts, args} = Keyword.split(opts, genargs)
      #   GenServer.start_link(__MODULE__, [topic | args], genopts)
      # end
      @doc """
      Retrieve all the topics enabled for this bus
      """
      def topics() do
        @topics
      end

      defoverridable topics: 0

      defdelegate topic(pid, opts \\ []), to: CicadaBus.Handler
      defdelegate subscribe(pid, opts \\ []), to: CicadaBus.Handler
      defdelegate unsubscribe(ref, pid, opts \\ []), to: CicadaBus.Handler
      defdelegate input(ev, pid, opts \\ []), to: CicadaBus.Handler
      defdelegate init(args), to: CicadaBus.Handler
      defdelegate handle_info(arg, state), to: CicadaBus.Handler
      defdelegate handle_cast(arg, state), to: CicadaBus.Handler
      defdelegate handle_call(arg, from, state), to: CicadaBus.Handler

      @doc """
      Start a handler paired with a dynamic supervisor
      """
      def start_link(topic, opts \\ []) do
        valid_opts = ~w(prefix module supervisor name)a

        opts = Keyword.put_new(opts, :module, unquote(caller))

        invalid = Keyword.drop(opts, valid_opts)
        cond do
          [] != invalid ->
            keys = Keyword.keys(invalid)
            raise ArgumentError, message: "invalid keys '#{Enum.join(keys, "', '")}'"

          opts[:module] == nil or not is_atom(opts[:module])->
            raise ArgumentError, message: "option 'module' is null or not an atom"


          opts[:module] == nil ->
            raise ArgumentError, message: "missing key 'module'"

          true ->
            genopts = Keyword.take(opts, ~w(name)a)
            targetopts = Keyword.take(opts, ~w(module supervisor)a)
            GenServer.start_link(opts[:module], [topic | targetopts], genopts)
        end
      end



      @doc """
      Spec to use for child supervison

      Each topic spawns a dynamic supervisor for it's child topics and a GenServer
      for handling it's internal state
      """
      def child_spec({topic, opts}) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [topic, opts]},
          restart: :transient
        }
      end
    end
  end

  ## Public API

  @doc """
  Retreive the topic for this worker
  """
  def topic(pid, opts \\ []) do
    GenServer.call(pid, :topic, opts[:timeout] || 5000)
  end


  @doc """
  A bare handler does not have any topics
  """
  def topics() do
    []
  end


  @doc """
  Retrieve all topics of worker
  """
  def topics(pid, opts \\ []) do
    GenServer.call(pid, :topics, opts[:timeout] || 5000)
  end


  @doc """
  Peek into the queue, possibly at a given priority
  """
  def peek(pid, priority \\ 0, opts \\ []) do
    GenServer.call(pid, {:peek, priority}, opts[:timeout] || 5000)
  end


  @doc """
  Subscribe to this worker
  """
  def subscribe(pid, opts \\ []) do
    GenServer.call(pid, :subscribe, opts[:timeout] || 5000)
  end


  @doc """
  Attach a specific process to a server
  """
  def attach(upstream, downstream, opts \\ []) do
    GenServer.call(upstream, {:subscribe, downstream}, opts[:timeout] || 5000)
  end

  @doc """
  Unsubscribe from the worker
  """
  def unsubscribe(ref, pid, opts \\ []) do
    GenServer.call(pid, {:unsubscribe, ref}, opts[:timeout] || 5000)
  end

  @doc """
  Signal a new event has been found and should be delivered
  """
  def input(%Event{} = ev, pid, opts \\ []) do
    if opts[:sync] do
      GenServer.call(pid, {:event, :sync, ev}, opts[:timeout] || 5000)
    else
      GenServer.cast(pid, {:event, :async, ev})
    end
  end


  @doc """
  Start a bare handler
  """
  def start_link(topic, opts \\ []) do
    {opts, genopts} = Keyword.split(opts, ~w(module prefix supervisor)a)
    GenServer.start_link(__MODULE__, [topic | opts], genopts)
  end


  @doc """
  Spec to use for child supervison
  """
  def child_spec({topic, opts}) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [topic, opts]},
      restart: :transient
    }
  end


  ## Private API

  # Private GenServer API
  @doc """
  Start a new topic handler

  The topic handler keeps a priority queue for data it has accepted and passes
  it aysnc down to it's subscribers and subtopics.

  When using the handler module you can `deftopic` a specific topic.
  This creates a dependency on a downstream handler. It takes an optional
  key `:to` which denotes the module or a MFA triplet on what to call to
  initialize the sub-topic. If `:to` is undefined a bare handler is used

  """
  @impl true
  def init([topic | args]) do
    {module, args} = Keyword.pop(args, :module, __MODULE__)
    # {prefix, args} = Keyword.pop(args, :prefix, "#{module}#")
    {supervisor, args} = Keyword.pop(args, :supervisor, nil)

    Logger.debug("starting topic #{topic} for module #{module}")

    # Spawn the sub topics as children
    subworkers =
      for {match, _regex, matchopts} <- module.topics(), into: %{} do
        {to, _matchopts} = Keyword.pop!(matchopts, :to)
        target =
          case to do
            nil ->
              Logger.debug(" -> #{match}, 1/to: #{__MODULE__}.start_link/1")
              __MODULE__

            mod when is_atom(mod) ->
              Logger.debug(" -> #{match}, 2/to: #{mod}.start_link/1")
              mod

            {m, f, a} ->
              Logger.debug(" -> #{match}, 3/to: #{m}.#{f}/#{length(a)}")
              m
          end

        # Prepend prefix such that the same topic pattern does not conflict accross
        # multiple handlers. This means there's only one (module, topic)
        opts = [
          {:supervisor, supervisor},
          {:module, target}
        ]

        pad = String.pad_leading(" ", byte_size(match) + 5)
        cond do
          nil != supervisor ->
            {:ok, pid} = TopicSupervisor.start_child target, {match, opts}, args[:supervisor]
            # This is a static worker, the child may crash and then we'll end up with a double
            # regitration.

            # in case of supervision we need to make sure up-to-date pid is feched from
            # when dispatching. We can't use a Registry since that will automatically
            # delete the crashed pid. Instead the spawned process should register with
            # parent.
            #
            # 1. spawn **
            # 1.a. spawn child something/** with opts [parent: <pid>]
            #
            # If top-level crashes it's restarted and has lost it's child....
            # If child is crashed it's respawned with the parent pid....
            #
            # If they both use `{:via | _}` and Registry we can have a constant lookup
            # of `{topic, ctx, identifier}`
            #
            # With the combination of Registry and TopicSupervisor we can't really know
            # if a child has been spawned without calling Registry
            ref = Process.monitor(pid)
            {ref, %{pid: pid, topic: match, module: target, supervise: true}}


          # Check if the same (topic, module, input) pair has been registered

          true ->
            {m, f, a} =
              if function_exported?(target, :child_spec, 1) do
                %{start: {m, f, a}} = target.child_spec({match, opts})
                {m, f, a}
              else
                {target, :start_link, []}
              end

            argstr = a |> inspect |> String.trim("[") |> String.trim("]")

            Logger.info("  >> start #{match}, module: #{m}")
            {:ok, pid} = apply m, f, a
            # add a monitor such that the
            ref = Process.monitor(pid)
            {ref, %{pid: pid, topic: match, module: m}}
        end
      end

    regex = PathGlob.compile(topic)

    {:ok,
      %{
        # Our topic, which all input must match
        topic: topic,
        match: &([] != match?(&1, {topic, regex, []}, &2, [])),
        # The current event we're processing
        event: {nil, _delivered = [], _pending = []},
        module: module,
        # The total queue
        queue: :pqueue.new(),
        subscribers: subworkers
      }}
  end

  @impl true
  def handle_call(:topic, _, %{topic: t} = state), do: {:reply, {:ok, t}, state}

  def handle_call(:subscribe, {pid, _} = from, state) do
    handle_call({:subscribe, pid}, from, state)
  end

  def handle_call({:subscribe, pid}, _from, state) do
    case Enum.find(state.subscribers, fn {_ref, %{pid: p}} -> p == pid end) do
      nil ->
        ref = Process.monitor(pid)

        Logger.info("Subscribing #{inspect(pid)} to '#{state.topic}', ref := #{inspect(ref)}")

        newstate = sync_new_subscriber(state, {ref, pid})

        {:reply, {:ok, ref}, newstate}

      {ref, pid} ->
        Logger.debug("already subscribed #{inspect(pid)} to '#{state.topic}, ref := #{inspect pid}'")
        {:reply, {:ok, ref}, state}
    end
  end

  def handle_call({:unsubscribe, ref}, {_pid, _}, %{subscribers: subscribers} = state) do
    case Map.drop(subscribers, [ref]) do
      # if value  is unchanged we don't have a subscription by that ref
      ^subscribers ->
        Logger.warning(
          "Error unsubscribe #{inspect(ref)} from '#{state.topic}': NO SUBSCRIPTION"
        )

        {:reply, {:error, :no_subscription}, state}

      subscribers ->
        Logger.info("Successfully unsubscribed #{inspect(ref)} from '#{state.topic}'")
        newstate = drop_pending_output(ref, %{state | subscribers: subscribers})
        {:reply, :ok, newstate}
    end
  end

  # Like cast but returns :ok | :drop depending on event acceptance
  def handle_call({:event, :sync, ev}, _from, state) do
    {r, nextstate} = on_event(ev, state)
    {:reply, r, nextstate}
  end


  @impl true
  def handle_cast({:event, :async, ev}, state) do
    {_, nextstate} = on_event(ev, state)
    {:noreply, nextstate}
  end


  @impl true
  def handle_info({:DOWN, monref, :process, pid, _reason}, state) do
    {:reply, _, state} = handle_call({:unsubscribe, monref}, {pid, make_ref()}, state)
    {:noreply, state}
  end

  def handle_info({_fromref, {:event, _ackref, %Event{} = ev}}, state) do
    {_, nextstate} = on_event(ev, state)
    {:noreply, state}
  end


  defp on_event(%Event{priority: priority} = ev, state) do
    cond do
      not state.match.(ev, []) ->
        Logger.debug("drop #{Enum.join(ev.topic, "/")} from unnmatched '#{state.topic}'")
        {:drop, state}

      not pending?(state) ->
        Logger.debug("accept  #{Enum.join(ev.topic, "/")} on '#{state.topic}' -> output")
        {:ok, %{state | event: output({ev, [], []}, state)}}

      true ->
        Logger.debug("accept #{Enum.join(ev.topic, "/")} on '#{state.topic}' -> queue")
        {:ok, queue_event({priority, ev}, state)}
    end
  end


  defp pending?(_state = %{event: {_ev = nil, __delivered, _pending}}), do: false

  defp pending?(_state = %{event: {_ev = %{meta: %{guarantee: g}}, delivered, pending}}) do
    case g do
      nil -> false
      :any -> [] == delivered
      :all -> [] != pending
    end
  end

  defp pending?(%{event: {_ev, _deliverd, _pending}}), do: false


  # keep a queue of events we expect to process soon
  defp queue_event({priority, event}, %{queue: queue} = state) do
    %{state | queue: :pqueue.in(event, priority, queue)}
  end


  defp drop_pending_output(_ref, %{event: {_, nil, _, _}} = s), do: s

  defp drop_pending_output(ref, %{event: {ev, delivered, pending}} = s) do
    %{s | event: {ev, delivered -- [ref], pending -- [ref]}}
  end


  defp sync_new_subscriber(state, {ref, pid}) do
    newstate = %{state | subscribers: Map.put(state.subscribers, ref, %{pid: pid})}

    case state.event do
      {nil, _, _} ->
        newstate

      # We have a event and it's waiting to be consumed
      {_event, [], []} = e ->
        %{newstate | event: output(e, newstate)}
    end
  end


  defp output({nil, _delivered, _pending} = null_event, _state), do: null_event

  defp output({input, delivered, pending}, state) do
    # Deliver only to these subscribers
    new_targets = Map.drop(state.subscribers, delivered ++ pending)
    Logger.debug("output  #{Enum.join(input.topic, "/")} to #{map_size new_targets} subscriber")

    newly_delivered =
      for {ref, target} <- new_targets do
        event_ref = make_ref()

        case target do
          %{pid: pid} when is_pid(pid) ->
            Logger.debug("deliver #{Enum.join(input.topic, "/")} to #{inspect pid} with ref #{inspect event_ref}")
            send(pid, {ref, {:event, event_ref, input}})
            {ref, Process.alive?(pid), event_ref}
        end
      end

    newly_delivered =
      newly_delivered
      |> ensure_liveness()
      |> maybe_await_ack(input)

    guarantee = input.meta.guarantee

    handlers = for {:handlers, [v]} <- state.module.__info__(:attributes), do: v

    # handlers = state.module.__info__(:attributes)[:handlers] || []
    uncalled_handlers  = handlers -- delivered

    for {_, _, [to: {mod, fnname, _}]} <- uncalled_handlers do
      apply mod, fnname, [input, []]
    end

    delivered = newly_delivered ++ delivered ++ uncalled_handlers

    cond do
      nil == guarantee ->
        {nil, [], []}

      # At least one was delivered
      :any == guarantee and [] != delivered ->
        {nil, [], []}

      true ->
        # We've reached here since not all items were delivered. This means the
        # request must be retried. In the case of a crashed subscriber we don't
        # need, in case a topic handler crashed it must be retried.
        new_target_keys = Map.keys(new_targets)
        {input, delivered, (pending ++ new_target_keys) -- delivered}
    end
  end


  defp ensure_liveness(delivered) do
    for {subref, true, evref} <- delivered, into: %{}, do: {evref, subref}
  end

  defp maybe_await_ack(delivered, %Event{acknowledge: true} = ev) do
    {:ok, tref} = :timer.send_after(ev.meta.ttl, :cancel_ack_wait)

    delivered = maybe_await_ack2(delivered)

    {:ok, :close} = :timer.cancel(tref)
    delivered
  end

  defp maybe_await_ack(delivered, %Event{}), do: Map.values(delivered)

  def maybe_await_ack2(%{} = delivered, acc \\ []) do
    receive do
      :cancel_ack_wait ->
        acc

      {ackref, :ack} ->
        case delivered[ackref] do
          nil -> maybe_await_ack2(delivered, acc)
          ^ackref -> maybe_await_ack2(Map.drop(delivered, [ackref]), [ackref | acc])
        end
    end
  end


  defp match?(%Event{topic: topic} = event, {match, regex, matchopts}, opts, acc) do
    if String.match?(Enum.join(topic, "/"), regex) do
      {mod, fun, args} =
        case matchopts[:to] do
          mod when is_atom(mod) ->
            {mod, :cast, [event, opts]}

          {mod, fun} ->
            {mod, fun, [event, opts]}

          {mod, fun, args} ->
            {mod, fun, args ++ [event, opts]}
        end

      cond do
        true == matchopts[:check] and nil != apply(mod, fun, args) ->
          [match | acc]

        true != matchopts[:check] ->
          [match | acc]

        true ->
          acc
      end
    else
      acc
    end
  end
end

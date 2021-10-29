defmodule CicadaBus do
  @moduledoc """
  Handles incoming data, matches by topic and processes by priority.

  Each topic will spawn a new worker will be spawned. This has has the
  responsibility of fanning out data to it's subscribers. Once the data
  has been delivered to all subscribers (if any) it should acknolwedge
  the event and fetch the next one. In case no subscribers the ACK-REQUIRED
  flag controls the behavour: either discard the event or wait until a
  subscriber is available.

  Bus
    -> spawn topic a/b
    -> spawn topic aa/x
    -> spawn topic x/y/z

  Send event x/y/z without ACK-REQUIRED, the event is added to queue and the
  worker fetches it when free and then discards as there's no workers

  In case ACK-REQUIRED is set to true then the worker will keep it until there's
  a subscriber or a event of higher priority is received.

  The following messages are transmited between Bus and worker:
    bus > worker :: new-head (when a event of higher priority is found)
    bus < worker :: ack<ref> (acknowledge the event we have)

  Between a subscriber and worker:
    sub > worker :: subscribe
    sub < worker :: subscribed
    sub < worker :: event
    sub > worker :: ack
    sub > worker :: close

  """
  use CicadaBus.Handler
  require Logger

  defhandle "$deadletter/**", ev, _opts do
    Logger.warning(
      """
      Failed to handle processing of #{Enum.join(tl(ev.topic), "/")}, revoking item

      #{inspect ev.meta.error, pretty: true}
      """)

    :ok
  end
end

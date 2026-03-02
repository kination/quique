defmodule Snaq.Application do
  use Application

  def start(_type, _args) do
    port = System.get_env("TOQ_PORT", "7001") |> String.to_integer()

    children = [
      {Registry, keys: :unique, name: Snaq.QueueRegistry},
      Snaq.Queue.Supervisor,
      {Task.Supervisor, name: Snaq.TCP.TaskSupervisor},
      {Snaq.TCP.Server, port: port}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Snaq.Supervisor)
  end
end

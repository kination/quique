defmodule Snaq.TCP.Server do
  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(opts) do
    port = Keyword.get(opts, :port, 7001)

    {:ok, listen_socket} =
      :gen_tcp.listen(port, [:binary, packet: :raw, active: false, reuseaddr: true])

    Logger.info("Snaq listening on port #{port}")
    send(self(), :accept)
    {:ok, listen_socket}
  end

  # Non-blocking accept loop: 500ms timeout lets the GenServer stay responsive
  def handle_info(:accept, listen_socket) do
    case :gen_tcp.accept(listen_socket, 500) do
      {:ok, client} ->
        {:ok, pid} =
          Task.Supervisor.start_child(Snaq.TCP.TaskSupervisor, fn ->
            Snaq.TCP.Handler.handle(client)
          end)

        :gen_tcp.controlling_process(client, pid)

      {:error, :timeout} ->
        :ok

      {:error, reason} ->
        Logger.warning("Accept error: #{inspect(reason)}")
    end

    send(self(), :accept)
    {:noreply, listen_socket}
  end
end

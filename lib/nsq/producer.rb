require_relative 'exceptions'
require_relative 'selectable_queue'
require_relative 'retry'
require_relative 'client_base'

module Nsq
  class Producer < ClientBase
    attr_reader :topic

    def initialize(opts = {})
      @connections = {}
      @topic = opts[:topic]
      @synchronous = opts[:synchronous] || false
      @discovery_interval = opts[:discovery_interval] || 60
      @ssl_context = opts[:ssl_context]
      @tls_options = opts[:tls_options]
      @tls_v1 = opts[:tls_v1]
      @retry_attempts = opts[:retry_attempts] || 10

      @write_queue = SelectableQueue.new(10000)
      @response_queue = SelectableQueue.new(10000)

      if nsqd = [opts[:nsqd]].flatten.first
        @connection = add_connection(nsqd)
      else
        @connection = add_connection('127.0.0.1:4150')
      end

      Thread.new { start_router() }
    end

    def write(*raw_messages)
      if !@topic
        raise 'No topic specified. Either specify a topic when instantiating the Producer or use write_to_topic.'
      end

      write_to_topic(@topic, *raw_messages)
    end

    # Arg 'delay' in seconds
    def deferred_write(delay, *raw_messages)
      if !@topic
        raise 'No topic specified. Either specify a topic when instantiating the Producer or use write_to_topic.'
      end
      if delay < 0.0
        raise "Delay can't be negative, use a positive float."
      end

      deferred_write_to_topic(@topic, delay, *raw_messages)
    end

    def write_to_topic(topic, *raw_messages)
      # return error if message(s) not provided
      raise ArgumentError, 'message not provided' if raw_messages.empty?

      # stringify the messages
      messages = raw_messages.map(&:to_s)

      if messages.length > 1
        msg = { op: :mpub, topic: topic, payload: messages }
      else
        msg = { op: :pub, topic: topic, payload: messages.first }
      end

      Nsq::with_retries max_attempts: @retry_attempts do
        msg[:result] = SizedQueue.new(1) if @synchronous
        @write_queue.push(msg)
        if msg[:result]
          value = msg[:result].pop
          raise value if value.is_a?(Exception)
        end
      end
    end

    def start_router(connection)
      transactions = []
      loop do
        ready, _, _ = IO::select([@response_queue, @write_queue])
        if ready.include?(@response_queue)
          data = @response_queue.pop
          result = transactions.pop
          if data[:frame].is_a?(Response)
            result.push(nil)
          elsif data[:frame].is_a?(Error)
            result.push(ErrorFrameException.new(frame.data))
          else
            result.push(InvalidFrameException.new(frame.data))
          end
        else
          data = @write_queue.pop
          return if data[:op] == :stop_router
          @connection.send(data[:op], data[:topic], data[:payload])
          transactions.push(data[:result])
        end
      end
    end

    def deferred_write_to_topic(topic, delay, *raw_messages)
      raise ArgumentError, 'message not provided' if raw_messages.empty?
      messages = raw_messages.map(&:to_s)
      connection = connection_for_write
      messages.each do |msg|
        connection.dpub(topic, (delay * 1000).to_i, msg)
      end
    end

    def stop_router
      @write_queue.push(op: :stop_router)
    end

    def terminate
      stop_router
      super
    end

    private
    def connection_for_write
      # Choose a random Connection that's currently connected
      # Or, if there's nothing connected, just take any random one
      connections_currently_connected = connections.select{|_,c| c.connected?}
      connection = connections_currently_connected.values.sample || connections.values.sample

      # Raise an exception if there's no connection available
      unless connection
        raise 'No connections available'
      end

      connection
    end

  end
end
